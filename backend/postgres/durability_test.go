package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const durabilityInstanceID = "strix-durability-test"

func TestActivityRedeliveredAfterLeaseExpiryAndStaleCompletionRollsBack(t *testing.T) {
	ctx := context.Background()
	reset := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)
	resetDurabilityTables(t, ctx, reset)

	workerA := reset
	workerB := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)

	insertActivity(t, ctx, workerA, durabilityInstanceID, 1)

	first, err := workerA.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire activity: %v", err)
	}

	// Simulate a process crash after the external side effect but before
	// CompleteActivityWorkItem: no abandon/complete call is made by worker A.
	time.Sleep(160 * time.Millisecond)

	second, err := workerB.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker B failed to redeliver expired activity: %v", err)
	}
	if second.SequenceNumber != first.SequenceNumber {
		t.Fatalf("redelivery selected different activity: first=%d second=%d", first.SequenceNumber, second.SequenceNumber)
	}
	if second.LockedBy == first.LockedBy {
		t.Fatalf("expected a distinct lease owner, both work items use %q", second.LockedBy)
	}

	first.Result = historyEvent(101)
	if err := workerA.CompleteActivityWorkItem(ctx, first); !errors.Is(err, backend.ErrWorkItemLockLost) {
		t.Fatalf("stale worker completion error = %v, want %v", err, backend.ErrWorkItemLockLost)
	}

	// CompleteActivityWorkItem inserts the result before deleting NewTasks.
	// Both statements are in one transaction, so the failed stale delete must
	// roll the result insert back as well.
	if got := countRows(t, ctx, workerA, "NewEvents"); got != 0 {
		t.Fatalf("stale completion leaked %d result event(s); transaction was not atomic", got)
	}
	if got := countRows(t, ctx, workerA, "NewTasks"); got != 1 {
		t.Fatalf("stale completion removed activity: NewTasks count=%d, want 1", got)
	}

	second.Result = historyEvent(102)
	if err := workerB.CompleteActivityWorkItem(ctx, second); err != nil {
		t.Fatalf("current lease owner failed to complete activity: %v", err)
	}
	if got := countRows(t, ctx, workerB, "NewTasks"); got != 0 {
		t.Fatalf("completed activity remains queued: NewTasks count=%d", got)
	}
	if got := countRows(t, ctx, workerB, "NewEvents"); got != 1 {
		t.Fatalf("activity result count=%d, want 1", got)
	}
}

func TestActivityReacquiredBySameBackendRejectsStaleCompletion(t *testing.T) {
	ctx := context.Background()
	be := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)
	resetDurabilityTables(t, ctx, be)
	insertActivity(t, ctx, be, durabilityInstanceID, 1)

	first, err := be.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("first activity acquisition: %v", err)
	}
	time.Sleep(160 * time.Millisecond)
	second, err := be.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("same backend reacquisition: %v", err)
	}
	if second.LockedBy == first.LockedBy {
		t.Fatal("same backend reused the lease token")
	}

	first.Result = historyEvent(101)
	if err := be.CompleteActivityWorkItem(ctx, first); !errors.Is(err, backend.ErrWorkItemLockLost) {
		t.Fatalf("stale completion error=%v, want %v", err, backend.ErrWorkItemLockLost)
	}
	if got := countRows(t, ctx, be, "NewEvents"); got != 0 {
		t.Fatalf("stale completion leaked %d result event(s)", got)
	}
	second.Result = historyEvent(102)
	if err := be.CompleteActivityWorkItem(ctx, second); err != nil {
		t.Fatalf("current lease completion: %v", err)
	}
}

func TestActivityLeaseExpiryCanOverlapExternalExecutions(t *testing.T) {
	ctx := context.Background()
	const lease = 80 * time.Millisecond
	workerA := newDurabilityBackend(t, lease, lease)
	resetDurabilityTables(t, ctx, workerA)
	workerB := newDurabilityBackend(t, lease, lease)

	var externalEffects atomic.Int32
	effectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		externalEffects.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(effectServer.Close)

	insertActivity(t, ctx, workerA, durabilityInstanceID, 1)
	first, err := workerA.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire activity: %v", err)
	}

	effectStarted := make(chan struct{})
	allowFirstCompletion := make(chan struct{})
	firstCompletion := make(chan error, 1)
	go func() {
		if err := invokeExternalEffect(effectServer.URL); err != nil {
			firstCompletion <- err
			return
		}
		close(effectStarted)
		<-allowFirstCompletion
		first.Result = historyEvent(101)
		firstCompletion <- workerA.CompleteActivityWorkItem(ctx, first)
	}()
	<-effectStarted

	time.Sleep(2 * lease)
	second, err := workerB.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker B failed to acquire activity while worker A was still executing: %v", err)
	}
	if second.SequenceNumber != first.SequenceNumber {
		t.Fatalf("overlapping delivery selected sequence=%d, want %d", second.SequenceNumber, first.SequenceNumber)
	}
	if err := invokeExternalEffect(effectServer.URL); err != nil {
		t.Fatalf("worker B external effect: %v", err)
	}
	if got := externalEffects.Load(); got != 2 {
		t.Fatalf("external effects while worker A is still active=%d, want 2", got)
	}

	second.Result = historyEvent(102)
	if err := workerB.CompleteActivityWorkItem(ctx, second); err != nil {
		t.Fatalf("worker B failed to complete redelivered activity: %v", err)
	}
	close(allowFirstCompletion)
	if err := <-firstCompletion; !errors.Is(err, backend.ErrWorkItemLockLost) {
		t.Fatalf("worker A completion error=%v, want %v", err, backend.ErrWorkItemLockLost)
	}

	if got := countRows(t, ctx, workerB, "NewTasks"); got != 0 {
		t.Fatalf("completed activity remains queued: NewTasks count=%d", got)
	}
	if got := countRows(t, ctx, workerB, "NewEvents"); got != 1 {
		t.Fatalf("activity results=%d, want 1 from current lease owner", got)
	}
}

func TestWorkflowRedeliveredAfterLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	workerA := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)
	resetDurabilityTables(t, ctx, workerA)
	workerB := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)

	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, nil, 1)

	first, err := workerA.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire workflow: %v", err)
	}

	// Simulate worker A disappearing while holding the workflow lease.
	time.Sleep(160 * time.Millisecond)

	second, err := workerB.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker B failed to recover expired workflow lease: %v", err)
	}
	if second.InstanceID != first.InstanceID {
		t.Fatalf("recovery selected different workflow: first=%s second=%s", first.InstanceID, second.InstanceID)
	}
	if second.LockedBy == first.LockedBy {
		t.Fatalf("expected a distinct workflow lease owner, both use %q", second.LockedBy)
	}
	if second.RetryCount < 1 {
		t.Fatalf("redelivered workflow RetryCount=%d, want >= 1", second.RetryCount)
	}
}

func TestWorkflowReacquiredBySameBackendRejectsStaleCompletion(t *testing.T) {
	ctx := context.Background()
	be := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)
	resetDurabilityTables(t, ctx, be)
	insertWorkflowWithEvent(t, ctx, be, durabilityInstanceID, nil, 1)

	first, err := be.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("first workflow acquisition: %v", err)
	}
	first.State = &protos.WorkflowRuntimeState{
		InstanceId: string(first.InstanceID),
		NewEvents:  []*protos.HistoryEvent{historyEvent(101)},
	}
	time.Sleep(160 * time.Millisecond)
	second, err := be.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("same backend workflow reacquisition: %v", err)
	}
	if second.LockedBy == first.LockedBy {
		t.Fatal("same backend reused the workflow lease token")
	}

	if err := be.CompleteWorkflowWorkItem(ctx, first); err == nil {
		t.Fatal("stale workflow completion succeeded")
	}
	if got := countRows(t, ctx, be, "History"); got != 0 {
		t.Fatalf("stale workflow completion leaked %d history row(s)", got)
	}
	if got := countRows(t, ctx, be, "NewEvents"); got != 1 {
		t.Fatalf("stale workflow completion changed queued events=%d, want 1", got)
	}

	second.State = &protos.WorkflowRuntimeState{InstanceId: string(second.InstanceID)}
	if err := be.CompleteWorkflowWorkItem(ctx, second); err != nil {
		t.Fatalf("current workflow lease completion: %v", err)
	}
}

func TestWorkflowCompletionFailureBeforeCommitRollsBack(t *testing.T) {
	ctx := context.Background()
	workerA := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)
	resetDurabilityTables(t, ctx, workerA)
	workerB := newDurabilityBackend(t, 80*time.Millisecond, 80*time.Millisecond)

	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, nil, 1)
	first, err := workerA.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire workflow: %v", err)
	}
	first.State = &protos.WorkflowRuntimeState{
		InstanceId: string(first.InstanceID),
		NewEvents:  []*protos.HistoryEvent{historyEvent(2)},
	}

	const triggerName = "durabletask_fail_workflow_completion"
	const functionName = "durabletask_fail_workflow_completion_before_commit"
	if _, err := workerA.db.Exec(ctx, `CREATE FUNCTION `+functionName+`() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'injected workflow completion failure';
END;
$$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create failure injection function: %v", err)
	}
	if _, err := workerA.db.Exec(ctx, `CREATE TRIGGER `+triggerName+` BEFORE DELETE ON NewEvents
FOR EACH STATEMENT EXECUTE FUNCTION `+functionName+`()`); err != nil {
		t.Fatalf("create failure injection trigger: %v", err)
	}
	removeFailureInjection := func() {
		if _, err := workerA.db.Exec(ctx, "DROP TRIGGER IF EXISTS "+triggerName+" ON NewEvents"); err != nil {
			t.Errorf("drop failure injection trigger: %v", err)
		}
		if _, err := workerA.db.Exec(ctx, "DROP FUNCTION IF EXISTS "+functionName+"()"); err != nil {
			t.Errorf("drop failure injection function: %v", err)
		}
	}
	t.Cleanup(removeFailureInjection)

	err = workerA.CompleteWorkflowWorkItem(ctx, first)
	if err == nil || !strings.Contains(err.Error(), "injected workflow completion failure") {
		t.Fatalf("CompleteWorkflowWorkItem error = %v, want injected failure", err)
	}
	removeFailureInjection()

	if got := countRows(t, ctx, workerA, "History"); got != 0 {
		t.Fatalf("history rows after failed completion=%d, want 0", got)
	}
	if got := countRows(t, ctx, workerA, "NewEvents"); got != 1 {
		t.Fatalf("queued events after failed completion=%d, want 1", got)
	}

	var lockedBy string
	var leaseActive bool
	if err := workerA.db.QueryRow(ctx, "SELECT LockedBy, LockExpiration IS NOT NULL FROM Instances WHERE InstanceID = $1", durabilityInstanceID).Scan(&lockedBy, &leaseActive); err != nil {
		t.Fatalf("read workflow lease after failed completion: %v", err)
	}
	if lockedBy != first.LockedBy || !leaseActive {
		t.Fatalf("workflow lease after failed completion=(owner=%q active=%t), want owner=%q active=true", lockedBy, leaseActive, first.LockedBy)
	}

	time.Sleep(160 * time.Millisecond)
	redelivered, err := workerB.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker B failed to redeliver workflow after failed completion: %v", err)
	}
	if redelivered.InstanceID != first.InstanceID {
		t.Fatalf("redelivery selected instance %q, want %q", redelivered.InstanceID, first.InstanceID)
	}
	if len(redelivered.NewEvents) != 1 || redelivered.NewEvents[0].GetEventId() != 1 {
		t.Fatalf("redelivery events=%v, want original event ID 1", redelivered.NewEvents)
	}
}

func TestWorkflowCompletionAfterCommitIsNotAppliedTwice(t *testing.T) {
	ctx := context.Background()
	workerA := newDurabilityBackend(t, 2*time.Second, 2*time.Second)
	resetDurabilityTables(t, ctx, workerA)

	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, nil, 1)
	wi, err := workerA.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire workflow: %v", err)
	}
	wi.State = &protos.WorkflowRuntimeState{
		InstanceId: string(wi.InstanceID),
		NewEvents:  []*protos.HistoryEvent{historyEvent(2)},
	}

	if err := workerA.CompleteWorkflowWorkItem(ctx, wi); err != nil {
		t.Fatalf("worker A failed to complete workflow: %v", err)
	}
	if got := countRows(t, ctx, workerA, "History"); got != 1 {
		t.Fatalf("history rows after first completion=%d, want 1", got)
	}
	if got := countRows(t, ctx, workerA, "NewEvents"); got != 0 {
		t.Fatalf("queued events after first completion=%d, want 0", got)
	}

	// Simulate a worker crash after PostgreSQL committed the completion but
	// before the worker received the successful response. The restarted worker
	// retries the original work item without knowing whether it committed.
	workerA.db.Close()
	workerA.db = nil
	workerB := newDurabilityBackend(t, 2*time.Second, 2*time.Second)

	if err := workerB.CompleteWorkflowWorkItem(ctx, wi); err == nil {
		t.Fatal("retry of committed workflow completion succeeded")
	}
	if got := countRows(t, ctx, workerB, "History"); got != 1 {
		t.Fatalf("history rows after completion retry=%d, want 1", got)
	}
	if got := countRows(t, ctx, workerB, "NewEvents"); got != 0 {
		t.Fatalf("queued events after completion retry=%d, want 0", got)
	}
	if _, err := workerB.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("workflow item after committed completion retry: %v, want %v", err, errNoWorkItems)
	}
}

func TestDurableTimerSurvivesBackendRestart(t *testing.T) {
	ctx := context.Background()
	workerA := newDurabilityBackend(t, 100*time.Millisecond, 100*time.Millisecond)
	resetDurabilityTables(t, ctx, workerA)

	visibleAt := time.Now().UTC().Add(250 * time.Millisecond)
	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, &visibleAt, 7)

	if _, err := workerA.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("timer became visible before due time: %v", err)
	}

	// Closing the pool approximates the persistence boundary of a process
	// restart: the in-memory backend is discarded while PostgreSQL remains.
	workerA.db.Close()
	workerA.db = nil

	workerB := newDurabilityBackend(t, 100*time.Millisecond, 100*time.Millisecond)
	if _, err := workerB.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("restarted backend observed timer before due time: %v", err)
	}

	time.Sleep(time.Until(visibleAt) + 100*time.Millisecond)
	wi, err := workerB.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("restarted backend failed to observe due timer: %v", err)
	}
	if string(wi.InstanceID) != durabilityInstanceID {
		t.Fatalf("timer delivered to instance %q, want %q", wi.InstanceID, durabilityInstanceID)
	}
	if len(wi.NewEvents) != 1 || wi.NewEvents[0].GetEventId() != 7 {
		t.Fatalf("unexpected timer payload after restart: %+v", wi.NewEvents)
	}
}

func newDurabilityBackend(t *testing.T, workflowLease, activityLease time.Duration) *postgresBackend {
	t.Helper()

	host := getenv("PGHOST", "127.0.0.1")
	portText := getenv("PGPORT", "5432")
	port64, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		t.Fatalf("invalid PGPORT %q: %v", portText, err)
	}

	opts := NewPostgresOptions(
		host,
		uint16(port64),
		getenv("PGDATABASE", "durabletask"),
		getenv("PGUSER", "postgres"),
		getenv("PGPASSWORD", "postgres"),
	)
	opts.WorkflowLockTimeout = workflowLease
	opts.ActivityLockTimeout = activityLease

	be := NewPostgresBackend(opts, backend.DefaultLogger()).(*postgresBackend)
	if err := be.CreateTaskHub(context.Background()); err != nil {
		t.Fatalf("CreateTaskHub: %v", err)
	}
	t.Cleanup(func() {
		if be.db != nil {
			be.db.Close()
		}
	})
	return be
}

func resetDurabilityTables(t *testing.T, ctx context.Context, be *postgresBackend) {
	t.Helper()
	_, err := be.db.Exec(ctx, "TRUNCATE TABLE History, NewEvents, NewTasks, Instances RESTART IDENTITY")
	if err != nil {
		t.Fatalf("reset durability tables: %v", err)
	}
}

func insertActivity(t *testing.T, ctx context.Context, be *postgresBackend, instanceID string, eventID int32) {
	t.Helper()
	payload, err := backend.MarshalHistoryEvent(historyEvent(eventID))
	if err != nil {
		t.Fatalf("marshal activity: %v", err)
	}
	if _, err := be.db.Exec(ctx, "INSERT INTO NewTasks (InstanceID, EventPayload) VALUES ($1, $2)", instanceID, payload); err != nil {
		t.Fatalf("insert activity: %v", err)
	}
}

func insertWorkflowWithEvent(t *testing.T, ctx context.Context, be *postgresBackend, instanceID string, visibleAt *time.Time, eventID int32) {
	t.Helper()
	if _, err := be.db.Exec(ctx, `INSERT INTO Instances (InstanceID, ExecutionID, Name, RuntimeStatus) VALUES ($1, $2, $3, 'PENDING')`, instanceID, "execution-1", "durability-test"); err != nil {
		t.Fatalf("insert workflow instance: %v", err)
	}
	payload, err := backend.MarshalHistoryEvent(historyEvent(eventID))
	if err != nil {
		t.Fatalf("marshal workflow event: %v", err)
	}
	if _, err := be.db.Exec(ctx, "INSERT INTO NewEvents (InstanceID, VisibleTime, EventPayload) VALUES ($1, $2, $3)", instanceID, visibleAt, payload); err != nil {
		t.Fatalf("insert workflow event: %v", err)
	}
}

func countRows(t *testing.T, ctx context.Context, be *postgresBackend, table string) int {
	t.Helper()
	var count int
	if err := be.db.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func historyEvent(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   id,
		Timestamp: timestamppb.Now(),
	}
}

func invokeExternalEffect(effectURL string) error {
	response, err := http.Post(effectURL, "text/plain", nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("external effect status=%d, want %d", response.StatusCode, http.StatusNoContent)
	}
	return nil
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
