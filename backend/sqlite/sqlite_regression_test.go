package sqlite

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActivityLockTimeoutIsApplied(t *testing.T) {
	ctx := context.Background()
	be := newRegressionBackend(t, time.Second, 50*time.Millisecond)
	insertRegressionActivity(t, ctx, be, "timeout", 1)

	if _, err := be.getActivityWorkItem(ctx); err != nil {
		t.Fatalf("first activity acquisition: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, err := be.getActivityWorkItem(ctx); errors.Is(err, errNoWorkItems) {
		t.Fatal("activity did not expire after ActivityLockTimeout")
	} else if err != nil {
		t.Fatalf("redeliver expired activity: %v", err)
	}
}

func TestActivityDequeueOrdersSequenceNumbers(t *testing.T) {
	ctx := context.Background()
	be := newRegressionBackend(t, time.Second, time.Second)
	insertRegressionActivity(t, ctx, be, "fifo", 1)
	insertRegressionActivity(t, ctx, be, "fifo", 2)

	wi, err := be.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("dequeue activity: %v", err)
	}
	if wi.NewEvent.GetEventId() != 1 {
		t.Fatalf("first activity event ID=%d, want 1", wi.NewEvent.GetEventId())
	}
}

func TestWorkflowCompletionRejectsLostLease(t *testing.T) {
	ctx := context.Background()
	be := newRegressionBackend(t, time.Second, time.Second)
	instanceID := "lost-workflow-lease"
	insertRegressionWorkflow(t, ctx, be, instanceID, 1)

	wi, err := be.getWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("acquire workflow: %v", err)
	}
	wi.LockedBy = "stale-lease-token"
	wi.State = &protos.WorkflowRuntimeState{InstanceId: instanceID}
	if err := be.CompleteWorkflowWorkItem(ctx, wi); !errors.Is(err, backend.ErrWorkItemLockLost) {
		t.Fatalf("complete stale workflow error=%v, want %v", err, backend.ErrWorkItemLockLost)
	}
}

func TestActivityReacquiredBySameBackendRejectsStaleCompletion(t *testing.T) {
	ctx := context.Background()
	be := newRegressionBackend(t, time.Second, 50*time.Millisecond)
	insertRegressionActivity(t, ctx, be, "stale-activity", 1)

	first, err := be.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("first activity acquisition: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	second, err := be.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("same backend activity reacquisition: %v", err)
	}
	if second.LockedBy == first.LockedBy {
		t.Fatal("same backend reused the activity lease token")
	}

	first.Result = regressionHistoryEvent(101)
	if err := be.CompleteActivityWorkItem(ctx, first); !errors.Is(err, backend.ErrWorkItemLockLost) {
		t.Fatalf("stale activity completion error=%v, want %v", err, backend.ErrWorkItemLockLost)
	}
	if got := countRegressionRows(t, ctx, be, "NewTasks"); got != 1 {
		t.Fatalf("stale activity completion removed queued task: count=%d, want 1", got)
	}

	second.Result = regressionHistoryEvent(102)
	if err := be.CompleteActivityWorkItem(ctx, second); err != nil {
		t.Fatalf("current activity completion: %v", err)
	}
}

func TestWorkflowReacquiredBySameBackendRejectsStaleCompletion(t *testing.T) {
	ctx := context.Background()
	be := newRegressionBackend(t, 50*time.Millisecond, time.Second)
	instanceID := "stale-workflow"
	insertRegressionWorkflow(t, ctx, be, instanceID, 1)

	first, err := be.getWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("first workflow acquisition: %v", err)
	}
	first.State = &protos.WorkflowRuntimeState{InstanceId: instanceID, NewEvents: []*protos.HistoryEvent{regressionHistoryEvent(101)}}
	time.Sleep(120 * time.Millisecond)
	second, err := be.getWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("same backend workflow reacquisition: %v", err)
	}
	if second.LockedBy == first.LockedBy {
		t.Fatal("same backend reused the workflow lease token")
	}

	if err := be.CompleteWorkflowWorkItem(ctx, first); !errors.Is(err, backend.ErrWorkItemLockLost) {
		t.Fatalf("stale workflow completion error=%v, want %v", err, backend.ErrWorkItemLockLost)
	}
	if got := countRegressionRows(t, ctx, be, "NewEvents"); got != 1 {
		t.Fatalf("stale workflow completion removed queued event: count=%d, want 1", got)
	}

	second.State = &protos.WorkflowRuntimeState{InstanceId: instanceID}
	if err := be.CompleteWorkflowWorkItem(ctx, second); err != nil {
		t.Fatalf("current workflow completion: %v", err)
	}
}

func TestWorkflowDequeueDeclaresFIFOOrdering(t *testing.T) {
	source, err := os.ReadFile("sqlite.go")
	if err != nil {
		t.Fatalf("read sqlite.go: %v", err)
	}
	workflow := functionRegion(t, string(source), "func (be *sqliteBackend) getWorkflowWorkItem", "func (be *sqliteBackend) NextWorkflowWorkItem")
	if !strings.Contains(workflow, "ORDER BY [SequenceNumber] ASC") {
		t.Fatal("NewEvents dequeue does not explicitly ORDER BY SequenceNumber ASC")
	}
}

func TestWorkflowDequeueChecksReturningCursorErrors(t *testing.T) {
	source, err := os.ReadFile("sqlite.go")
	if err != nil {
		t.Fatalf("read sqlite.go: %v", err)
	}
	workflow := functionRegion(t, string(source), "func (be *sqliteBackend) getWorkflowWorkItem", "func (be *sqliteBackend) NextWorkflowWorkItem")

	closeIndex := strings.LastIndex(workflow, "events.Close()")
	errIndex := strings.Index(workflow, "events.Err()")
	commitIndex := strings.Index(workflow, "tx.Commit()")
	if strings.Count(workflow, "events.Close()") < 2 || closeIndex < 0 || errIndex < 0 || commitIndex < 0 || closeIndex > errIndex || errIndex > commitIndex {
		t.Fatal("workflow dequeue must close and check the UPDATE RETURNING cursor before committing")
	}
}

func functionRegion(t *testing.T, source, startMarker, endMarker string) string {
	t.Helper()
	start := strings.Index(source, startMarker)
	if start < 0 {
		t.Fatalf("start marker %q not found", startMarker)
	}
	endRel := strings.Index(source[start:], endMarker)
	if endRel < 0 {
		t.Fatalf("end marker %q not found", endMarker)
	}
	return source[start : start+endRel]
}

func newRegressionBackend(t *testing.T, workflowLease, activityLease time.Duration) *sqliteBackend {
	t.Helper()
	opts := NewSqliteOptions("")
	opts.WorkflowLockTimeout = workflowLease
	opts.ActivityLockTimeout = activityLease
	be := NewSqliteBackend(opts, backend.DefaultLogger()).(*sqliteBackend)
	if err := be.CreateTaskHub(context.Background()); err != nil {
		t.Fatalf("CreateTaskHub: %v", err)
	}
	t.Cleanup(func() { _ = be.db.Close() })
	return be
}

func insertRegressionActivity(t *testing.T, ctx context.Context, be *sqliteBackend, instanceID string, eventID int32) {
	t.Helper()
	payload, err := backend.MarshalHistoryEvent(&protos.HistoryEvent{EventId: eventID, Timestamp: timestamppb.Now()})
	if err != nil {
		t.Fatalf("marshal activity: %v", err)
	}
	if _, err := be.db.ExecContext(ctx, "INSERT INTO NewTasks ([InstanceID], [EventPayload]) VALUES (?, ?)", instanceID, payload); err != nil {
		t.Fatalf("insert activity: %v", err)
	}
}

func insertRegressionWorkflow(t *testing.T, ctx context.Context, be *sqliteBackend, instanceID string, eventID int32) {
	t.Helper()
	if _, err := be.db.ExecContext(ctx, "INSERT INTO Instances ([InstanceID], [ExecutionID], [Name], [RuntimeStatus]) VALUES (?, ?, ?, 'PENDING')", instanceID, "execution-1", "regression-test"); err != nil {
		t.Fatalf("insert workflow instance: %v", err)
	}
	payload, err := backend.MarshalHistoryEvent(&protos.HistoryEvent{EventId: eventID, Timestamp: timestamppb.Now()})
	if err != nil {
		t.Fatalf("marshal workflow event: %v", err)
	}
	if _, err := be.db.ExecContext(ctx, "INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)", instanceID, payload); err != nil {
		t.Fatalf("insert workflow event: %v", err)
	}
}

func countRegressionRows(t *testing.T, ctx context.Context, be *sqliteBackend, table string) int {
	t.Helper()
	var count int
	if err := be.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func regressionHistoryEvent(eventID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: eventID, Timestamp: timestamppb.Now()}
}
