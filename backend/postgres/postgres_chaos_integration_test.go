//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
)

func TestIntegrationPostgresRestartDuringWorkflowLease(t *testing.T) {
	requirePostgresChaos(t)

	ctx := context.Background()
	const lease = 15 * time.Second
	workerA := newDurabilityBackend(t, lease, lease)
	resetDurabilityTables(t, ctx, workerA)
	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, nil, 1)

	leaseExpiresAt := time.Now().UTC().Add(lease)
	first, err := workerA.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire workflow: %v", err)
	}

	restartPostgresChaos(t)
	workerB := newDurabilityBackend(t, lease, lease)
	if !time.Now().Before(leaseExpiresAt) {
		t.Fatalf("PostgreSQL restart exceeded the workflow lease of %s", lease)
	}
	if _, err := workerB.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("workflow became available before its lease expired after PostgreSQL restart: %v", err)
	}

	time.Sleep(time.Until(leaseExpiresAt) + 200*time.Millisecond)
	redelivered, err := workerB.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("workflow was not redelivered after lease expiry and PostgreSQL restart: %v", err)
	}
	if redelivered.InstanceID != first.InstanceID {
		t.Fatalf("redelivery selected instance %q, want %q", redelivered.InstanceID, first.InstanceID)
	}
	if len(redelivered.NewEvents) != 1 || redelivered.NewEvents[0].GetEventId() != 1 {
		t.Fatalf("redelivery events=%v, want original event ID 1", redelivered.NewEvents)
	}
}

func TestIntegrationPostgresRestartAfterExternalActivityEffectIsRedelivered(t *testing.T) {
	requirePostgresChaos(t)

	var externalEffects atomic.Int32
	effectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		externalEffects.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(effectServer.Close)

	ctx := context.Background()
	const lease = 15 * time.Second
	workerA := newDurabilityBackend(t, lease, lease)
	resetDurabilityTables(t, ctx, workerA)
	insertActivity(t, ctx, workerA, durabilityInstanceID, 1)

	leaseExpiresAt := time.Now().UTC().Add(lease)
	first, err := workerA.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire activity: %v", err)
	}
	invokeExternalActivityEffect(t, effectServer.URL)

	restartPostgresChaos(t)
	workerB := newDurabilityBackend(t, lease, lease)
	if !time.Now().Before(leaseExpiresAt) {
		t.Fatalf("PostgreSQL restart exceeded the activity lease of %s", lease)
	}
	if _, err := workerB.getActivityWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("activity became available before its lease expired after PostgreSQL restart: %v", err)
	}

	time.Sleep(time.Until(leaseExpiresAt) + 200*time.Millisecond)
	redelivered, err := workerB.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("activity was not redelivered after PostgreSQL restart: %v", err)
	}
	if redelivered.SequenceNumber != first.SequenceNumber {
		t.Fatalf("redelivery selected activity sequence=%d, want %d", redelivered.SequenceNumber, first.SequenceNumber)
	}
	invokeExternalActivityEffect(t, effectServer.URL)

	if got := externalEffects.Load(); got != 2 {
		t.Fatalf("external activity effects=%d, want 2 for at-least-once delivery", got)
	}
}

func TestIntegrationPostgresRestartWithPendingTimer(t *testing.T) {
	requirePostgresChaos(t)

	ctx := context.Background()
	const timerDelay = 15 * time.Second
	workerA := newDurabilityBackend(t, timerDelay, timerDelay)
	resetDurabilityTables(t, ctx, workerA)

	visibleAt := time.Now().UTC().Add(timerDelay)
	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, &visibleAt, 7)
	restartPostgresChaos(t)

	workerB := newDurabilityBackend(t, timerDelay, timerDelay)
	if !time.Now().Before(visibleAt) {
		t.Fatalf("PostgreSQL restart exceeded the timer delay of %s", timerDelay)
	}
	if _, err := workerB.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("timer became visible before due time after PostgreSQL restart: %v", err)
	}

	time.Sleep(time.Until(visibleAt) + 200*time.Millisecond)
	wi, err := workerB.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("due timer was not delivered after PostgreSQL restart: %v", err)
	}
	if string(wi.InstanceID) != durabilityInstanceID {
		t.Fatalf("timer delivered to instance %q, want %q", wi.InstanceID, durabilityInstanceID)
	}
	if len(wi.NewEvents) != 1 || wi.NewEvents[0].GetEventId() != 7 {
		t.Fatalf("timer events=%v, want event ID 7", wi.NewEvents)
	}
}

func TestIntegrationPostgresRestartDuringWorkflowCompletion(t *testing.T) {
	requirePostgresChaos(t)

	ctx := context.Background()
	const lease = 15 * time.Second
	workerA := newDurabilityBackend(t, lease, lease)
	resetDurabilityTables(t, ctx, workerA)
	observer := newDurabilityBackend(t, lease, lease)
	insertWorkflowWithEvent(t, ctx, workerA, durabilityInstanceID, nil, 1)
	leaseExpiresAt := time.Now().UTC().Add(lease)
	wi, err := workerA.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire workflow: %v", err)
	}
	wi.State = &protos.WorkflowRuntimeState{
		InstanceId: string(wi.InstanceID),
		NewEvents:  []*protos.HistoryEvent{historyEvent(2)},
	}

	const triggerName = "durabletask_pause_workflow_completion"
	const functionName = "durabletask_pause_workflow_completion_for_restart"
	if _, err := workerA.db.Exec(ctx, `CREATE FUNCTION `+functionName+`() RETURNS trigger AS $$
BEGIN
  PERFORM pg_sleep(30);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create completion pause function: %v", err)
	}
	if _, err := workerA.db.Exec(ctx, `CREATE TRIGGER `+triggerName+` BEFORE DELETE ON NewEvents
FOR EACH STATEMENT EXECUTE FUNCTION `+functionName+`()`); err != nil {
		t.Fatalf("create completion pause trigger: %v", err)
	}

	completed := make(chan error, 1)
	go func() {
		completed <- workerA.CompleteWorkflowWorkItem(ctx, wi)
	}()
	waitForActiveDelete(t, ctx, observer)
	restartPostgresChaos(t)

	select {
	case err := <-completed:
		if err == nil {
			t.Fatal("workflow completion succeeded despite PostgreSQL restart during its transaction")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("workflow completion did not return after PostgreSQL restart")
	}

	recovery := newDurabilityBackend(t, lease, lease)
	removeCompletionPause(t, ctx, recovery, triggerName, functionName)
	if got := countRows(t, ctx, recovery, "History"); got != 0 {
		t.Fatalf("history rows after PostgreSQL restart during completion=%d, want 0", got)
	}
	if got := countRows(t, ctx, recovery, "NewEvents"); got != 1 {
		t.Fatalf("queued events after PostgreSQL restart during completion=%d, want 1", got)
	}

	if !time.Now().Before(leaseExpiresAt) {
		t.Fatalf("PostgreSQL restart exceeded the workflow lease of %s", lease)
	}
	time.Sleep(time.Until(leaseExpiresAt) + 200*time.Millisecond)
	redelivered, err := recovery.GetWorkflowWorkItem(ctx)
	if err != nil {
		t.Fatalf("workflow was not redelivered after PostgreSQL restart during completion: %v", err)
	}
	if redelivered.InstanceID != wi.InstanceID || len(redelivered.NewEvents) != 1 || redelivered.NewEvents[0].GetEventId() != 1 {
		t.Fatalf("redelivery after PostgreSQL restart during completion=%+v, want original workflow event", redelivered)
	}
}

func TestIntegrationPostgresRestartAfterWorkflowCommitBeforeAcknowledgement(t *testing.T) {
	requirePostgresChaos(t)

	ctx := context.Background()
	workerA := newDurabilityBackend(t, 15*time.Second, 15*time.Second)
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

	committed := make(chan struct{})
	allowReturn := make(chan struct{})
	previousHook := workflowCompletionAfterCommitHook
	workflowCompletionAfterCommitHook = func() {
		close(committed)
		<-allowReturn
	}
	t.Cleanup(func() {
		workflowCompletionAfterCommitHook = previousHook
	})

	completed := make(chan error, 1)
	go func() {
		completed <- workerA.CompleteWorkflowWorkItem(ctx, wi)
	}()
	select {
	case <-committed:
	case <-time.After(15 * time.Second):
		t.Fatal("workflow completion did not reach the post-COMMIT hook")
	}

	restartPostgresChaos(t)
	close(allowReturn)
	if err := <-completed; err != nil {
		t.Fatalf("completion failed after its committed transaction survived PostgreSQL restart: %v", err)
	}

	workerB := newDurabilityBackend(t, 15*time.Second, 15*time.Second)
	if err := workerB.CompleteWorkflowWorkItem(ctx, wi); err == nil {
		t.Fatal("retry of a committed workflow completion succeeded after PostgreSQL restart")
	}
	if got := countRows(t, ctx, workerB, "History"); got != 1 {
		t.Fatalf("history rows after stale completion retry=%d, want 1", got)
	}
	if got := countRows(t, ctx, workerB, "NewEvents"); got != 0 {
		t.Fatalf("queued events after stale completion retry=%d, want 0", got)
	}
	if _, err := workerB.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("workflow item after stale completion retry: %v, want %v", err, errNoWorkItems)
	}
}

func requirePostgresChaos(t *testing.T) {
	t.Helper()
	if os.Getenv("STRIX_TEST_POSTGRES_CHAOS") != "1" {
		t.Skip("PostgreSQL chaos integration; set STRIX_TEST_POSTGRES_CHAOS=1")
	}
	if os.Getenv("STRIX_POSTGRES_CHAOS_COMPOSE_FILE") == "" || os.Getenv("STRIX_POSTGRES_CHAOS_PROJECT") == "" {
		t.Fatal("PostgreSQL chaos integration requires compose file and project environment variables")
	}
}

func restartPostgresChaos(t *testing.T) {
	t.Helper()
	composeFile := os.Getenv("STRIX_POSTGRES_CHAOS_COMPOSE_FILE")
	project := os.Getenv("STRIX_POSTGRES_CHAOS_PROJECT")
	runCompose(t, composeFile, project, "kill", "db")
	runCompose(t, composeFile, project, "up", "-d", "--wait", "db")
}

func runCompose(t *testing.T, composeFile, project string, args ...string) {
	t.Helper()
	commandArgs := []string{"compose", "-f", composeFile, "-p", project}
	commandArgs = append(commandArgs, args...)
	command := exec.Command("docker", commandArgs...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("docker %v: %v\n%s", commandArgs, err, output)
	}
}

func invokeExternalActivityEffect(t *testing.T, effectURL string) {
	t.Helper()
	response, err := http.Post(effectURL, "text/plain", nil)
	if err != nil {
		t.Fatalf("invoke external activity effect: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("external activity effect status=%d, want %d", response.StatusCode, http.StatusNoContent)
	}
}

func waitForActiveDelete(t *testing.T, ctx context.Context, observer *postgresBackend) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := observer.db.QueryRow(ctx, `SELECT COUNT(*) FROM pg_stat_activity
WHERE datname = current_database() AND state = 'active' AND query LIKE 'DELETE FROM NewEvents%'`).Scan(&count)
		if err != nil {
			t.Fatalf("inspect active PostgreSQL queries: %v", err)
		}
		if count > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("workflow completion did not reach the blocked NewEvents delete")
}

func removeCompletionPause(t *testing.T, ctx context.Context, be *postgresBackend, triggerName, functionName string) {
	t.Helper()
	if _, err := be.db.Exec(ctx, "DROP TRIGGER IF EXISTS "+triggerName+" ON NewEvents"); err != nil {
		t.Fatalf("drop completion pause trigger: %v", err)
	}
	if _, err := be.db.Exec(ctx, "DROP FUNCTION IF EXISTS "+functionName+"()"); err != nil {
		t.Fatalf("drop completion pause function: %v", err)
	}
}
