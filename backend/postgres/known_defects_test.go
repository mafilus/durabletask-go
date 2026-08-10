package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func requireKnownDefects(t *testing.T) {
	t.Helper()
	if os.Getenv("STRIX_RUN_KNOWN_DEFECTS") != "1" {
		t.Skip("known-defect reproduction; set STRIX_RUN_KNOWN_DEFECTS=1")
	}
}

func TestKnownDefectActivityLockTimeoutIsApplied(t *testing.T) {
	requireKnownDefects(t)
	ctx := context.Background()

	// The workflow lease is deliberately much longer than the activity lease.
	// Correct behavior allows worker B to reacquire after the activity lease.
	workerA := newDurabilityBackend(t, 500*time.Millisecond, 50*time.Millisecond)
	resetDurabilityTables(t, ctx, workerA)
	workerB := newDurabilityBackend(t, 500*time.Millisecond, 50*time.Millisecond)

	insertActivity(t, ctx, workerA, durabilityInstanceID, 1)
	first, err := workerA.getActivityWorkItem(ctx)
	if err != nil {
		t.Fatalf("worker A failed to acquire activity: %v", err)
	}

	time.Sleep(120 * time.Millisecond)
	second, err := workerB.getActivityWorkItem(ctx)
	if errors.Is(err, errNoWorkItems) {
		t.Fatalf("activity did not expire after ActivityLockTimeout; first lease owner=%q", first.LockedBy)
	}
	if err != nil {
		t.Fatalf("worker B failed to reacquire activity: %v", err)
	}
	if second.SequenceNumber != first.SequenceNumber {
		t.Fatalf("reacquired sequence=%d, want %d", second.SequenceNumber, first.SequenceNumber)
	}
}

func TestKnownDefectDequeueQueriesDeclareFIFOOrdering(t *testing.T) {
	requireKnownDefects(t)

	sourceBytes, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatalf("read postgres.go: %v", err)
	}
	source := string(sourceBytes)

	activity := functionRegion(t, source, "func (be *postgresBackend) getActivityWorkItem", "func (be *postgresBackend) CompleteActivityWorkItem")
	if !strings.Contains(activity, "ORDER BY SequenceNumber ASC") {
		t.Error("NewTasks dequeue does not explicitly ORDER BY SequenceNumber ASC before LIMIT")
	}

	workflow := functionRegion(t, source, "func (be *postgresBackend) GetWorkflowWorkItem", "func (be *postgresBackend) getActivityWorkItem")
	if !strings.Contains(workflow, "ORDER BY SequenceNumber ASC") {
		t.Error("NewEvents dequeue does not explicitly ORDER BY SequenceNumber ASC before LIMIT/RETURNING")
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
