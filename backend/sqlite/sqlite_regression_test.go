package sqlite

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
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

func TestWorkflowDequeueDeclaresFIFOOrdering(t *testing.T) {
	source, err := os.ReadFile("sqlite.go")
	if err != nil {
		t.Fatalf("read sqlite.go: %v", err)
	}
	workflow := string(source)
	if !strings.Contains(workflow, "ORDER BY [SequenceNumber] ASC") {
		t.Fatal("NewEvents dequeue does not explicitly ORDER BY SequenceNumber ASC")
	}
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
