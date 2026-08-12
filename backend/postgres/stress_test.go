package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
)

func TestStressConcurrentActivityPollersDoNotDoubleAcquire(t *testing.T) {
	ctx := context.Background()
	be := newDurabilityBackendWithMaxConns(t, 30*time.Second, 30*time.Second, 8)
	resetDurabilityTables(t, ctx, be)

	const workers = 8
	const perWorker = 8
	const total = workers * perWorker

	for i := 0; i < total; i++ {
		insertActivity(t, ctx, be, fmt.Sprintf("stress-%03d", i), int32(i+1))
	}

	acquired := make(chan int64, total)
	errs := make(chan error, total)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				wi, err := be.getActivityWorkItem(ctx)
				if err != nil {
					errs <- err
					return
				}
				acquired <- wi.SequenceNumber
			}
		}()
	}

	wg.Wait()
	close(acquired)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent poller failed: %v", err)
		}
	}

	seen := make(map[int64]struct{}, total)
	for seq := range acquired {
		if _, exists := seen[seq]; exists {
			t.Fatalf("activity sequence %d acquired more than once", seq)
		}
		seen[seq] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("unique acquisitions=%d, want %d", len(seen), total)
	}
}

func TestStressConcurrentWorkflowPollersDoNotDoubleAcquire(t *testing.T) {
	ctx := context.Background()
	be := newDurabilityBackendWithMaxConns(t, 30*time.Second, 30*time.Second, 8)

	const workers = 8
	const rounds = 20

	for round := 0; round < rounds; round++ {
		resetDurabilityTables(t, ctx, be)
		instanceID := fmt.Sprintf("workflow-stress-%03d", round)
		insertWorkflowWithEvent(t, ctx, be, instanceID, nil, int32(round+1))

		start := make(chan struct{})
		results := make(chan *backend.WorkflowWorkItem, workers)
		errs := make(chan error, workers)
		var wg sync.WaitGroup

		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				wi, err := be.GetWorkflowWorkItem(ctx)
				if err != nil {
					errs <- err
					return
				}
				results <- wi
			}()
		}

		close(start)
		wg.Wait()
		close(results)
		close(errs)

		acquisitions := 0
		for wi := range results {
			if string(wi.InstanceID) != instanceID {
				t.Fatalf("round %d: acquired instance %q, want %q", round, wi.InstanceID, instanceID)
			}
			acquisitions++
		}
		for err := range errs {
			if !errors.Is(err, errNoWorkItems) {
				t.Fatalf("round %d: concurrent workflow poller failed: %v", round, err)
			}
		}

		if acquisitions != 1 {
			t.Fatalf("round %d: workflow acquired %d times, want 1", round, acquisitions)
		}
	}
}

func TestWorkflowEventBatchesAreFIFOAndComplete(t *testing.T) {
	ctx := context.Background()
	be := newDurabilityBackend(t, 2*time.Second, 2*time.Second)
	resetDurabilityTables(t, ctx, be)

	const totalEvents = 1001
	const batchSize = 1000
	const instanceID = "workflow-event-batches"

	tx, err := be.db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin workflow event setup: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO Instances (InstanceID, ExecutionID, Name, RuntimeStatus) VALUES ($1, $2, $3, 'PENDING')`, instanceID, "execution-1", "durability-test"); err != nil {
		t.Fatalf("insert workflow instance: %v", err)
	}
	for eventID := int32(1); eventID <= totalEvents; eventID++ {
		payload, err := backend.MarshalHistoryEvent(historyEvent(eventID))
		if err != nil {
			t.Fatalf("marshal workflow event %d: %v", eventID, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)", instanceID, payload); err != nil {
			t.Fatalf("insert workflow event %d: %v", eventID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit workflow event setup: %v", err)
	}

	for firstEventID := int32(1); firstEventID <= totalEvents; firstEventID += batchSize {
		wi, err := be.GetWorkflowWorkItem(ctx)
		if err != nil {
			t.Fatalf("get workflow batch starting at event %d: %v", firstEventID, err)
		}

		expectedCount := batchSize
		if remaining := totalEvents - int(firstEventID) + 1; remaining < expectedCount {
			expectedCount = remaining
		}
		if got := len(wi.NewEvents); got != expectedCount {
			t.Fatalf("batch starting at event %d has %d events, want %d", firstEventID, got, expectedCount)
		}
		for index, event := range wi.NewEvents {
			wantEventID := firstEventID + int32(index)
			if gotEventID := event.GetEventId(); gotEventID != wantEventID {
				t.Fatalf("batch starting at event %d, index %d: event ID=%d, want %d", firstEventID, index, gotEventID, wantEventID)
			}
		}

		wi.State = &protos.WorkflowRuntimeState{}
		if err := be.CompleteWorkflowWorkItem(ctx, wi); err != nil {
			t.Fatalf("complete workflow batch starting at event %d: %v", firstEventID, err)
		}
	}

	if got := countRows(t, ctx, be, "NewEvents"); got != 0 {
		t.Fatalf("remaining workflow events=%d, want 0", got)
	}
	if _, err := be.GetWorkflowWorkItem(ctx); !errors.Is(err, errNoWorkItems) {
		t.Fatalf("workflow item after all batches: %v, want %v", err, errNoWorkItems)
	}
}

func newDurabilityBackendWithMaxConns(t *testing.T, workflowLease, activityLease time.Duration, maxConns int32) *postgresBackend {
	t.Helper()

	port64, err := strconv.ParseUint(getenv("PGPORT", "5432"), 10, 16)
	if err != nil {
		t.Fatalf("invalid PGPORT: %v", err)
	}
	opts := NewPostgresOptions(
		getenv("PGHOST", "127.0.0.1"),
		uint16(port64),
		getenv("PGDATABASE", "durabletask"),
		getenv("PGUSER", "postgres"),
		getenv("PGPASSWORD", "postgres"),
	)
	opts.WorkflowLockTimeout = workflowLease
	opts.ActivityLockTimeout = activityLease
	opts.PgOptions.MaxConns = maxConns

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
