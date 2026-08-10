package postgres

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/backend"
)

func TestStressConcurrentActivityPollersDoNotDoubleAcquire(t *testing.T) {
	ctx := context.Background()
	be := newDurabilityBackendWithMaxConns(t, 2*time.Second, 2*time.Second, 8)
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
