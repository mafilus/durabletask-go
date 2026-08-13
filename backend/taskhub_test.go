package backend

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type shutdownBackend struct {
	Backend
	workersStopped *atomic.Int32
}

func (b *shutdownBackend) Stop(context.Context) error {
	if b.workersStopped.Load() != 2 {
		return fmt.Errorf("backend stopped before all workers drained")
	}
	return nil
}

type shutdownWorker[T WorkItem] struct {
	stopped *atomic.Int32
}

type lifecycleBackend struct {
	Backend
	starts  atomic.Int32
	stopErr error
}

func (*lifecycleBackend) CreateTaskHub(context.Context) error { return nil }
func (b *lifecycleBackend) Start(context.Context) error       { b.starts.Add(1); return nil }
func (b *lifecycleBackend) Stop(context.Context) error        { return b.stopErr }

type lifecycleWorker[T WorkItem] struct {
	starts atomic.Int32
	stops  atomic.Int32
}

type deadlineWorker[T WorkItem] struct {
	release chan struct{}
}

func (*deadlineWorker[T]) Start(context.Context) {}
func (w *deadlineWorker[T]) StopAndDrain(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.release:
		return nil
	}
}

func (w *lifecycleWorker[T]) Start(context.Context) { w.starts.Add(1) }
func (w *lifecycleWorker[T]) StopAndDrain(context.Context) error {
	w.stops.Add(1)
	return nil
}

func (*shutdownWorker[T]) Start(context.Context) {}

func (w *shutdownWorker[T]) StopAndDrain(context.Context) error {
	w.stopped.Add(1)
	return nil
}

func TestTaskHubWorkerDrainsWorkersBeforeStoppingBackend(t *testing.T) {
	var workersStopped atomic.Int32
	taskHub := &taskHubWorker{
		backend:        &shutdownBackend{workersStopped: &workersStopped},
		workflowWorker: &shutdownWorker[*WorkflowWorkItem]{stopped: &workersStopped},
		activityWorker: &shutdownWorker[*ActivityWorkItem]{stopped: &workersStopped},
		logger:         DefaultLogger(),
		started:        true,
	}

	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.Equal(t, int32(2), workersStopped.Load())
}

func TestTaskHubWorkerStartAndShutdownAreIdempotent(t *testing.T) {
	be := &lifecycleBackend{}
	workflowWorker := &lifecycleWorker[*WorkflowWorkItem]{}
	activityWorker := &lifecycleWorker[*ActivityWorkItem]{}
	taskHub := NewTaskHubWorker(be, workflowWorker, activityWorker, DefaultLogger())

	require.NoError(t, taskHub.Start(context.Background()))
	require.NoError(t, taskHub.Start(context.Background()))
	require.Equal(t, int32(1), be.starts.Load())
	require.Equal(t, int32(1), workflowWorker.starts.Load())
	require.Equal(t, int32(1), activityWorker.starts.Load())

	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.Equal(t, int32(1), workflowWorker.stops.Load())
	require.Equal(t, int32(1), activityWorker.stops.Load())
}

func TestTaskHubWorkerCanRestartAfterBackendStopFailure(t *testing.T) {
	be := &lifecycleBackend{stopErr: errors.New("backend stop failed")}
	workflowWorker := &lifecycleWorker[*WorkflowWorkItem]{}
	activityWorker := &lifecycleWorker[*ActivityWorkItem]{}
	taskHub := NewTaskHubWorker(be, workflowWorker, activityWorker, DefaultLogger())

	require.NoError(t, taskHub.Start(context.Background()))
	require.Error(t, taskHub.Shutdown(context.Background()))
	require.NoError(t, taskHub.Start(context.Background()))
	require.Equal(t, int32(2), be.starts.Load())
	require.Equal(t, int32(2), workflowWorker.starts.Load())
	require.Equal(t, int32(2), activityWorker.starts.Load())
}

func TestTaskHubWorkerShutdownHonorsDrainDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	workflowWorker := &deadlineWorker[*WorkflowWorkItem]{release: make(chan struct{})}
	activityWorker := &deadlineWorker[*ActivityWorkItem]{release: make(chan struct{})}
	taskHub := &taskHubWorker{
		backend:        &lifecycleBackend{},
		workflowWorker: workflowWorker,
		activityWorker: activityWorker,
		logger:         DefaultLogger(),
		started:        true,
	}

	require.ErrorIs(t, taskHub.Shutdown(ctx), context.DeadlineExceeded)
	close(workflowWorker.release)
	close(activityWorker.release)
}

func TestTaskHubWorkerRefusesStartUntilTimedOutDrainCompletes(t *testing.T) {
	workflowRelease := make(chan struct{})
	activityRelease := make(chan struct{})
	be := &lifecycleBackend{}
	taskHub := &taskHubWorker{
		backend:        be,
		workflowWorker: &deadlineWorker[*WorkflowWorkItem]{release: workflowRelease},
		activityWorker: &deadlineWorker[*ActivityWorkItem]{release: activityRelease},
		logger:         DefaultLogger(),
		started:        true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, taskHub.Shutdown(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, taskHub.Start(context.Background()), ErrTaskHubStopping)

	close(workflowRelease)
	close(activityRelease)
	require.Eventually(t, func() bool {
		return taskHub.Start(context.Background()) == nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), be.starts.Load())
}
