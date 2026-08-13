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
	stops   atomic.Int32
	stopErr error
}

func (*lifecycleBackend) CreateTaskHub(context.Context) error { return nil }
func (b *lifecycleBackend) Start(context.Context) error       { b.starts.Add(1); return nil }
func (b *lifecycleBackend) Stop(context.Context) error        { b.stops.Add(1); return b.stopErr }

type lifecycleWorker[T WorkItem] struct {
	starts atomic.Int32
	stops  atomic.Int32
}

type deadlineWorker[T WorkItem] struct {
	release        chan struct{}
	completion     chan struct{}
	completionOnce atomic.Bool
}

type delayedDrainWorker[T WorkItem] struct {
	release        chan struct{}
	completion     chan struct{}
	completionOnce atomic.Bool
}

type unobservableDrainWorker[T WorkItem] struct {
	release chan struct{}
	drained chan struct{}
	once    atomic.Bool
	calls   atomic.Int32
}

func (*delayedDrainWorker[T]) Start(context.Context) {}

func (w *delayedDrainWorker[T]) StopAndDrain(ctx context.Context) error {
	if w.completionOnce.CompareAndSwap(false, true) {
		go func() {
			<-w.release
			close(w.completion)
		}()
	}
	select {
	case <-w.completion:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *delayedDrainWorker[T]) DrainCompletion() <-chan struct{} { return w.completion }

func (*deadlineWorker[T]) Start(context.Context) {}
func (w *deadlineWorker[T]) StopAndDrain(ctx context.Context) error {
	if w.completionOnce.CompareAndSwap(false, true) {
		go func() {
			<-w.release
			close(w.completion)
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.completion:
		return nil
	}
}
func (w *deadlineWorker[T]) DrainCompletion() <-chan struct{} { return w.completion }

func (*unobservableDrainWorker[T]) Start(context.Context) {}
func (w *unobservableDrainWorker[T]) StopAndDrain(ctx context.Context) error {
	w.calls.Add(1)
	if w.once.CompareAndSwap(false, true) {
		go func() {
			<-w.release
			close(w.drained)
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.drained:
		return nil
	}
}

type nonReentrantWorker[T WorkItem] struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (*nonReentrantWorker[T]) Start(context.Context) {}
func (w *nonReentrantWorker[T]) StopAndDrain(context.Context) error {
	if w.calls.Add(1) != 1 {
		return errors.New("concurrent StopAndDrain invocation")
	}
	close(w.entered)
	<-w.release
	return nil
}

type failingDrainWorker[T WorkItem] struct{}

func (*failingDrainWorker[T]) Start(context.Context) {}
func (*failingDrainWorker[T]) StopAndDrain(context.Context) error {
	return errors.New("drain failed")
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

func completedDrainCompletion() <-chan struct{} {
	completion := make(chan struct{})
	close(completion)
	return completion
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
	workflowWorker := &deadlineWorker[*WorkflowWorkItem]{release: make(chan struct{}), completion: make(chan struct{})}
	activityWorker := &deadlineWorker[*ActivityWorkItem]{release: make(chan struct{}), completion: make(chan struct{})}
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
		workflowWorker: &deadlineWorker[*WorkflowWorkItem]{release: workflowRelease, completion: make(chan struct{})},
		activityWorker: &deadlineWorker[*ActivityWorkItem]{release: activityRelease, completion: make(chan struct{})},
		logger:         DefaultLogger(),
		started:        true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, taskHub.Shutdown(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, taskHub.Start(context.Background()), ErrTaskHubStopping)

	close(workflowRelease)
	close(activityRelease)
	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.NoError(t, taskHub.Start(context.Background()))
	require.Equal(t, int32(1), be.starts.Load())
}

func TestTaskHubWorkerWaitsForActualDrainBeforeStoppingBackend(t *testing.T) {
	workflowRelease := make(chan struct{})
	activityRelease := make(chan struct{})
	be := &lifecycleBackend{}
	workflowWorker := &delayedDrainWorker[*WorkflowWorkItem]{release: workflowRelease, completion: make(chan struct{})}
	activityWorker := &delayedDrainWorker[*ActivityWorkItem]{release: activityRelease, completion: make(chan struct{})}
	taskHub := &taskHubWorker{
		backend:        be,
		workflowWorker: workflowWorker,
		activityWorker: activityWorker,
		logger:         DefaultLogger(),
		started:        true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, taskHub.Shutdown(ctx), context.DeadlineExceeded)
	require.Zero(t, be.stops.Load())
	require.ErrorIs(t, taskHub.Start(context.Background()), ErrTaskHubStopping)

	close(workflowRelease)
	close(activityRelease)
	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.Equal(t, int32(1), be.stops.Load())
	require.NoError(t, taskHub.Start(context.Background()))
	require.Equal(t, int32(1), be.starts.Load())
}

func TestTaskHubWorkerResumesUnobservableTimedOutDrain(t *testing.T) {
	workflowRelease := make(chan struct{})
	activityRelease := make(chan struct{})
	be := &lifecycleBackend{}
	taskHub := &taskHubWorker{
		backend:        be,
		workflowWorker: &unobservableDrainWorker[*WorkflowWorkItem]{release: workflowRelease, drained: make(chan struct{})},
		activityWorker: &unobservableDrainWorker[*ActivityWorkItem]{release: activityRelease, drained: make(chan struct{})},
		logger:         DefaultLogger(),
		started:        true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, taskHub.Shutdown(ctx), context.DeadlineExceeded)
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer retryCancel()
	require.ErrorIs(t, taskHub.Shutdown(retryCtx), context.DeadlineExceeded)
	require.Equal(t, int32(1), taskHub.workflowWorker.(*unobservableDrainWorker[*WorkflowWorkItem]).calls.Load())
	require.Equal(t, int32(1), taskHub.activityWorker.(*unobservableDrainWorker[*ActivityWorkItem]).calls.Load())
	close(workflowRelease)
	close(activityRelease)
	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.Equal(t, int32(1), be.stops.Load())
	require.NoError(t, taskHub.Start(context.Background()))
}

func TestTaskHubWorkerDoesNotStopAfterWorkerErrorWithoutDrainSignal(t *testing.T) {
	release := make(chan struct{})
	workflowWorker := &nonReentrantWorker[*WorkflowWorkItem]{entered: make(chan struct{}), release: release}
	taskHub := &taskHubWorker{
		backend:        &lifecycleBackend{},
		workflowWorker: workflowWorker,
		activityWorker: &failingDrainWorker[*ActivityWorkItem]{},
		logger:         DefaultLogger(),
		started:        true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, taskHub.Shutdown(ctx), context.DeadlineExceeded)
	select {
	case <-workflowWorker.entered:
	case <-time.After(time.Second):
		t.Fatal("workflow drain did not start")
	}
	require.ErrorIs(t, taskHub.Start(context.Background()), ErrTaskHubStopping)
	require.Equal(t, int32(1), workflowWorker.calls.Load())

	close(release)
	require.Error(t, taskHub.Shutdown(context.Background()))
	require.ErrorIs(t, taskHub.Start(context.Background()), ErrTaskHubStopping)
	require.Equal(t, int32(1), workflowWorker.calls.Load())
}
