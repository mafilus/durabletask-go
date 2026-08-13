package backend

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

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

func (w *lifecycleWorker[T]) Start(context.Context) { w.starts.Add(1) }
func (w *lifecycleWorker[T]) StopAndDrain()         { w.stops.Add(1) }

func (*shutdownWorker[T]) Start(context.Context) {}

func (w *shutdownWorker[T]) StopAndDrain() {
	w.stopped.Add(1)
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
