package backend

import (
	"context"
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
	}

	require.NoError(t, taskHub.Shutdown(context.Background()))
	require.Equal(t, int32(2), workersStopped.Load())
}
