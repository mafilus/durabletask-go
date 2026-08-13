package backend

import (
	"context"
	"sync"
	"time"
)

const taskHubCleanupTimeout = 5 * time.Second

type TaskHubWorker interface {
	// Start starts the backend and the configured internal workers.
	Start(context.Context) error

	// Shutdown stops the backend and all internal workers.
	Shutdown(context.Context) error
}

type taskHubWorker struct {
	backend        Backend
	workflowWorker TaskWorker[*WorkflowWorkItem]
	activityWorker TaskWorker[*ActivityWorkItem]
	logger         Logger
	mu             sync.Mutex
	started        bool
	stopping       bool
	shutdownDone   chan struct{}
	shutdownErr    error
}

func NewTaskHubWorker(be Backend, workflowWorker TaskWorker[*WorkflowWorkItem], activityWorker TaskWorker[*ActivityWorkItem], logger Logger) TaskHubWorker {
	return &taskHubWorker{
		backend:        be,
		workflowWorker: workflowWorker,
		activityWorker: activityWorker,
		logger:         logger,
	}
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return ErrTaskHubStopping
	}
	if w.started {
		return nil
	}

	if err := w.backend.CreateTaskHub(ctx); err != nil && err != ErrTaskHubExists {
		return err
	}

	if err := w.backend.Start(ctx); err != nil {
		return err
	}

	w.logger.Infof("worker started with backend %v", w.backend)

	w.workflowWorker.Start(ctx)
	w.activityWorker.Start(ctx)
	w.started = true
	return nil
}

func (w *taskHubWorker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return nil
	}
	if !w.stopping {
		w.stopping = true
		w.shutdownDone = make(chan struct{})
		w.shutdownErr = nil
		w.logger.Info("workers stopping and draining...")
		go w.finishShutdown(context.WithoutCancel(ctx))
	}
	done := w.shutdownDone
	w.mu.Unlock()

	select {
	case <-done:
		w.mu.Lock()
		err := w.shutdownErr
		w.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishShutdown uses a background context so a caller timeout only stops
// waiting for shutdown; it never interrupts a worker's drain. This preserves
// a single, non-overlapping StopAndDrain call for external TaskWorker
// implementations that do not expose a separate completion signal.
func (w *taskHubWorker) finishShutdown(drainCtx context.Context) {
	workflowDrained := make(chan error, 1)
	activityDrained := make(chan error, 1)
	go func() { workflowDrained <- w.workflowWorker.StopAndDrain(drainCtx) }()
	go func() { activityDrained <- w.activityWorker.StopAndDrain(drainCtx) }()

	workflowErr := <-workflowDrained
	activityErr := <-activityDrained
	if workflowErr != nil || activityErr != nil {
		err := workflowErr
		if err == nil {
			err = activityErr
		}
		w.finishShutdownWithError(err, false)
		return
	}

	w.logger.Info("finished stopping and draining workers!")
	w.logger.Info("backend stopping...")
	ctx, cancel := context.WithTimeout(context.Background(), taskHubCleanupTimeout)
	err := w.backend.Stop(ctx)
	cancel()
	w.finishShutdownWithError(err, true)
}

func (w *taskHubWorker) finishShutdownWithError(err error, stopped bool) {
	w.mu.Lock()
	w.shutdownErr = err
	if stopped {
		w.started = false
		w.stopping = false
	}
	close(w.shutdownDone)
	w.mu.Unlock()
}
