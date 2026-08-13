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
	defer w.mu.Unlock()
	if !w.started {
		return nil
	}
	if w.stopping {
		return ErrTaskHubStopping
	}
	w.stopping = true

	w.logger.Info("workers stopping and draining...")

	drained := make(chan error, 2)
	go func() { drained <- w.workflowWorker.StopAndDrain(ctx) }()
	go func() { drained <- w.activityWorker.StopAndDrain(ctx) }()
	for range 2 {
		select {
		case err := <-drained:
			if err != nil {
				go w.finishShutdownAfterDrain()
				return err
			}
		case <-ctx.Done():
			go w.finishShutdownAfterDrain()
			return ctx.Err()
		}
	}
	w.logger.Info("finished stopping and draining workers!")

	w.logger.Info("backend stopping...")
	err := w.backend.Stop(ctx)
	w.started = false
	w.stopping = false
	if err != nil {
		return err
	}

	return nil
}

// finishShutdownAfterDrain completes an interrupted shutdown without allowing
// a new Start to race workers that are still draining.
func (w *taskHubWorker) finishShutdownAfterDrain() {
	_ = w.workflowWorker.StopAndDrain(context.Background())
	_ = w.activityWorker.StopAndDrain(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), taskHubCleanupTimeout)
	defer cancel()
	if err := w.backend.Stop(ctx); err != nil {
		w.logger.Errorf("failed to stop backend after workers drained: %v", err)
	}

	w.mu.Lock()
	w.started = false
	w.stopping = false
	w.mu.Unlock()
}
