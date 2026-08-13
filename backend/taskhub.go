package backend

import (
	"context"
	"sync"
)

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
	// Workers are drained below even when stopping the backend fails. Allow a
	// caller to retry the lifecycle instead of leaving the task hub wedged in a
	// started state with no running workers.
	defer func() { w.started = false }()

	w.logger.Info("workers stopping and draining...")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.workflowWorker.StopAndDrain()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.activityWorker.StopAndDrain()
	}()

	wg.Wait()
	w.logger.Info("finished stopping and draining workers!")

	w.logger.Info("backend stopping...")
	if err := w.backend.Stop(ctx); err != nil {
		return err
	}

	return nil
}
