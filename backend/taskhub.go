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

	workflowCompletion := drainCompletion[*WorkflowWorkItem](w.workflowWorker)
	activityCompletion := drainCompletion[*ActivityWorkItem](w.activityWorker)
	workflowDrained := make(chan error, 1)
	activityDrained := make(chan error, 1)
	go func() { workflowDrained <- w.workflowWorker.StopAndDrain(ctx) }()
	go func() { activityDrained <- w.activityWorker.StopAndDrain(ctx) }()
	for remaining := 2; remaining > 0; {
		select {
		case err := <-workflowDrained:
			workflowDrained = nil
			remaining--
			if err != nil {
				w.finishShutdownWhenDrained(workflowDrained, activityDrained, workflowCompletion, activityCompletion)
				return err
			}
		case err := <-activityDrained:
			activityDrained = nil
			remaining--
			if err != nil {
				w.finishShutdownWhenDrained(workflowDrained, activityDrained, workflowCompletion, activityCompletion)
				return err
			}
		case <-ctx.Done():
			w.finishShutdownWhenDrained(workflowDrained, activityDrained, workflowCompletion, activityCompletion)
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

func drainCompletion[T WorkItem](worker TaskWorker[T]) <-chan struct{} {
	if worker, ok := worker.(drainCompletionWorker); ok {
		return worker.DrainCompletion()
	}
	return nil
}

// finishShutdownWhenDrained arranges deferred backend shutdown only when every
// worker provides a real drain-completion signal. TaskWorker deliberately does
// not require that capability for source compatibility, so an external worker
// that returns early from StopAndDrain cannot be mistaken for a drained worker.
func (w *taskHubWorker) finishShutdownWhenDrained(workflowDrained, activityDrained <-chan error, workflowCompletion, activityCompletion <-chan struct{}) {
	if workflowCompletion == nil || activityCompletion == nil {
		w.logger.Warn("not stopping backend after interrupted drain: a worker does not expose drain completion")
		return
	}
	go w.finishShutdownAfterDrain(workflowDrained, activityDrained, workflowCompletion, activityCompletion)
}

// finishShutdownAfterDrain completes an interrupted shutdown without allowing
// a new Start to race workers that are still draining.
func (w *taskHubWorker) finishShutdownAfterDrain(workflowDrained, activityDrained <-chan error, workflowCompletion, activityCompletion <-chan struct{}) {
	if workflowDrained != nil {
		<-workflowDrained
	}
	if activityDrained != nil {
		<-activityDrained
	}
	if workflowCompletion != nil {
		<-workflowCompletion
	}
	if activityCompletion != nil {
		<-activityCompletion
	}

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
