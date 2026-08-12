package backend

import (
	"context"
	"runtime"
	"sync"
)

type TaskWorker[T WorkItem] interface {
	// Start starts background polling for the activity work items.
	Start(context.Context)

	// StopAndDrain stops the worker and waits for all outstanding work items to finish.
	StopAndDrain()
}

type TaskProcessor[T WorkItem] interface {
	Name() string
	ProcessWorkItem(context.Context, T) error
	NextWorkItem(context.Context) (T, error)
	AbandonWorkItem(context.Context, T) error
	CompleteWorkItem(context.Context, T) error
}

type worker[T WorkItem] struct {
	logger Logger

	processor    TaskProcessor[T]
	closeCh      chan struct{}
	wg           sync.WaitGroup
	workItems    chan T
	parallelLock chan struct{}
}

type NewTaskWorkerOptions func(*WorkerOptions)

type WorkerOptions struct {
	MaxParallelWorkItems *int32
}

const (
	minDefaultMaxParallelism int32 = 16
	maxDefaultMaxParallelism int32 = 64
)

// DefaultMaxParallelism returns the default limit used by workflow and activity
// workers. The limit keeps the number of in-flight work items bounded while
// allowing enough concurrency for workloads that spend time waiting on I/O.
func DefaultMaxParallelism() int32 {
	maxParallelism := int32(4 * runtime.GOMAXPROCS(0))
	if maxParallelism < minDefaultMaxParallelism {
		return minDefaultMaxParallelism
	}
	if maxParallelism > maxDefaultMaxParallelism {
		return maxDefaultMaxParallelism
	}
	return maxParallelism
}

func NewWorkerOptions() *WorkerOptions {
	maxParallelism := DefaultMaxParallelism()
	return &WorkerOptions{MaxParallelWorkItems: &maxParallelism}
}

// WithMaxParallelism overrides the default concurrency limit for a task worker.
func WithMaxParallelism(n int32) NewTaskWorkerOptions {
	return func(o *WorkerOptions) {
		o.MaxParallelWorkItems = &n
	}
}

func NewTaskWorker[T WorkItem](p TaskProcessor[T], logger Logger, opts ...NewTaskWorkerOptions) TaskWorker[T] {
	options := NewWorkerOptions()
	for _, configure := range opts {
		configure(options)
	}

	if options.MaxParallelWorkItems == nil || *options.MaxParallelWorkItems <= 0 {
		panic("max parallelism must be greater than zero")
	}
	parallelLock := make(chan struct{}, *options.MaxParallelWorkItems)

	return &worker[T]{
		processor:    p,
		logger:       logger,
		workItems:    make(chan T),
		parallelLock: parallelLock,
		closeCh:      make(chan struct{}),
	}
}

func (w *worker[T]) Name() string {
	return w.processor.Name()
}

func (w *worker[T]) Start(ctx context.Context) {
	w.wg.Add(2)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer w.wg.Done()
		defer cancel()

		select {
		case <-w.closeCh:
		case <-ctx.Done():
		}
	}()

	go func() {
		defer w.wg.Done()
		defer w.logger.Infof("%v: worker stopped", w.Name())

		for {

			select {
			case w.parallelLock <- struct{}{}:
			case <-ctx.Done():
				return
			}

			wi, err := w.processor.NextWorkItem(ctx)
			if err != nil {
				<-w.parallelLock

				if ctx.Err() != nil {
					return
				}

				w.logger.Errorf("%v: failed to get next work item: %v", w.Name(), err)
				continue
			}

			w.wg.Add(1)
			go func() {
				defer func() {
					<-w.parallelLock
					w.wg.Done()
				}()
				w.processWorkItem(ctx, wi)
			}()
		}
	}()
}

func (w *worker[T]) StopAndDrain() {
	close(w.closeCh)
	w.wg.Wait()
}

func (w *worker[T]) processWorkItem(ctx context.Context, wi T) {
	w.logger.Debugf("%v: processing work item: %s", w.Name(), wi)

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to process work item: %v", w.Name(), err)
		if err = w.processor.AbandonWorkItem(context.Background(), wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
		}
		return
	}

	if err := w.processor.CompleteWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to complete work item: %v", w.Name(), err)
		if err = w.processor.AbandonWorkItem(context.Background(), wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
		}
		return
	}

	w.logger.Debugf("%v: work item processed successfully", w.Name())
}
