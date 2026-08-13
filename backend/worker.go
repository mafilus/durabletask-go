package backend

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"
)

type TaskWorker[T WorkItem] interface {
	// Start starts background polling for the activity work items.
	Start(context.Context)

	// StopAndDrain stops the worker and waits for outstanding work items until
	// the context expires.
	StopAndDrain(context.Context) error
}

// drainCompletionWorker is an optional internal capability used to complete a
// timed-out task-hub shutdown once a worker has actually drained. It is kept
// separate from TaskWorker so existing external worker implementations remain
// source compatible.
type drainCompletionWorker interface {
	DrainCompletion() <-chan struct{}
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
	parallelLock chan struct{}

	mu        sync.Mutex
	running   bool
	cancel    context.CancelFunc
	pollDone  chan struct{}
	drainDone chan struct{}
	workWG    sync.WaitGroup
}

type NewTaskWorkerOptions func(*WorkerOptions)

type WorkerOptions struct {
	MaxParallelWorkItems *int32
}

const (
	minDefaultMaxParallelism int32 = 16
	maxDefaultMaxParallelism int32 = 64

	workerRetryInitialDelay = 100 * time.Millisecond
	workerRetryMaxDelay     = 5 * time.Second
	workerAbandonTimeout    = 5 * time.Second
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
		parallelLock: parallelLock,
	}
}

func (w *worker[T]) Name() string {
	return w.processor.Name()
}

func (w *worker[T]) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	pollDone := make(chan struct{})
	drainDone := make(chan struct{})
	w.running = true
	w.cancel = cancel
	w.pollDone = pollDone
	w.drainDone = drainDone
	w.mu.Unlock()

	go func() {
		defer close(pollDone)
		defer w.logger.Infof("%v: worker stopped", w.Name())

		var retryAttempt int
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
				if !waitForWorkerRetry(ctx, retryDelay(retryAttempt)) {
					return
				}
				retryAttempt++
				continue
			}

			retryAttempt = 0
			w.workWG.Add(1)
			go func() {
				defer func() {
					<-w.parallelLock
					w.workWG.Done()
				}()
				w.processWorkItem(ctx, wi)
			}()
		}
	}()
	go func() {
		// Waiting only after polling has stopped avoids racing Wait with Add.
		<-pollDone
		w.workWG.Wait()

		w.mu.Lock()
		if w.pollDone == pollDone {
			w.running = false
			w.cancel = nil
			w.pollDone = nil
			w.drainDone = nil
		}
		w.mu.Unlock()
		close(drainDone)
	}()
}

func (w *worker[T]) StopAndDrain(ctx context.Context) error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	cancel := w.cancel
	drainDone := w.drainDone
	w.mu.Unlock()

	cancel()
	select {
	case <-drainDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DrainCompletion returns a channel that closes when the current drain has
// completed. If the worker is already stopped, the returned channel is closed.
func (w *worker[T]) DrainCompletion() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.drainDone != nil {
		return w.drainDone
	}

	done := make(chan struct{})
	close(done)
	return done
}

func retryDelay(attempt int) time.Duration {
	delay := workerRetryInitialDelay
	for i := 0; i < attempt && delay < workerRetryMaxDelay; i++ {
		delay *= 2
		if delay > workerRetryMaxDelay {
			delay = workerRetryMaxDelay
		}
	}
	// Keep retries from synchronizing when several workers lose the backend at once.
	return delay/2 + time.Duration(rand.Float64()*float64(delay)/2)
}

func waitForWorkerRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *worker[T]) processWorkItem(ctx context.Context, wi T) {
	w.logger.Debugf("%v: processing work item: %s", w.Name(), wi)

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to process work item: %v", w.Name(), err)
		w.abandonWorkItem(wi)
		return
	}

	if err := w.processor.CompleteWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to complete work item: %v", w.Name(), err)
		w.abandonWorkItem(wi)
		return
	}

	w.logger.Debugf("%v: work item processed successfully", w.Name())
}

func (w *worker[T]) abandonWorkItem(wi T) {
	ctx, cancel := context.WithTimeout(context.Background(), workerAbandonTimeout)
	defer cancel()
	if err := w.processor.AbandonWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
	}
}
