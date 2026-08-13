package backend

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultMaxParallelism(t *testing.T) {
	want := int32(4 * runtime.GOMAXPROCS(0))
	if want < minDefaultMaxParallelism {
		want = minDefaultMaxParallelism
	}
	if want > maxDefaultMaxParallelism {
		want = maxDefaultMaxParallelism
	}
	require.Equal(t, want, DefaultMaxParallelism())
}

type blockingActivityProcessor struct {
	calls   atomic.Int32
	started chan struct{}
	once    sync.Once
}

func (*blockingActivityProcessor) Name() string { return "test" }

func (p *blockingActivityProcessor) NextWorkItem(ctx context.Context) (*ActivityWorkItem, error) {
	p.calls.Add(1)
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingActivityProcessor) ProcessWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*blockingActivityProcessor) AbandonWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*blockingActivityProcessor) CompleteWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}

type failingActivityProcessor struct {
	calls   atomic.Int32
	started chan struct{}
	once    sync.Once
}

func (*failingActivityProcessor) Name() string { return "test" }

func (p *failingActivityProcessor) NextWorkItem(context.Context) (*ActivityWorkItem, error) {
	p.calls.Add(1)
	p.once.Do(func() { close(p.started) })
	return nil, errors.New("backend unavailable")
}

func (*failingActivityProcessor) ProcessWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*failingActivityProcessor) AbandonWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*failingActivityProcessor) CompleteWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}

type stuckWorkItem struct{}

func (stuckWorkItem) String() string   { return "stuck" }
func (stuckWorkItem) IsWorkItem() bool { return true }

type stuckProcessor struct {
	pollOnce sync.Once
	started  chan struct{}
	release  chan struct{}
}

func (*stuckProcessor) Name() string { return "stuck" }
func (p *stuckProcessor) NextWorkItem(ctx context.Context) (stuckWorkItem, error) {
	emit := false
	p.pollOnce.Do(func() { emit = true })
	if emit {
		return stuckWorkItem{}, nil
	}
	<-ctx.Done()
	return stuckWorkItem{}, ctx.Err()
}
func (p *stuckProcessor) ProcessWorkItem(context.Context, stuckWorkItem) error {
	close(p.started)
	<-p.release
	return nil
}
func (*stuckProcessor) AbandonWorkItem(context.Context, stuckWorkItem) error  { return nil }
func (*stuckProcessor) CompleteWorkItem(context.Context, stuckWorkItem) error { return nil }

func TestTaskWorkerStartAndStopAreIdempotent(t *testing.T) {
	processor := &blockingActivityProcessor{started: make(chan struct{})}
	worker := NewTaskWorker[*ActivityWorkItem](processor, DefaultLogger(), WithMaxParallelism(1))
	worker.Start(context.Background())
	worker.Start(context.Background())
	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start polling")
	}

	require.NoError(t, worker.StopAndDrain(context.Background()))
	require.NoError(t, worker.StopAndDrain(context.Background()))
	require.Equal(t, int32(1), processor.calls.Load())
}

func TestTaskWorkerBacksOffAndCancellationInterruptsRetry(t *testing.T) {
	processor := &failingActivityProcessor{started: make(chan struct{})}
	worker := NewTaskWorker[*ActivityWorkItem](processor, DefaultLogger(), WithMaxParallelism(1))
	worker.Start(context.Background())
	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt to poll")
	}

	// The first retry has at least half the initial delay, so it must not spin.
	time.Sleep(workerRetryInitialDelay / 4)
	require.Equal(t, int32(1), processor.calls.Load())

	started := time.Now()
	require.NoError(t, worker.StopAndDrain(context.Background()))
	require.Less(t, time.Since(started), workerRetryInitialDelay/2)
}

func TestTaskWorkerStopAndDrainHonorsDeadline(t *testing.T) {
	processor := &stuckProcessor{started: make(chan struct{}), release: make(chan struct{})}
	worker := NewTaskWorker[stuckWorkItem](processor, DefaultLogger(), WithMaxParallelism(1))
	worker.Start(context.Background())
	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start processing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, worker.StopAndDrain(ctx), context.DeadlineExceeded)

	close(processor.release)
	require.Eventually(t, func() bool {
		return worker.StopAndDrain(context.Background()) == nil
	}, time.Second, 10*time.Millisecond)
}

func TestNewTaskWorkerLimitsParallelismByDefault(t *testing.T) {
	worker := NewTaskWorker[*ActivityWorkItem](nil, nil).(*worker[*ActivityWorkItem])
	require.Equal(t, int(DefaultMaxParallelism()), cap(worker.parallelLock))
}

func TestNewTaskWorkerAllowsExplicitParallelismOverride(t *testing.T) {
	worker := NewTaskWorker[*ActivityWorkItem](nil, nil, WithMaxParallelism(3)).(*worker[*ActivityWorkItem])
	require.Equal(t, 3, cap(worker.parallelLock))
}
