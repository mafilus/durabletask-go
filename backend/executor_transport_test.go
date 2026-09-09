package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type transportTestBackend struct {
	Backend
	mu         sync.Mutex
	waiters    map[string]chan *protos.WorkflowResponse
	activities map[string]chan *protos.ActivityResponse
	registered chan struct{}
}

func (b *transportTestBackend) WaitForActivityCompletion(req *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	key := GetActivityExecutionKey(req.GetWorkflowInstance().GetInstanceId(), req.TaskId)
	ch := make(chan *protos.ActivityResponse, 1)
	b.mu.Lock()
	b.activities[key] = ch
	b.mu.Unlock()
	return func(ctx context.Context) (*protos.ActivityResponse, error) {
		defer func() {
			b.mu.Lock()
			if b.activities[key] == ch {
				delete(b.activities, key)
			}
			b.mu.Unlock()
		}()
		select {
		case res := <-ch:
			return res, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (b *transportTestBackend) CompleteActivityTask(_ context.Context, res *protos.ActivityResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := b.activities[GetActivityExecutionKey(res.InstanceId, res.TaskId)]
	if ch == nil {
		return errors.New("missing backend activity waiter")
	}
	ch <- res
	return nil
}

func (b *transportTestBackend) WaitForWorkflowTaskCompletion(req *protos.WorkflowRequest) func(context.Context) (*protos.WorkflowResponse, error) {
	ch := make(chan *protos.WorkflowResponse, 1)
	b.mu.Lock()
	b.waiters[req.InstanceId] = ch
	b.mu.Unlock()
	if b.registered != nil {
		b.registered <- struct{}{}
	}
	return func(ctx context.Context) (*protos.WorkflowResponse, error) {
		defer func() {
			b.mu.Lock()
			if b.waiters[req.InstanceId] == ch {
				delete(b.waiters, req.InstanceId)
			}
			b.mu.Unlock()
		}()
		select {
		case res := <-ch:
			return res, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (b *transportTestBackend) CompleteWorkflowTask(_ context.Context, res *protos.WorkflowResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := b.waiters[res.InstanceId]
	if ch == nil {
		return errors.New("missing backend waiter")
	}
	ch <- res
	return nil
}

type transportTestStream struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*protos.WorkItem) error
}

func (s *transportTestStream) Context() context.Context       { return s.ctx }
func (s *transportTestStream) Send(wi *protos.WorkItem) error { return s.send(wi) }

func newTransportTestExecutor() *grpcExecutor {
	be := &transportTestBackend{waiters: make(map[string]chan *protos.WorkflowResponse), activities: make(map[string]chan *protos.ActivityResponse)}
	ex, _ := NewGrpcExecutor(be, DefaultLogger())
	return ex.(*grpcExecutor)
}

func transportReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("transport operation did not complete")
		var zero T
		return zero
	}
}

func startTransportStream(t *testing.T, g *grpcExecutor) (<-chan *protos.WorkItem, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan *protos.WorkItem, 4)
	done := make(chan error, 1)
	s := &transportTestStream{ctx: ctx, send: func(wi *protos.WorkItem) error { delivered <- wi; return nil }}
	go func() { done <- g.GetWorkItems(&protos.GetWorkItemsRequest{}, s) }()
	stop := func() { cancel(); transportReceive(t, done) }
	return delivered, stop
}

func executeTransportWorkflow(g *grpcExecutor, iid string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := g.ExecuteWorkflow(context.Background(), api.InstanceID(iid), nil, nil, ExecuteOptions{})
		done <- err
	}()
	return done
}

func TestTransportDisconnectFencesPreviousAttempt(t *testing.T) {
	g := newTransportTestExecutor()
	defer g.Shutdown(context.Background())
	itemsA, stopA := startTransportStream(t, g)
	doneA := executeTransportWorkflow(g, "same")
	a := transportReceive(t, itemsA)
	require.NotEmpty(t, a.CompletionToken)
	stopA()
	require.Error(t, transportReceive(t, doneA))
	itemsB, stopB := startTransportStream(t, g)
	defer stopB()
	doneB := executeTransportWorkflow(g, "same")
	b := transportReceive(t, itemsB)
	require.NotEqual(t, a.CompletionToken, b.CompletionToken)
	_, err := g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "same", CompletionToken: a.CompletionToken})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	select {
	case <-doneB:
		t.Fatal("old token completed replacement")
	default:
	}
	_, err = g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "other", CompletionToken: b.CompletionToken})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "same", CompletionToken: b.CompletionToken})
	require.NoError(t, err)
	require.NoError(t, transportReceive(t, doneB))
	_, err = g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "same", CompletionToken: b.CompletionToken})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestTransportCompletionDuringSend(t *testing.T) {
	g := newTransportTestExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamDone := make(chan error, 1)
	s := &transportTestStream{ctx: ctx, send: func(wi *protos.WorkItem) error {
		_, err := g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: wi.GetWorkflowRequest().InstanceId, CompletionToken: wi.CompletionToken})
		return err
	}}
	go func() { streamDone <- g.GetWorkItems(&protos.GetWorkItemsRequest{}, s) }()
	require.NoError(t, transportReceive(t, executeTransportWorkflow(g, "immediate")))
	require.NoError(t, g.Shutdown(context.Background()))
	require.Equal(t, codes.Canceled, status.Code(transportReceive(t, streamDone)))
}

func TestTransportLegacyAndStrictTokens(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "strict"}[strict], func(t *testing.T) {
			g := newTransportTestExecutor()
			if strict {
				WithRequireCompletionTokens()(g)
			}
			items, stop := startTransportStream(t, g)
			defer stop()
			done := executeTransportWorkflow(g, "legacy")
			wi := transportReceive(t, items)
			_, err := g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "legacy"})
			if strict {
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				_, err = g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "legacy", CompletionToken: wi.CompletionToken})
			}
			require.NoError(t, err)
			require.NoError(t, transportReceive(t, done))
		})
	}
}

func TestTransportShutdownUnblocksUndispatchedAndIsIdempotent(t *testing.T) {
	g := newTransportTestExecutor()
	registered := make(chan struct{}, 1)
	g.backend.(*transportTestBackend).registered = registered
	done := executeTransportWorkflow(g, "blocked")
	transportReceive(t, registered)
	require.NoError(t, g.Shutdown(context.Background()))
	require.NoError(t, g.Shutdown(context.Background()))
	require.Error(t, transportReceive(t, done))
	require.Error(t, transportReceive(t, executeTransportWorkflow(g, "after")))
}

func TestTransportCancellationDoesNotWaitForBackendCompletion(t *testing.T) {
	g := newTransportTestExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &pendingWorkflow{instanceID: "blocked", transportAttempt: transportAttempt{token: "token", cancel: cancel}}
	require.NoError(t, g.registerAttempt(g.pendingWorkflows, api.InstanceID("blocked"), p))
	require.True(t, p.claim("owner", "token"))
	entered, release := make(chan struct{}), make(chan struct{})
	completed := make(chan error, 1)
	go func() { completed <- p.complete("token", true, func() error { close(entered); <-release; return nil }) }()
	transportReceive(t, entered)
	// Neither a different stream's cleanup nor Shutdown may wait on this RPC.
	stopped := make(chan error, 1)
	go func() {
		p.stopForStream("other")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		stopped <- g.Shutdown(shutdownCtx)
	}()
	require.NoError(t, transportReceive(t, stopped))
	require.Error(t, ctx.Err())
	_, present := g.pendingWorkflows.Load(api.InstanceID("blocked"))
	require.True(t, present)
	close(release)
	require.NoError(t, transportReceive(t, completed))
}

func TestTransportRetirementReservesKeyDuringBackendCompletion(t *testing.T) {
	g := newTransportTestExecutor()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := api.InstanceID("reserved")
	p := &pendingWorkflow{transportAttempt: transportAttempt{token: "token", cancel: cancel}}
	require.NoError(t, g.registerAttempt(g.pendingWorkflows, key, p))
	require.True(t, p.claim("owner", "token"))
	entered, release := make(chan struct{}), make(chan struct{})
	completed := make(chan error, 1)
	go func() { completed <- p.complete("token", true, func() error { close(entered); <-release; return nil }) }()
	transportReceive(t, entered)
	p.stopForStream("owner")
	retired := make(chan struct{})
	go func() { p.finish(); g.pendingWorkflows.CompareAndDelete(key, p); close(retired) }()
	require.Equal(t, codes.AlreadyExists, status.Code(g.registerAttempt(g.pendingWorkflows, key, &pendingWorkflow{})))
	close(release)
	require.NoError(t, transportReceive(t, completed))
	transportReceive(t, retired)
	require.NoError(t, g.registerAttempt(g.pendingWorkflows, key, &pendingWorkflow{}))
}

func TestTransportActivityDisconnectAndTokenValidation(t *testing.T) {
	g := newTransportTestExecutor()
	execute := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := g.ExecuteActivity(context.Background(), "activity", &protos.HistoryEvent{EventId: 9, EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "test"}}}, ExecuteOptions{})
			done <- err
		}()
		return done
	}
	itemsA, stopA := startTransportStream(t, g)
	doneA := execute()
	a := transportReceive(t, itemsA)
	stopA()
	require.Error(t, transportReceive(t, doneA))
	itemsB, stopB := startTransportStream(t, g)
	defer stopB()
	doneB := execute()
	b := transportReceive(t, itemsB)
	_, err := g.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "activity", TaskId: 9, CompletionToken: a.CompletionToken})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = g.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "activity", TaskId: 10, CompletionToken: b.CompletionToken})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = g.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "activity", TaskId: 9, CompletionToken: b.CompletionToken})
	require.NoError(t, err)
	require.NoError(t, transportReceive(t, doneB))
}

func TestTransportSendFailureCancelsAttempt(t *testing.T) {
	g := newTransportTestExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamDone := make(chan error, 1)
	sendErr := errors.New("send failed")
	s := &transportTestStream{ctx: ctx, send: func(*protos.WorkItem) error { return sendErr }}
	go func() { streamDone <- g.GetWorkItems(&protos.GetWorkItemsRequest{}, s) }()
	done := executeTransportWorkflow(g, "send-fail")
	require.ErrorIs(t, transportReceive(t, streamDone), sendErr)
	require.Error(t, transportReceive(t, done))
	_, pending := g.pendingWorkflows.Load(api.InstanceID("send-fail"))
	require.False(t, pending)
	items, stop := startTransportStream(t, g)
	defer stop()
	retry := executeTransportWorkflow(g, "send-fail")
	wi := transportReceive(t, items)
	_, err := g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "send-fail", CompletionToken: wi.CompletionToken})
	require.NoError(t, err)
	require.NoError(t, transportReceive(t, retry))
}

func TestTransportDisconnectOnlyCancelsOwningStream(t *testing.T) {
	g := newTransportTestExecutor()
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	itemsA := make(chan *protos.WorkItem, 1)
	streamA := &transportTestStream{ctx: ctxA, send: func(wi *protos.WorkItem) error {
		itemsA <- wi
		// Keep A occupied so the next shared-queue item must go to B.
		<-ctxA.Done()
		return ctxA.Err()
	}}
	streamADone := make(chan error, 1)
	go func() { streamADone <- g.GetWorkItems(&protos.GetWorkItemsRequest{}, streamA) }()
	doneA := executeTransportWorkflow(g, "owner-a")
	transportReceive(t, itemsA)
	itemsB, stopB := startTransportStream(t, g)
	defer stopB()
	doneB := executeTransportWorkflow(g, "owner-b")
	b := transportReceive(t, itemsB)
	cancelA()
	transportReceive(t, streamADone)
	require.Error(t, transportReceive(t, doneA))
	_, err := g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "owner-b", CompletionToken: b.CompletionToken})
	require.NoError(t, err)
	require.NoError(t, transportReceive(t, doneB))
}

func TestTransportSendTimeoutCancelsAndAllowsRetry(t *testing.T) {
	g := newTransportTestExecutor()
	WithStreamSendTimeout(20 * time.Millisecond)(g)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	sendReturned := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	s := &transportTestStream{ctx: ctx, send: func(*protos.WorkItem) error {
		close(entered)
		<-release
		close(sendReturned)
		return nil
	}}
	streamDone := make(chan error, 1)
	go func() { streamDone <- g.GetWorkItems(&protos.GetWorkItemsRequest{}, s) }()
	done := executeTransportWorkflow(g, "send-timeout")
	transportReceive(t, entered)
	require.ErrorIs(t, transportReceive(t, streamDone), context.DeadlineExceeded)
	require.Error(t, transportReceive(t, done))
	_, pending := g.pendingWorkflows.Load(api.InstanceID("send-timeout"))
	require.False(t, pending)
	select {
	case <-sendReturned:
		t.Fatal("fake Send returned before being released")
	default:
	}
	// A real gRPC Send is released by RPC teardown. This fake deliberately
	// ignores teardown so that the test proves timeout cleanup independently.
	unblock()
	transportReceive(t, sendReturned)
	items, stop := startTransportStream(t, g)
	defer stop()
	retry := executeTransportWorkflow(g, "send-timeout")
	wi := transportReceive(t, items)
	_, err := g.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "send-timeout", CompletionToken: wi.CompletionToken})
	require.NoError(t, err)
	require.NoError(t, transportReceive(t, retry))
}
