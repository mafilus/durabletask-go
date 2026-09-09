package client

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type lifecycleSidecar struct {
	protos.TaskHubSidecarServiceClient
	hello func(context.Context) error
	open  func(context.Context) (grpc.ServerStreamingClient[protos.WorkItem], error)
}

func (s *lifecycleSidecar) Hello(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.hello(ctx)
}

func (s *lifecycleSidecar) GetWorkItems(ctx context.Context, _ *protos.GetWorkItemsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[protos.WorkItem], error) {
	return s.open(ctx)
}

type lifecycleStream struct {
	grpc.ServerStreamingClient[protos.WorkItem]
	recv func() (*protos.WorkItem, error)
}

func (s lifecycleStream) Recv() (*protos.WorkItem, error) { return s.recv() }

func requireListenerCanceled(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("listener resource context was not canceled")
	}
}

func TestListenerFailedStartupCancelsResources(t *testing.T) {
	for _, phase := range []string{"hello", "get-work-items"} {
		t.Run(phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			var listenerCtx, streamCtx context.Context
			sidecar := &lifecycleSidecar{
				hello: func(ctx context.Context) error {
					listenerCtx = ctx
					if phase == "hello" {
						return errors.New("offline")
					}
					return nil
				},
				open: func(ctx context.Context) (grpc.ServerStreamingClient[protos.WorkItem], error) {
					streamCtx = ctx
					return nil, errors.New("cannot open stream")
				},
			}
			client := &TaskHubGrpcClient{client: sidecar, logger: backend.DefaultLogger()}
			for i := 0; i < 10; i++ {
				require.Error(t, client.StartWorkItemListener(parent, task.NewTaskRegistry()))
				requireListenerCanceled(t, listenerCtx) // Also owns the janitor.
				if streamCtx != nil {
					requireListenerCanceled(t, streamCtx)
				}
				require.NoError(t, parent.Err())
			}
		})
	}
}

func TestListenerTerminalReceiveCancelsResources(t *testing.T) {
	for _, callerCancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-retriable", true: "caller-cancel"}[callerCancel], func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			var listenerCtx, streamCtx context.Context
			sidecar := &lifecycleSidecar{
				hello: func(ctx context.Context) error { listenerCtx = ctx; return nil },
				open: func(ctx context.Context) (grpc.ServerStreamingClient[protos.WorkItem], error) {
					streamCtx = ctx
					return lifecycleStream{recv: func() (*protos.WorkItem, error) {
						if callerCancel {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						return nil, errors.New("non-grpc permanent receive failure")
					}}, nil
				},
			}
			client := &TaskHubGrpcClient{client: sidecar, logger: backend.DefaultLogger()}
			require.NoError(t, client.StartWorkItemListener(parent, task.NewTaskRegistry()))
			if callerCancel {
				cancel()
			}
			requireListenerCanceled(t, listenerCtx)
			requireListenerCanceled(t, streamCtx)
			if !callerCancel {
				require.NoError(t, parent.Err())
			}
		})
	}
}

func TestListenerReconnectKeepsListenerAliveAndCancelsOldStream(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	contexts := make(chan context.Context, 2)
	listenerContexts := make(chan context.Context, 2)
	opens := 0 // Accessed serially by initialization and then receive loop.
	sidecar := &lifecycleSidecar{
		hello: func(ctx context.Context) error { listenerContexts <- ctx; return nil },
		open: func(ctx context.Context) (grpc.ServerStreamingClient[protos.WorkItem], error) {
			opens++
			first := opens == 1
			contexts <- ctx
			return lifecycleStream{recv: func() (*protos.WorkItem, error) {
				if first {
					return nil, io.EOF
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}}, nil
		},
	}
	client := &TaskHubGrpcClient{client: sidecar, logger: backend.DefaultLogger()}
	require.NoError(t, client.StartWorkItemListener(parent, task.NewTaskRegistry()))
	firstCtx := <-contexts
	var secondCtx context.Context
	select {
	case secondCtx = <-contexts:
	case <-time.After(time.Second):
		t.Fatal("no reconnect")
	}
	requireListenerCanceled(t, firstCtx)
	require.NoError(t, secondCtx.Err())
	listenerCtx := <-listenerContexts
	require.NoError(t, listenerCtx.Err())
	cancel()
	requireListenerCanceled(t, secondCtx)
	requireListenerCanceled(t, listenerCtx)
}
