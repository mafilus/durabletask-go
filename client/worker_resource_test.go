package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResourceRetryBudgetNotifiesAfterCancelingListener(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var opens atomic.Int32
	var listenerCtx context.Context
	sidecar := &lifecycleSidecar{
		hello: func(ctx context.Context) error { listenerCtx = ctx; return nil },
		open: func(ctx context.Context) (grpc.ServerStreamingClient[protos.WorkItem], error) {
			opens.Add(1)
			return lifecycleStream{recv: func() (*protos.WorkItem, error) {
				return nil, status.Error(codes.ResourceExhausted, "quota or message limit")
			}}, nil
		},
	}
	notified := make(chan error, 2)
	c := &TaskHubGrpcClient{client: sidecar, logger: backend.DefaultLogger()}
	WithWorkItemListenerErrorHandler(func(err error) {
		require.ErrorIs(t, listenerCtx.Err(), context.Canceled)
		notified <- err
	})(c)
	require.NoError(t, c.StartWorkItemListener(parent, task.NewTaskRegistry()))
	select {
	case err := <-notified:
		require.ErrorIs(t, err, ErrWorkItemStreamExhausted)
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
	case <-parent.Done():
		t.Fatal("terminal error was not delivered to the host")
	}
	require.Equal(t, int32(workItemResourceErrorLimit), opens.Load())
	require.NoError(t, parent.Err(), "the host context remains owned by its caller")
	select {
	case err := <-notified:
		t.Fatalf("duplicate terminal notification: %v", err)
	default:
	}
}

func TestResourceRetryCancellationDoesNotNotifyFailure(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := make(chan struct{})
	var listenerCtx context.Context
	sidecar := &lifecycleSidecar{
		hello: func(ctx context.Context) error { listenerCtx = ctx; return nil },
		open: func(ctx context.Context) (grpc.ServerStreamingClient[protos.WorkItem], error) {
			return lifecycleStream{recv: func() (*protos.WorkItem, error) {
				close(failed)
				cancel()
				return nil, status.Error(codes.ResourceExhausted, "quota")
			}}, nil
		},
	}
	var notifications atomic.Int32
	c := &TaskHubGrpcClient{client: sidecar, logger: backend.DefaultLogger()}
	WithWorkItemListenerErrorHandler(func(error) { notifications.Add(1) })(c)
	require.NoError(t, c.StartWorkItemListener(parent, task.NewTaskRegistry()))
	<-failed
	requireListenerCanceled(t, listenerCtx)
	require.Zero(t, notifications.Load())
}
