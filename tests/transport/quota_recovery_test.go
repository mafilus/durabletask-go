package transport_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/client"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type transientQuotaServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	calls atomic.Int32
	ended chan struct{}
}

func (s *transientQuotaServer) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (s *transientQuotaServer) GetWorkItems(_ *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	if s.calls.Add(1) == 1 {
		defer close(s.ended)
		return status.Error(codes.ResourceExhausted, "temporary worker quota; retry later")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func TestTemporaryQuotaRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := &transientQuotaServer{ended: make(chan struct{})}
	gs := grpc.NewServer()
	defer gs.Stop()
	protos.RegisterTaskHubSidecarServiceServer(gs, srv)
	lis := bufconn.Listen(1 << 20)
	defer lis.Close()
	go gs.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///quota", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	logger := &receiveBudgetLogger{Logger: backend.DefaultLogger(), stopped: make(chan string, 1)}
	terminalErrors := make(chan error, 1)
	cl := client.NewTaskHubGrpcClient(conn, logger, client.WithWorkItemListenerErrorHandler(func(err error) { terminalErrors <- err }))
	require.NoError(t, cl.StartWorkItemListener(ctx, task.NewTaskRegistry()))
	require.Eventually(t, func() bool { return srv.calls.Load() >= 2 }, 2*time.Second, 10*time.Millisecond, "transient quota should reconnect once the server is healthy")
	select {
	case err := <-terminalErrors:
		t.Fatalf("temporary quota must not stop the listener: %v", err)
	default:
	}
}

type progressQuotaServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	calls     atomic.Int32
	completed chan struct{}
	once      sync.Once
}

func (s *progressQuotaServer) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *progressQuotaServer) GetWorkItems(_ *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	switch s.calls.Add(1) {
	case 1, 3:
		return status.Error(codes.ResourceExhausted, "temporary quota")
	case 2:
		if err := stream.Send(&protos.WorkItem{Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{Name: "signal", WorkflowInstance: &protos.WorkflowInstance{InstanceId: "progress"}}}}); err != nil {
			return err
		}
		select {
		case <-s.completed:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		return status.Error(codes.ResourceExhausted, "new quota after useful work")
	default:
		<-stream.Context().Done()
		return stream.Context().Err()
	}
}

func (s *progressQuotaServer) CompleteActivityTask(context.Context, *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	s.once.Do(func() { close(s.completed) })
	return &protos.CompleteTaskResponse{}, nil
}

func TestUsefulWorkResetsResourceRetryBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := &progressQuotaServer{completed: make(chan struct{})}
	gs := grpc.NewServer()
	defer gs.Stop()
	protos.RegisterTaskHubSidecarServiceServer(gs, srv)
	lis := bufconn.Listen(1 << 20)
	defer lis.Close()
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///progress", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	terminalErrors := make(chan error, 1)
	cl := client.NewTaskHubGrpcClient(conn, backend.DefaultLogger(), client.WithWorkItemListenerErrorHandler(func(err error) { terminalErrors <- err }))
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddActivityN("signal", func(task.ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, cl.StartWorkItemListener(ctx, r))
	require.Eventually(t, func() bool { return srv.calls.Load() >= 4 }, 3*time.Second, 10*time.Millisecond, "completed work must reset the resource error budget")
	select {
	case err := <-terminalErrors:
		t.Fatalf("listener stopped despite work progress: %v", err)
	default:
	}
}
