package backend

import (
	"context"
	"sync"
	"testing"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type executorRegressionBackend struct{ Backend }

func (executorRegressionBackend) WaitForWorkflowTaskCompletion(*protos.WorkflowRequest) func(context.Context) (*protos.WorkflowResponse, error) {
	return func(ctx context.Context) (*protos.WorkflowResponse, error) { return nil, ctx.Err() }
}

func (executorRegressionBackend) WaitForActivityCompletion(*protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	return func(ctx context.Context) (*protos.ActivityResponse, error) { return nil, ctx.Err() }
}

func newRegressionExecutor() *grpcExecutor {
	return &grpcExecutor{
		workItemQueue:     make(chan *protos.WorkItem),
		backend:           executorRegressionBackend{},
		logger:            DefaultLogger(),
		pendingWorkflows:  &sync.Map{},
		pendingActivities: &sync.Map{},
		streams:           &sync.Map{},
	}
}

func TestGrpcExecutorRemovesPendingWorkflowWhenDispatchIsCanceled(t *testing.T) {
	executor := newRegressionExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := executor.ExecuteWorkflow(ctx, "workflow", nil, nil, ExecuteOptions{})
	require.Error(t, err)
	_, ok := executor.pendingWorkflows.Load(api.InstanceID("workflow"))
	require.False(t, ok)
}

func TestGrpcExecutorRemovesPendingActivityWhenDispatchIsCanceled(t *testing.T) {
	executor := newRegressionExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	event := &protos.HistoryEvent{EventId: 42, EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "activity"}}}

	_, err := executor.ExecuteActivity(ctx, "workflow", event, ExecuteOptions{})
	require.Error(t, err)
	_, ok := executor.pendingActivities.Load(GetActivityExecutionKey("workflow", 42))
	require.False(t, ok)
}

type closedQueueStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *closedQueueStream) Context() context.Context  { return s.ctx }
func (*closedQueueStream) Send(*protos.WorkItem) error { return nil }

func TestGrpcExecutorGetWorkItemsReturnsWhenWorkItemQueueIsClosed(t *testing.T) {
	executor := newRegressionExecutor()
	close(executor.workItemQueue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := executor.GetWorkItems(&protos.GetWorkItemsRequest{}, &closedQueueStream{ctx: ctx})
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Equal(t, "shutting down", status.Convert(err).Message())
}
