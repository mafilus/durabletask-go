package client

import (
	"context"
	"errors"
	"testing"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type completionTokenClient struct {
	protos.TaskHubSidecarServiceClient
	workflow *protos.WorkflowResponse
	activity *protos.ActivityResponse
}

func (c *completionTokenClient) CompleteWorkflowTask(_ context.Context, r *protos.WorkflowResponse, _ ...grpc.CallOption) (*protos.CompleteTaskResponse, error) {
	c.workflow = r
	return &protos.CompleteTaskResponse{}, nil
}

func (c *completionTokenClient) CompleteActivityTask(_ context.Context, r *protos.ActivityResponse, _ ...grpc.CallOption) (*protos.CompleteTaskResponse, error) {
	c.activity = r
	return &protos.CompleteTaskResponse{}, nil
}

type completionTokenExecutor struct {
	backend.Executor
	err   error
	event *protos.HistoryEvent
}

func (e completionTokenExecutor) ExecuteWorkflow(context.Context, api.InstanceID, []*protos.HistoryEvent, []*protos.HistoryEvent, backend.ExecuteOptions) (*protos.WorkflowResponse, error) {
	return &protos.WorkflowResponse{}, e.err
}

func (e completionTokenExecutor) ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent, backend.ExecuteOptions) (*protos.HistoryEvent, error) {
	return e.event, e.err
}

func TestWorkerEchoesCompletionToken(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		event *protos.HistoryEvent
	}{
		{name: "success", event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{}}}},
		{name: "execution error", err: errors.New("execution failed")},
		{name: "task failure", event: &protos.HistoryEvent{EventType: &protos.HistoryEvent_TaskFailed{TaskFailed: &protos.TaskFailedEvent{FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "failed"}}}}},
		{name: "unknown result", event: &protos.HistoryEvent{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &completionTokenClient{}
			c := &TaskHubGrpcClient{client: stub, logger: backend.DefaultLogger(), statefulHistoryDisabled: true}
			executor := completionTokenExecutor{err: tc.err, event: tc.event}
			c.processWorkflowWorkItem(context.Background(), executor, nil, &protos.WorkflowRequest{InstanceId: "workflow"}, nil, "workflow-token")
			require.NotNil(t, stub.workflow)
			require.Equal(t, "workflow-token", stub.workflow.CompletionToken)
			c.processActivityWorkItem(context.Background(), executor, &protos.ActivityRequest{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "workflow"}, TaskId: 42}, "activity-token")
			require.NotNil(t, stub.activity)
			require.Equal(t, "activity-token", stub.activity.CompletionToken)
		})
	}
}
