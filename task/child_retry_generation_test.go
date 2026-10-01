package task

import (
	"context"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIndependentReviewEarlyChildFailureRetainsHistoricalRetryGroup(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(w *WorkflowContext) (any, error) {
		return nil, w.CallChildWorkflow("child", WithChildWorkflowRetryPolicy(&RetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second})).Await(nil)
	}))
	scheduled := &protos.HistoryEvent{EventId: 0, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child", InstanceId: "instance:0000"}}}
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "created-first", true: "failure-first"}[early], func(t *testing.T) {
			events := []*protos.HistoryEvent{evExecutionStarted("wf"), scheduled, evChildFailed(0)}
			if early {
				events = []*protos.HistoryEvent{evExecutionStarted("wf"), evChildFailed(0), scheduled}
			}
			response, err := NewTaskExecutor(r).ExecuteWorkflow(context.Background(), "instance", nil, events, backend.ExecuteOptions{})
			require.NoError(t, err)
			require.Len(t, response.Actions, 1)
			timer := response.Actions[0].GetCreateTimer()
			require.NotNil(t, timer)
			require.Equal(t, "instance:0000", timer.GetChildWorkflowRetry().GetInstanceId())
		})
	}
}
