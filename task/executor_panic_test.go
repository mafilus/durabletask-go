package task

import (
	"context"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
)

func TestActivityPanicPreservesRetryIdentity(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddActivityN("panic", func(ActivityContext) (any, error) { panic("boom") }))
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		return nil, ctx.CallActivity("panic", WithActivityRetryPolicy(&RetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second})).Await(nil)
	}))
	executor := NewTaskExecutor(r)
	start := evExecutionStarted("wf")
	response, err := executor.ExecuteWorkflow(context.Background(), "instance", nil, []*protos.HistoryEvent{start}, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Len(t, response.Actions, 1)
	action := response.Actions[0]
	scheduled := evTaskScheduled(action.Id, "panic")
	scheduled.GetTaskScheduled().TaskExecutionId = action.GetScheduleTask().TaskExecutionId
	require.NotEmpty(t, scheduled.GetTaskScheduled().TaskExecutionId)
	failure, err := executor.ExecuteActivity(context.Background(), "instance", scheduled, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, scheduled.GetTaskScheduled().TaskExecutionId, failure.GetTaskFailed().GetTaskExecutionId())
	require.Equal(t, "TaskActivityPanic", failure.GetTaskFailed().GetFailureDetails().GetErrorType())
	response, err = executor.ExecuteWorkflow(context.Background(), "instance", []*protos.HistoryEvent{start, scheduled}, []*protos.HistoryEvent{failure}, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Len(t, response.Actions, 1)
	timer := response.Actions[0]
	require.Equal(t, scheduled.GetTaskScheduled().TaskExecutionId, timer.GetCreateTimer().GetActivityRetry().GetTaskExecutionId())
	created := &protos.HistoryEvent{EventId: timer.Id, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{FireAt: timer.GetCreateTimer().FireAt, Origin: &protos.TimerCreatedEvent_ActivityRetry{ActivityRetry: timer.GetCreateTimer().GetActivityRetry()}}}}
	response, err = executor.ExecuteWorkflow(context.Background(), "instance", []*protos.HistoryEvent{start, scheduled, failure, created}, []*protos.HistoryEvent{evTimerFired(timer.Id)}, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Len(t, response.Actions, 1)
	require.Equal(t, scheduled.GetTaskScheduled().TaskExecutionId, response.Actions[0].GetScheduleTask().GetTaskExecutionId())
}
