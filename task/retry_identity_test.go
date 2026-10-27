package task

import (
	"testing"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
)

func TestRetryAuditRetrySuccessExecutionIDReplayStability(t *testing.T) {
	for _, child := range []bool{false, true} {
		t.Run(map[bool]string{false: "activity", true: "child"}[child], func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				var call Task
				if child {
					call = ctx.CallChildWorkflow("child", WithChildWorkflowRetryPolicy(retryTestPolicy()))
				} else {
					call = ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy()))
				}
				if err := call.Await(nil); err != nil {
					return nil, err
				}
				return call.TaskExecutionId(), nil
			}))
			events := []*protos.HistoryEvent{retryTurnStarted(retryChainPatch), evExecutionStarted("wf")}
			if child {
				events = append(events, &protos.HistoryEvent{EventId: 0, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child", InstanceId: "recorded-child"}}}, evChildCompleted(0, `null`))
			} else {
				scheduled := evTaskScheduled(0, "act")
				scheduled.GetTaskScheduled().TaskExecutionId = "recorded-execution"
				completed := evTaskCompleted(0, `null`)
				completed.GetTaskCompleted().TaskExecutionId = "recorded-execution"
				events = append(events, scheduled, completed)
			}
			var outputs []string
			for i := 0; i < 2; i++ {
				resp, err := NewTaskExecutor(r).ExecuteWorkflow(t.Context(), "instance", events, nil, backend.ExecuteOptions{})
				require.NoError(t, err)
				co := completeAction(t, resp.Actions)
				require.NotNil(t, co)
				outputs = append(outputs, co.GetResult().GetValue())
			}
			t.Logf("same serialized history, two replays: %v", outputs)
			require.Equal(t, outputs[0], outputs[1], "TaskExecutionId used as workflow data must replay deterministically")
		})
	}
}
func TestRetryAuditLegacySuccessExecutionIDReplayStability(t *testing.T) {
	for _, child := range []bool{false, true} {
		t.Run(map[bool]string{false: "activity", true: "child"}[child], func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				var call Task
				if child {
					call = ctx.CallChildWorkflow("child", WithChildWorkflowRetryPolicy(retryTestPolicy()))
				} else {
					call = ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy()))
				}
				if err := call.Await(nil); err != nil {
					return nil, err
				}
				return call.TaskExecutionId(), nil
			}))
			events := []*protos.HistoryEvent{retryTurnStarted(), evExecutionStarted("wf")}
			if child {
				events = append(events, &protos.HistoryEvent{EventId: 0, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child", InstanceId: "recorded-child"}}}, evChildCompleted(0, `null`))
			} else {
				scheduled := evTaskScheduled(0, "act")
				scheduled.GetTaskScheduled().TaskExecutionId = "recorded-execution"
				completed := evTaskCompleted(0, `null`)
				completed.GetTaskCompleted().TaskExecutionId = "recorded-execution"
				events = append(events, scheduled, completed)
			}
			var outputs []string
			for i := 0; i < 2; i++ {
				resp, err := NewTaskExecutor(r).ExecuteWorkflow(t.Context(), "instance", events, nil, backend.ExecuteOptions{})
				require.NoError(t, err)
				co := completeAction(t, resp.Actions)
				require.NotNil(t, co)
				outputs = append(outputs, co.GetResult().GetValue())
			}
			t.Logf("same serialized history, two replays: %v", outputs)
			require.Equal(t, outputs[0], outputs[1], "TaskExecutionId used as workflow data must replay deterministically")
		})
	}
}

func TestRetryAuditRetryExecutionIDChangesDownstreamAction(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		call := ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy()))
		if err := call.Await(nil); err != nil {
			return nil, err
		}
		return nil, ctx.CallActivity("consume-id", WithActivityInput(call.TaskExecutionId())).Await(nil)
	}))
	scheduled := evTaskScheduled(0, "act")
	scheduled.GetTaskScheduled().TaskExecutionId = "recorded-execution"
	completed := evTaskCompleted(0, `null`)
	completed.GetTaskCompleted().TaskExecutionId = "recorded-execution"
	events := []*protos.HistoryEvent{retryTurnStarted(retryChainPatch), evExecutionStarted("wf"), scheduled, completed}
	var inputs []string
	for i := 0; i < 2; i++ {
		resp, err := NewTaskExecutor(r).ExecuteWorkflow(t.Context(), "instance", events, nil, backend.ExecuteOptions{})
		require.NoError(t, err)
		require.Len(t, resp.Actions, 1)
		a := resp.Actions[0]
		require.EqualValues(t, 1, a.Id)
		require.Equal(t, "consume-id", a.GetScheduleTask().GetName())
		inputs = append(inputs, a.GetScheduleTask().GetInput().GetValue())
	}
	t.Logf("same history -> ScheduleTask id=1 inputs: %v", inputs)
	require.Equal(t, inputs[0], inputs[1], "persisted downstream activity input must remain stable")
}
