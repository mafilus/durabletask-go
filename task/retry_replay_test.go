package task

import (
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func retryTurnStarted(patches ...string) *protos.HistoryEvent {
	return &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &protos.WorkflowStartedEvent{Version: &protos.WorkflowVersion{Patches: patches}},
	}}
}

func retryTestPolicy() *RetryPolicy {
	return &RetryPolicy{MaxAttempts: 3, InitialRetryInterval: time.Second}
}

func TestRetryPatchPersistsBeforeFirstAttempt(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		ctx.CallActivity("unobserved-1", WithActivityRetryPolicy(retryTestPolicy()))
		ctx.CallActivity("unobserved-2", WithActivityRetryPolicy(retryTestPolicy()))
		return nil, ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy())).Await(nil)
	}))
	resp, err := NewTaskExecutor(r).ExecuteWorkflow(t.Context(), "instance", nil,
		[]*protos.HistoryEvent{retryTurnStarted(), evExecutionStarted("wf")}, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{retryChainPatch}, resp.GetVersion().GetPatches(), "fan-out must not multiply the marker in every replayed turn")
	require.Len(t, resp.Actions, 3)
	require.NotNil(t, resp.Actions[0].GetScheduleTask())
}

func TestRetryDecodeErrorDoesNotRepeatSuccessfulWork(t *testing.T) {
	for _, child := range []bool{false, true} {
		t.Run(map[bool]string{false: "activity", true: "child"}[child], func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				var result int
				var call Task
				if child {
					call = ctx.CallChildWorkflow("child", WithChildWorkflowRetryPolicy(retryTestPolicy()))
				} else {
					call = ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy()))
				}
				err := call.Await(&result)
				// A caller may correct its destination type and observe the same result.
				var text string
				if err != nil {
					if again := call.Await(&text); again != nil {
						return nil, again
					}
				}
				return text, err
			}))
			events := []*protos.HistoryEvent{retryTurnStarted(retryChainPatch), evExecutionStarted("wf")}
			if child {
				events = append(events, &protos.HistoryEvent{EventId: 0, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
					ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child"},
				}}, evChildCompleted(0, `"wrong type"`))
			} else {
				events = append(events, evTaskScheduled(0, "act"), evTaskCompleted(0, `"wrong type"`))
			}
			actions, _ := runBuffered(t, r, nil, events)
			co := completeAction(t, actions)
			require.NotNil(t, co, "no retry timer should replace the terminal decode error")
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, co.WorkflowStatus)
			require.Contains(t, co.GetFailureDetails().GetErrorMessage(), "decode task result")
			require.Len(t, actions, 1)
		})
	}
}

func TestRetryRepeatedAwaitRetainsTerminalAttempt(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "success"}[success], func(t *testing.T) {
			r := NewTaskRegistry()
			var firstErr, secondErr error
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				p := retryTestPolicy()
				p.MaxAttempts = 2
				call := ctx.CallActivity("act", WithActivityRetryPolicy(p))
				firstErr = call.Await(nil)
				secondErr = call.Await(nil)
				return call.TaskExecutionId(), secondErr
			}))
			events := []*protos.HistoryEvent{retryTurnStarted(retryChainPatch), evExecutionStarted("wf"),
				evTaskScheduled(0, "act"), evTaskFailed(0, "historical-id"),
				{EventId: 1, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}}},
				evTimerFired(1), evTaskScheduled(2, "act")}
			if success {
				events = append(events, evTaskCompleted(2, `null`))
			} else {
				events = append(events, evTaskFailed(2, "historical-id"))
			}
			actions, _ := runBuffered(t, r, nil, events)
			co := completeAction(t, actions)
			require.NotNil(t, co, "a second Await must not arm another timer")
			require.Len(t, actions, 1)
			if success {
				require.NoError(t, firstErr)
				require.NoError(t, secondErr)
				require.Equal(t, `"historical-id"`, co.GetResult().GetValue())
			} else {
				require.Equal(t, firstErr, secondErr)
				require.Error(t, firstErr)
			}
		})
	}
}

func TestRetryLegacyDecodeHistoryReplaysWithoutOptingIn(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		var out int
		err := ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy())).Await(&out)
		return out, err
	}))
	// v1.2.1 recorded a retry even though attempt zero succeeded: its output
	// could not decode into an int. Keep the historical timer/action IDs.
	events := []*protos.HistoryEvent{retryTurnStarted(), evExecutionStarted("wf"), evTaskScheduled(0, "act"),
		evTaskCompleted(0, `"wrong type"`), {EventId: 1, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}}},
		evTimerFired(1), evTaskScheduled(2, "act"), evTaskCompleted(2, `42`)}
	resp, err := NewTaskExecutor(r).ExecuteWorkflow(t.Context(), "instance", events, nil, backend.ExecuteOptions{})
	require.NoError(t, err)
	co := completeAction(t, resp.Actions)
	require.NotNil(t, co)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	require.Equal(t, "42", co.GetResult().GetValue())
	require.NotContains(t, resp.GetVersion().GetPatches(), retryChainPatch)
}

func TestRetryLegacyRepeatedAwaitHistoryReplays(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		call := ctx.CallActivity("act", WithActivityRetryPolicy(retryTestPolicy()))
		if err := call.Await(nil); err != nil {
			return nil, err
		}
		return nil, call.Await(nil)
	}))
	events := []*protos.HistoryEvent{retryTurnStarted(), evExecutionStarted("wf"), evTaskScheduled(0, "act"), evTaskFailed(0, "original"),
		{EventId: 1, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}}},
		evTimerFired(1), evTaskScheduled(2, "act"), evTaskCompleted(2, `null`),
		{EventId: 3, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}}},
		evTimerFired(3), evTaskScheduled(4, "act"), evTaskCompleted(4, `null`)}
	actions, _ := runBuffered(t, r, events, nil)
	co := completeAction(t, actions)
	require.NotNil(t, co)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	require.Len(t, actions, 1)
}

func TestRetryFanOutKeepsHistoricalObservationOrder(t *testing.T) {
	for _, patched := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "patched"}[patched], func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				a := ctx.CallActivity("A", WithActivityRetryPolicy(retryTestPolicy()))
				b := ctx.CallActivity("B", WithActivityRetryPolicy(retryTestPolicy()))
				c := ctx.CallActivity("C", WithActivityRetryPolicy(retryTestPolicy()))
				for _, call := range []Task{c, b, a} {
					if err := call.Await(nil); err != nil {
						return nil, err
					}
				}
				return "done", nil
			}))
			started := retryTurnStarted()
			if patched {
				started = retryTurnStarted(retryChainPatch)
			}
			events := []*protos.HistoryEvent{started, evExecutionStarted("wf"), evTaskScheduled(0, "A"), evTaskScheduled(1, "B"), evTaskScheduled(2, "C"),
				evTaskFailed(0, "A-id"), evTaskFailed(1, "B-id"), evTaskCompleted(2, `null`),
				{EventId: 3, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}}},
				evTimerFired(3), evTaskScheduled(4, "B"), evTaskCompleted(4, `null`),
				{EventId: 5, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{}}},
				evTimerFired(5), evTaskScheduled(6, "A"), evTaskCompleted(6, `null`)}
			actions, _ := runBuffered(t, r, events, nil)
			co := completeAction(t, actions)
			require.NotNil(t, co)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
			require.Equal(t, `"done"`, co.GetResult().GetValue())
			require.Len(t, actions, 1)
		})
	}
}
