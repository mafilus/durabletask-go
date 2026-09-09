package task

import (
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend/runtimestate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReplayMultipleOptionalTimers(t *testing.T) {
	for _, kind := range []string{"activity", "child", "timer"} {
		for _, modern := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/legacy", true: "/modern"}[modern], func(t *testing.T) {
				r := NewTaskRegistry()
				require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
					for _, name := range []string{"a", "b"} {
						if err := ctx.WaitForSingleEvent(name, -1).Await(nil); err != nil {
							return nil, err
						}
					}
					var task Task
					switch kind {
					case "activity":
						task = ctx.CallActivity("act")
					case "child":
						task = ctx.CallChildWorkflow("child")
					case "timer":
						task = ctx.CreateTimer(time.Second)
					}
					return nil, task.Await(nil)
				}))
				history := []*protos.HistoryEvent{evExecutionStarted("wf")}
				for i, name := range []string{"a", "b"} {
					if modern {
						history = append(history, evOptionalTimerCreated(int32(i), name))
					}
					history = append(history, evEventRaised(name))
				}
				id := int32(0)
				if modern {
					id = 2
				}
				switch kind {
				case "activity":
					history = append(history, evTaskScheduled(id, "act"), evTaskCompleted(id, `"ok"`))
				case "child":
					history = append(history, &protos.HistoryEvent{EventId: id, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child"}}}, evChildCompleted(id, `"ok"`))
				case "timer":
					history = append(history, &protos.HistoryEvent{EventId: id, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{FireAt: timestamppb.Now()}}}, evTimerFired(id))
				}
				actions, _ := runBuffered(t, r, nil, history)
				co := completeAction(t, actions)
				require.NotNil(t, co)
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus, "%v", co.FailureDetails)
			})
		}
	}
}

func TestTerminatedChildFailsParentAwait(t *testing.T) {
	childStart := evExecutionStarted("child")
	childStart.GetExecutionStarted().ParentInstance = &protos.ParentInstanceInfo{TaskScheduledId: 0, WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent"}}
	state := runtimestate.NewWorkflowRuntimeState("child", nil, []*protos.HistoryEvent{childStart})
	_, err := runtimestate.NewApplier("", "").Actions(state, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED}}}}, nil, nil)
	require.NoError(t, err)
	require.Len(t, state.PendingMessages, 1)
	resolution := state.PendingMessages[0].HistoryEvent
	require.NotNil(t, resolution.GetChildWorkflowInstanceFailed().FailureDetails)
	require.Contains(t, resolution.GetChildWorkflowInstanceFailed().FailureDetails.ErrorMessage, "TERMINATED")
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "legacy-nil-details"}[legacy], func(t *testing.T) {
			if legacy {
				resolution.GetChildWorkflowInstanceFailed().FailureDetails = nil
			}
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				return nil, ctx.CallChildWorkflow("child").Await(nil)
			}))
			actions, _ := runBuffered(t, r, nil, []*protos.HistoryEvent{evExecutionStarted("wf"), {EventId: 0, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child"}}}, resolution})
			co := completeAction(t, actions)
			require.NotNil(t, co)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, co.WorkflowStatus)
		})
	}
}

func TestTaskFailedWithoutDetailsCannotSucceed(t *testing.T) {
	task := newTask(nil)
	task.fail(nil)
	require.Error(t, task.Await(nil))
}

func TestChildFailureWithoutDetailsArmsRetry(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		return nil, ctx.CallChildWorkflow("child", WithChildWorkflowRetryPolicy(&RetryPolicy{
			MaxAttempts: 2, InitialRetryInterval: time.Second,
		})).Await(nil)
	}))
	failure := evChildFailed(0)
	failure.GetChildWorkflowInstanceFailed().FailureDetails = nil
	actions, _ := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		{EventId: 0, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "child"}}},
		failure,
	})
	require.Nil(t, completeAction(t, actions), "nil failure details must not bypass the retry policy")
	require.Equal(t, 1, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer().GetChildWorkflowRetry() != nil && a.Id == 1
	}))
}
