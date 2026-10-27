package task

import (
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/runtimestate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBufferedResolutionPersistedScheduleReplaysWithoutRedispatch(t *testing.T) {
	for _, kind := range []string{"activity", "timer", "child"} {
		t.Run(kind, func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
				if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
					return nil, err
				}
				var first Task
				switch kind {
				case "activity":
					first = ctx.CallActivity("act")
				case "timer":
					first = ctx.CreateTimer(time.Hour)
				case "child":
					first = ctx.CallChildWorkflow("child")
				}
				if err := first.Await(nil); err != nil {
					return nil, err
				}
				if err := ctx.CallActivity("act2").Await(nil); err != nil {
					return nil, err
				}
				return "done", nil
			}))
			state := runtimestate.NewWorkflowRuntimeState("buffered-test", nil, nil)
			resolution := evTaskCompleted(1, `null`)
			switch kind {
			case "timer":
				resolution = evTimerFired(1)
			case "child":
				resolution = evChildCompleted(1, `null`)
			}
			for _, e := range []*protos.HistoryEvent{evExecutionStarted("wf"), evOptionalTimerCreated(0, "go"), resolution, evEventRaised("go")} {
				require.NoError(t, runtimestate.AddEvent(state, e))
			}
			ex := NewTaskExecutor(r)
			resp, err := ex.ExecuteWorkflow(t.Context(), "buffered-test", nil, state.NewEvents, backend.ExecuteOptions{})
			require.NoError(t, err)
			require.Len(t, resp.Actions, 2, "both schedules must be emitted for recording")
			_, err = runtimestate.NewApplier("app", "").Actions(state, nil, resp.Actions, nil, nil)
			require.NoError(t, err)
			require.Empty(t, state.PendingMessages)
			require.Empty(t, state.PendingTimers)
			require.Len(t, state.PendingTasks, 1)
			require.Equal(t, int32(2), state.PendingTasks[0].EventId, "only the unresolved activity may dispatch")
			// Cross a serialization boundary before replaying the next turn.
			raw, err := proto.Marshal(state)
			require.NoError(t, err)
			var restored protos.WorkflowRuntimeState
			require.NoError(t, proto.Unmarshal(raw, &restored))
			var scheduled bool
			for _, e := range restored.NewEvents {
				scheduled = scheduled || ((e.GetTaskScheduled() != nil || e.GetTimerCreated() != nil || e.GetChildWorkflowInstanceCreated() != nil) && e.EventId == 1)
			}
			require.True(t, scheduled, "the resolved schedule must survive persistence")
			history := append(restored.OldEvents, restored.NewEvents...)
			resp, err = ex.ExecuteWorkflow(t.Context(), "buffered-test", history, []*protos.HistoryEvent{evTaskCompleted(2, `null`)}, backend.ExecuteOptions{})
			require.NoError(t, err)
			co := completeAction(t, resp.Actions)
			require.NotNil(t, co)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
			require.Equal(t, `"done"`, co.GetResult().GetValue())
			require.Len(t, resp.Actions, 1, "replay must not redispatch either activity")
		})
	}
}
