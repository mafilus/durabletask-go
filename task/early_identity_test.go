package task

import (
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend/runtimestate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestEarlyResolutionRecordedExecutionIdentity(t *testing.T) {
	for _, patched := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			for _, executionID := range []string{"historical-execution-id", ""} {
				name := map[bool]string{false: "legacy", true: "patched"}[patched] + "/" + map[bool]string{false: "success", true: "failure"}[failed] + "/" + executionID
				t.Run(name, func(t *testing.T) {
					r := NewTaskRegistry()
					require.NoError(t, r.AddWorkflowN("wf", func(w *WorkflowContext) (any, error) {
						if err := w.WaitForSingleEvent("go", -1).Await(nil); err != nil {
							return nil, err
						}
						return nil, w.CallActivity("act", WithActivityRetryPolicy(&RetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second})).Await(nil)
					}))
					resolution := evTaskCompleted(1, `null`)
					resolution.GetTaskCompleted().TaskExecutionId = executionID
					if failed {
						resolution = evTaskFailed(1, executionID)
					}
					events := []*protos.HistoryEvent{evExecutionStarted("wf"), evOptionalTimerCreated(0, "go"), resolution, evEventRaised("go")}
					if patched {
						events = append([]*protos.HistoryEvent{retryTurnStarted(retryChainPatch)}, events...)
					}
					for range 2 {
						actions, _ := runBuffered(t, r, nil, events)
						state := runtimestate.NewWorkflowRuntimeState("buffered-test", nil, events)
						_, err := runtimestate.NewApplier("app", "").Actions(state, nil, actions, nil, nil)
						require.NoError(t, err)
						require.Empty(t, state.PendingTasks, "resolved activity must not dispatch")
						raw, err := proto.Marshal(state)
						require.NoError(t, err)
						var restored protos.WorkflowRuntimeState
						require.NoError(t, proto.Unmarshal(raw, &restored))
						schedules, timers := 0, 0
						for _, e := range restored.NewEvents {
							if ts := e.GetTaskScheduled(); ts != nil && e.EventId == 1 {
								schedules++
								require.Equal(t, executionID, ts.TaskExecutionId, "record the resolution identity, including a historical absence")
							}
							if tc := e.GetTimerCreated(); tc != nil && tc.GetActivityRetry() != nil {
								timers++
								require.Equal(t, executionID, tc.GetActivityRetry().TaskExecutionId)
							}
						}
						require.Equal(t, 1, schedules)
						if failed {
							require.Equal(t, 1, timers)
						} else {
							require.Zero(t, timers)
						}
					}
				})
			}
		}
	}
}
