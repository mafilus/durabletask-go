package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

// These run through actual worker turns and persisted SQLite history, including
// the internal patch marker written onto WorkflowStarted by the processor.
func Test_RetryTerminalSemanticsPersistAcrossTurns(t *testing.T) {
	for _, decode := range []bool{false, true} {
		t.Run(map[bool]string{false: "second-await-after-retry", true: "decode-error"}[decode], func(t *testing.T) {
			var calls atomic.Int32
			r := task.NewTaskRegistry()
			require.NoError(t, r.AddActivityN("act", func(task.ActivityContext) (any, error) {
				n := calls.Add(1)
				if !decode && n == 1 {
					return nil, errors.New("retry once")
				}
				return "result", nil
			}))
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *task.WorkflowContext) (any, error) {
				call := ctx.CallActivity("act", task.WithActivityRetryPolicy(&task.RetryPolicy{MaxAttempts: 3, InitialRetryInterval: time.Millisecond}))
				if decode {
					var wrong int
					return nil, call.Await(&wrong)
				}
				var first, second string
				if err := call.Await(&first); err != nil {
					return nil, err
				}
				if err := call.Await(&second); err != nil {
					return nil, err
				}
				return []string{first, second}, nil
			}))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cl, worker := initTaskHubWorker(ctx, r)
			defer worker.Shutdown(context.Background())
			id, err := cl.ScheduleNewWorkflow(ctx, "wf")
			require.NoError(t, err)
			metadata, err := cl.WaitForWorkflowCompletion(ctx, id)
			require.NoError(t, err)
			if decode {
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
				require.Equal(t, int32(1), calls.Load(), "decoding must not repeat successful activity effects")
			} else {
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
				require.Equal(t, `["result","result"]`, metadata.Output.GetValue())
				require.Equal(t, int32(2), calls.Load(), "the second Await must not execute another retry")
			}
		})
	}
}
