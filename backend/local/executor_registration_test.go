package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/local"
	"github.com/mafilus/durabletask-go/backend/sqlite"
	"github.com/stretchr/testify/require"
)

// A registry is shared by all executors backed by the same local task backend.
// Refuse at registration, before exposing a duplicate to a remote worker.
func TestExecutorRefusesSharedBackendDuplicateBeforeDispatch(t *testing.T) {
	for _, activity := range []bool{false, true} {
		t.Run(map[bool]string{false: "workflow", true: "activity"}[activity], func(t *testing.T) {
			be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), backend.DefaultLogger())
			executor, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
			defer executor.Shutdown(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if activity {
				wait := be.WaitForActivityCompletion(&protos.ActivityRequest{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "same"}, TaskId: 1})
				_, err := executor.ExecuteActivity(ctx, "same", &protos.HistoryEvent{EventId: 1, EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "act"}}}, backend.ExecuteOptions{})
				require.ErrorIs(t, err, local.ErrTaskAlreadyPending, "must reject before blocking on a stream")
				require.NoError(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{InstanceId: "same", TaskId: 1}))
				_, err = wait(ctx)
				require.NoError(t, err)
			} else {
				wait := be.WaitForWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "same"})
				_, err := executor.ExecuteWorkflow(ctx, "same", nil, nil, backend.ExecuteOptions{})
				require.ErrorIs(t, err, local.ErrTaskAlreadyPending, "must reject before blocking on a stream")
				require.NoError(t, be.CompleteWorkflowTask(ctx, &protos.WorkflowResponse{InstanceId: "same"}))
				_, err = wait(ctx)
				require.NoError(t, err)
			}
		})
	}
}
