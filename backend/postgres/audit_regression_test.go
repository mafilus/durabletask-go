package postgres

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/stretchr/testify/require"
)

func TestWorkflowFanOutPersistsEveryActivity(t *testing.T) {
	for _, count := range []int{1, 2, 5} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			ctx := context.Background()
			be := newDurabilityBackend(t, time.Second, time.Second)
			resetDurabilityTables(t, ctx, be)
			insertWorkflowWithEvent(t, ctx, be, durabilityInstanceID, nil, 1)
			wi, err := be.GetWorkflowWorkItem(ctx)
			require.NoError(t, err)
			wi.State = &protos.WorkflowRuntimeState{InstanceId: string(wi.InstanceID)}
			for i := 0; i < count; i++ {
				wi.State.PendingTasks = append(wi.State.PendingTasks, &protos.HistoryEvent{
					EventId: int32(i), EventType: &protos.HistoryEvent_TaskScheduled{
						TaskScheduled: &protos.TaskScheduledEvent{Name: "activity"},
					},
				})
			}
			require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
			require.EqualValues(t, count, countRows(t, ctx, be, "NewTasks"))
			seen := make(map[int32]bool)
			for i := 0; i < count; i++ {
				a, err := be.getActivityWorkItem(ctx)
				require.NoError(t, err)
				require.False(t, seen[a.NewEvent.EventId])
				seen[a.NewEvent.EventId] = true
			}
			require.Len(t, seen, count)
		})
	}
}

func TestPostgresOptInPrecedesConnectionConfiguration(t *testing.T) {
	for _, enabled := range []string{"", "false", "1"} {
		t.Run("enabled="+enabled, func(t *testing.T) {
			t.Setenv("POSTGRES_ENABLED", enabled)
			// Parsing this would fail if either helper reached configuration.
			t.Setenv("PGPORT", "must-not-be-parsed")
			for _, pooled := range []bool{false, true} {
				reached := false
				ok := t.Run("helper", func(t *testing.T) {
					if pooled {
						newDurabilityBackendWithMaxConns(t, time.Second, time.Second, 2)
					} else {
						newDurabilityBackend(t, time.Second, time.Second)
					}
					reached = true
				})
				require.True(t, ok)
				require.False(t, reached, "helper must skip before connecting")
			}
		})
	}
}
