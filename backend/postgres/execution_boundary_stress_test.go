package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/runtimestate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Exercise real transaction interleavings, rather than concurrent access to a
// mock. No tables are reset while goroutines are running.
func TestStressContinueAsNewRacesActivityCompletionsAndExternalDeliveries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	be := newDurabilityBackendWithMaxConns(t, 2*time.Minute, 2*time.Minute, 32)
	resetDurabilityTables(t, ctx, be)
	cl := backend.NewTaskHubClient(be)
	const workflows, activities, deliveries, duplicates = 32, 16, 8, 3
	oldFireAt, newFireAt := time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
	type execution struct {
		id   api.InstanceID
		turn *backend.WorkflowWorkItem
		old  []*backend.ActivityWorkItem
		done chan struct{}
	}
	executions := make([]execution, workflows)
	applier := runtimestate.NewApplier("stress", "")
	var accepted, fenced atomic.Int64
	for i := range executions {
		id, err := cl.ScheduleNewWorkflow(ctx, "stress-boundary", api.WithInstanceID(api.InstanceID(fmt.Sprintf("stress-boundary-%02d", i))))
		require.NoError(t, err)
		turn, err := be.GetWorkflowWorkItem(ctx)
		require.NoError(t, err)
		require.Equal(t, id, turn.InstanceID)
		turn.State, err = be.GetWorkflowRuntimeState(ctx, turn)
		require.NoError(t, err)
		require.NoError(t, runtimestate.AddEvent(turn.State, turn.NewEvents[0]))
		actions := stressBoundaryActions(activities, "old-activity", oldFireAt)
		_, err = applier.Actions(turn.State, nil, actions, nil, nil)
		require.NoError(t, err)
		require.NoError(t, be.CompleteWorkflowWorkItem(ctx, turn))
		ex := &executions[i]
		ex.id, ex.done = id, make(chan struct{})
		for j := 0; j < activities; j++ {
			activity, err := be.getActivityWorkItem(ctx)
			require.NoError(t, err)
			require.Equal(t, id, activity.InstanceID)
			activity.Result = &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: activity.NewEvent.EventId, Result: wrapperspb.String(`"obsolete"`)}}}
			ex.old = append(ex.old, activity)
		}
		// Guarantee coverage of completion before the boundary, in addition to
		// the racing completions and the guaranteed late completion below.
		require.NoError(t, be.CompleteActivityWorkItem(ctx, ex.old[0]))
		accepted.Add(1)
		require.NoError(t, cl.RaiseEvent(ctx, id, "advance"))
		ex.turn, err = be.GetWorkflowWorkItem(ctx)
		require.NoError(t, err)
		ex.turn.State, err = be.GetWorkflowRuntimeState(ctx, ex.turn)
		require.NoError(t, err)
		_, err = applier.Actions(ex.turn.State, nil, []*protos.WorkflowAction{{Id: activities + 1, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW}}}}, nil, nil)
		require.NoError(t, err)
		_, err = applier.Actions(ex.turn.State, nil, stressBoundaryActions(activities, "new-activity", newFireAt), nil, nil)
		require.NoError(t, err)
	}

	start := make(chan struct{})
	errs := make(chan error, workflows*(activities+deliveries+1))
	var wg sync.WaitGroup
	run := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := fn(); err != nil {
				errs <- err
			}
		}()
	}
	for i := range executions {
		ex := &executions[i]
		run(func() error {
			defer close(ex.done)
			return be.CompleteWorkflowWorkItem(ctx, ex.turn)
		})
		for j, activity := range ex.old[1:] {
			run(func() error {
				if j == activities-2 {
					<-ex.done
				}
				err := be.CompleteActivityWorkItem(ctx, activity)
				if errors.Is(err, backend.ErrWorkItemLockLost) {
					fenced.Add(1)
					return nil
				}
				if err == nil {
					accepted.Add(1)
				}
				return err
			})
		}
		for delivery := 0; delivery < deliveries; delivery++ {
			run(func() error {
				for duplicate := 0; duplicate < duplicates; duplicate++ {
					if err := cl.RaiseEvent(ctx, ex.id, "keep", api.WithEventPayload(delivery), api.WithExternalDeliveryID(fmt.Sprintf("delivery-%02d", delivery))); err != nil {
						return err
					}
				}
				return nil
			})
		}
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, workflows*activities, accepted.Load()+fenced.Load())
	require.GreaterOrEqual(t, accepted.Load(), int64(workflows))
	require.GreaterOrEqual(t, fenced.Load(), int64(workflows))
	require.Equal(t, workflows*activities, countRows(t, ctx, be, "NewTasks"))
	require.Equal(t, workflows*deliveries, countRows(t, ctx, be, "ExternalEventDeliveries"))

	rows, err := be.db.Query(ctx, "SELECT InstanceID, EventPayload FROM NewEvents")
	require.NoError(t, err)
	seen := make(map[string]map[string]bool)
	timers := 0
	for rows.Next() {
		var id string
		var payload []byte
		require.NoError(t, rows.Scan(&id, &payload))
		event, err := backend.UnmarshalHistoryEvent(payload)
		require.NoError(t, err)
		if timer := event.GetTimerFired(); timer != nil {
			require.True(t, timer.FireAt.AsTime().Equal(newFireAt), "old-generation timer survived")
			timers++
			continue
		}
		raised := event.GetEventRaised()
		require.NotNil(t, raised, "old-generation result survived: %s", event)
		require.Equal(t, "keep", raised.Name)
		if seen[id] == nil {
			seen[id] = make(map[string]bool)
		}
		key := raised.GetInput().GetValue()
		require.False(t, seen[id][key], "duplicate external delivery reached the queue")
		seen[id][key] = true
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, workflows, timers)
	require.Len(t, seen, workflows)
	for _, values := range seen {
		require.Len(t, values, deliveries)
	}
	for _, ex := range executions {
		var storedExecution string
		require.NoError(t, be.db.QueryRow(ctx, "SELECT ExecutionID FROM Instances WHERE InstanceID = $1", string(ex.id)).Scan(&storedExecution))
		require.Equal(t, ex.turn.State.StartEvent.WorkflowInstance.ExecutionId.GetValue(), storedExecution)
	}

	// Concurrent acquisition of every fresh activity must return each row once.
	acquired := make(chan int64, workflows*activities)
	errs = make(chan error, 16)
	for poller := 0; poller < 16; poller++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < workflows*activities/16; j++ {
				activity, err := be.getActivityWorkItem(ctx)
				if err != nil {
					errs <- err
					return
				}
				if activity.NewEvent.GetTaskScheduled().GetName() != "new-activity" {
					errs <- fmt.Errorf("old activity reacquired")
					return
				}
				acquired <- activity.SequenceNumber
			}
		}()
	}
	wg.Wait()
	close(acquired)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	sequences := make(map[int64]bool)
	for sequence := range acquired {
		require.False(t, sequences[sequence], "activity acquired twice")
		sequences[sequence] = true
	}
	require.Len(t, sequences, workflows*activities)
	t.Logf("%d concurrent transitions; %d old completions accepted before retirement, %d fenced; %d external submissions deduplicated to %d; %d fresh activities acquired once", workflows, accepted.Load(), fenced.Load(), workflows*deliveries*duplicates, workflows*deliveries, len(sequences))
}

func stressBoundaryActions(activities int, name string, fireAt time.Time) []*protos.WorkflowAction {
	actions := make([]*protos.WorkflowAction, 0, activities+1)
	for taskID := 0; taskID < activities; taskID++ {
		actions = append(actions, &protos.WorkflowAction{Id: int32(taskID), WorkflowActionType: &protos.WorkflowAction_ScheduleTask{ScheduleTask: &protos.ScheduleTaskAction{Name: name}}})
	}
	return append(actions, &protos.WorkflowAction{Id: int32(activities), WorkflowActionType: &protos.WorkflowAction_CreateTimer{CreateTimer: &protos.CreateTimerAction{FireAt: timestamppb.New(fireAt)}}})
}
