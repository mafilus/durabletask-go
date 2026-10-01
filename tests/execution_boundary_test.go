package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/runtimestate"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func Test_ParallelActivitiesPersistInOneTurn(t *testing.T) {
	for i, be := range getRunnableBackends() {
		// PostgreSQL must connect before DeleteTaskHub can clear a prior run.
		require.NoError(t, be.CreateTaskHub(ctx))
		initTest(t, be, i, true)
		cl := backend.NewTaskHubClient(be)
		id, err := cl.ScheduleNewWorkflow(ctx, "parallel-persistence")
		require.NoError(t, err)
		wi, err := be.NextWorkflowWorkItem(ctx)
		require.NoError(t, err)
		wi.State, err = be.GetWorkflowRuntimeState(ctx, wi)
		require.NoError(t, err)
		require.NoError(t, runtimestate.AddEvent(wi.State, wi.NewEvents[0]))
		actions := []*protos.WorkflowAction{
			{Id: 0, WorkflowActionType: &protos.WorkflowAction_ScheduleTask{ScheduleTask: &protos.ScheduleTaskAction{Name: "first"}}},
			{Id: 1, WorkflowActionType: &protos.WorkflowAction_ScheduleTask{ScheduleTask: &protos.ScheduleTaskAction{Name: "second"}}},
		}
		_, err = runtimestate.NewApplier("test", "").Actions(wi.State, nil, actions, nil, nil)
		require.NoError(t, err)
		require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
		for _, name := range []string{"first", "second"} {
			activity, err := be.NextActivityWorkItem(ctx)
			require.NoError(t, err)
			require.Equal(t, id, activity.InstanceID)
			require.Equal(t, name, activity.NewEvent.GetTaskScheduled().GetName())
		}
		require.NoError(t, be.Stop(ctx))
	}
}

func Test_DetachedWorkflowHonorsStartTime(t *testing.T) {
	for i, be := range getRunnableBackends() {
		require.NoError(t, be.CreateTaskHub(ctx))
		initTest(t, be, i, true)
		cl := backend.NewTaskHubClient(be)
		_, err := cl.ScheduleNewWorkflow(ctx, "scheduled-parent")
		require.NoError(t, err)
		wi, err := be.NextWorkflowWorkItem(ctx)
		require.NoError(t, err)
		wi.State, err = be.GetWorkflowRuntimeState(ctx, wi)
		require.NoError(t, err)
		require.NoError(t, runtimestate.AddEvent(wi.State, wi.NewEvents[0]))
		_, err = runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CreateDetachedWorkflow{CreateDetachedWorkflow: &protos.CreateDetachedWorkflowAction{Name: "scheduled-child", InstanceId: "scheduled-child", ScheduledStartTimestamp: timestamppb.New(time.Now().Add(time.Hour))}}}}, nil, nil)
		require.NoError(t, err)
		require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
		wait, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err = be.NextWorkflowWorkItem(wait)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NoError(t, be.Stop(ctx))
	}
}

func Test_ContinueAsNewRetiresActivitiesAndPreservesExternalEvents(t *testing.T) {
	for i, be := range getRunnableBackends() {
		require.NoError(t, be.CreateTaskHub(ctx))
		initTest(t, be, i, true)
		cl := backend.NewTaskHubClient(be)
		id, err := cl.ScheduleNewWorkflow(ctx, "execution-boundary")
		require.NoError(t, err)
		wi, err := be.NextWorkflowWorkItem(ctx)
		require.NoError(t, err)
		wi.State, err = be.GetWorkflowRuntimeState(ctx, wi)
		require.NoError(t, err)
		require.NoError(t, runtimestate.AddEvent(wi.State, wi.NewEvents[0]))
		applier := runtimestate.NewApplier("test", "")
		_, err = applier.Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_ScheduleTask{ScheduleTask: &protos.ScheduleTaskAction{Name: "old-activity"}}}}, nil, nil)
		require.NoError(t, err)
		require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
		oldActivity, err := be.NextActivityWorkItem(ctx)
		require.NoError(t, err)
		require.NoError(t, cl.RaiseEvent(ctx, id, "advance"))
		wi, err = be.NextWorkflowWorkItem(ctx)
		require.NoError(t, err)
		wi.State, err = be.GetWorkflowRuntimeState(ctx, wi)
		require.NoError(t, err)
		oldExecution := wi.State.StartEvent.WorkflowInstance.ExecutionId.GetValue()
		// These arrive after acquisition, so they are outside the inbound batch.
		require.NoError(t, cl.RaiseEvent(ctx, id, "keep", api.WithEventPayload(42), api.WithExternalDeliveryID("keep-delivery")))
		require.NoError(t, be.AddNewWorkflowEvent(ctx, id, &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: 0, Result: wrapperspb.String("obsolete")}}}))
		require.NoError(t, be.AddNewWorkflowEvent(ctx, id, &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{TimerId: 0, FireAt: timestamppb.Now()}}}))
		_, err = applier.Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 1, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW, Result: wrapperspb.String("1")}}}}, nil, nil)
		require.NoError(t, err)
		_, err = applier.Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_ScheduleTask{ScheduleTask: &protos.ScheduleTaskAction{Name: "new-activity"}}}}, nil, nil)
		require.NoError(t, err)
		require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
		require.NotEqual(t, oldExecution, wi.State.StartEvent.WorkflowInstance.ExecutionId.GetValue())
		oldActivity.Result = &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: 0}}}
		require.ErrorIs(t, be.CompleteActivityWorkItem(ctx, oldActivity), backend.ErrWorkItemLockLost)
		fresh, err := be.NextActivityWorkItem(ctx)
		require.NoError(t, err)
		require.Equal(t, "new-activity", fresh.NewEvent.GetTaskScheduled().GetName())
		next, err := be.NextWorkflowWorkItem(ctx)
		require.NoError(t, err)
		require.Len(t, next.NewEvents, 1)
		require.Equal(t, "keep", next.NewEvents[0].GetEventRaised().GetName())
		require.Equal(t, "42", next.NewEvents[0].GetEventRaised().GetInput().GetValue())
		// The durable receipt also survives the generation boundary.
		require.NoError(t, cl.RaiseEvent(ctx, id, "keep", api.WithEventPayload(42), api.WithExternalDeliveryID("keep-delivery")))
		next.State, err = be.GetWorkflowRuntimeState(ctx, next)
		require.NoError(t, err)
		require.NoError(t, be.CompleteWorkflowWorkItem(ctx, next))
		wait, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err = be.NextWorkflowWorkItem(wait)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NoError(t, be.Stop(ctx))
	}
}

func Test_ContinueAsNewDoesNotConsumePreviousExecutionTimer(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("timer-generations", func(w *task.WorkflowContext) (any, error) {
		var generation int
		if err := w.GetInput(&generation); err != nil {
			return nil, err
		}
		if generation == 0 {
			w.CreateTimer(time.Second)
			if err := w.WaitForSingleEvent("advance", -1).Await(nil); err != nil {
				return nil, err
			}
			w.ContinueAsNew(1)
			return nil, nil
		}
		err := w.CreateTimer(5 * time.Second).Await(nil)
		return "new-generation", err
	}))
	for i, be := range getRunnableBackends() {
		t.Run(fmt.Sprintf("%T-%d", be, i), func(t *testing.T) {
			require.NoError(t, be.CreateTaskHub(ctx))
			initTest(t, be, i, false)
			executor := task.NewTaskExecutor(r)
			worker := backend.NewTaskHubWorker(be,
				backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{Backend: be, Executor: executor, Logger: logger, AppID: "test"}),
				backend.NewActivityTaskWorker(be, executor, logger), logger)
			require.NoError(t, worker.Start(ctx))
			defer worker.Shutdown(ctx)
			cl := backend.NewTaskHubClient(be)
			id, err := cl.ScheduleNewWorkflow(ctx, "timer-generations", api.WithInput(0))
			require.NoError(t, err)
			wait, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			_, err = cl.WaitForWorkflowStart(wait, id)
			require.NoError(t, err)
			began := time.Now()
			require.NoError(t, cl.RaiseEvent(wait, id, "advance"))
			metadata, err := cl.WaitForWorkflowCompletion(wait, id)
			require.NoError(t, err)
			require.GreaterOrEqual(t, time.Since(began), 5*time.Second)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		})
	}
}
