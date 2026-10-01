package postgres

import (
	"context"
	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/runtimestate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"testing"
	"time"
)

func TestChildCollisionRollsBackScheduling(t *testing.T) {
	ctx := context.Background()
	be := newChildGenerationBackend(t, time.Second, time.Second)
	require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("parent", "g0", nil)}))
	wi := independentOriginLoad(t, be, "parent")
	require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("occupied", "other", nil)}))
	_, err := runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{CreateChildWorkflow: &protos.CreateChildWorkflowAction{Name: "child", InstanceId: "occupied"}}}}, nil, nil)
	require.NoError(t, err)
	require.ErrorIs(t, be.CompleteWorkflowWorkItem(ctx, wi), api.ErrDuplicateInstance)
	var count int
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM History WHERE InstanceID='parent'").Scan(&count))
	require.Zero(t, count, "collision must not acknowledge scheduling")
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM NewEvents WHERE InstanceID='parent'").Scan(&count))
	require.Equal(t, 1, count, "collision must retain original inbound event")
}

func TestChildResultDestinationHistoryMismatch(t *testing.T) {
	ctx := context.Background()
	be := newChildGenerationBackend(t, time.Second, time.Second)
	require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("parent", "g1", nil)}))
	require.NoError(t, be.CompleteWorkflowWorkItem(ctx, independentOriginLoad(t, be, "parent")))
	_, err := be.db.Exec(context.Background(), "UPDATE Instances SET ExecutionID='g0' WHERE InstanceID='parent'")
	require.NoError(t, err)
	parent := &protos.ParentInstanceInfo{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent", ExecutionId: wrapperspb.String("g1")}}
	require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("child", "child-g1", parent)}))
	wi := independentOriginLoad(t, be, "child")
	_, err = runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED}}}}, nil, nil)
	require.NoError(t, err)
	require.ErrorIs(t, be.CompleteWorkflowWorkItem(ctx, wi), backend.ErrChildExecutionProvenanceRequired)
	var count int
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM History WHERE InstanceID='child'").Scan(&count))
	require.Zero(t, count)
}

func TestGenericIngressRejectsChildResult(t *testing.T) {
	be := newChildGenerationBackend(t, time.Second, time.Second)
	event := &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceCompleted{ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{TaskScheduledId: 0}}}
	require.ErrorIs(t, be.AddNewWorkflowEvent(context.Background(), "parent", event), backend.ErrChildExecutionProvenanceRequired)
	var count int
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM NewEvents").Scan(&count))
	require.Zero(t, count)
}

func TestChildResultLegacyQueueAndStaleOnly(t *testing.T) {
	for _, generation := range []string{"", "g0", "g1"} {
		t.Run("generation-"+generation, func(t *testing.T) {
			ctx := context.Background()
			be := newChildGenerationBackend(t, time.Second, time.Second)
			require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("parent", "g1", nil)}))
			require.NoError(t, be.CompleteWorkflowWorkItem(ctx, independentOriginLoad(t, be, "parent")))
			result := &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceFailed{ChildWorkflowInstanceFailed: &protos.ChildWorkflowInstanceFailedEvent{TaskScheduledId: 0}}}
			payload, err := backend.MarshalHistoryEvent(result)
			require.NoError(t, err)
			_, err = be.db.Exec(context.Background(), "INSERT INTO NewEvents (InstanceID,EventPayload,ExecutionID) VALUES ('parent',$1,$2)", payload, generation)
			require.NoError(t, err)
			wi, err := be.GetWorkflowWorkItem(ctx)
			var count int
			require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM NewEvents").Scan(&count))
			switch generation {
			case "":
				require.ErrorIs(t, err, backend.ErrChildExecutionProvenanceRequired)
				require.Equal(t, 1, count)
			case "g0":
				require.ErrorIs(t, err, errNoWorkItems)
				require.Zero(t, count)
				require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM Instances WHERE LockedBy IS NOT NULL").Scan(&count))
				require.Zero(t, count)
			case "g1":
				require.NoError(t, err)
				require.Len(t, wi.NewEvents, 1)
				require.NotNil(t, wi.NewEvents[0].GetChildWorkflowInstanceFailed())
			}
		})
	}
}

func TestChildResultMissingCapturedParentRejectsAtomically(t *testing.T) {
	ctx := context.Background()
	be := newChildGenerationBackend(t, time.Second, time.Second)
	parent := &protos.ParentInstanceInfo{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent"}, TaskScheduledId: 0}
	require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("child", "child-g0", parent)}))
	wi := independentOriginLoad(t, be, "child")
	_, err := runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED}}}}, nil, nil)
	require.NoError(t, err)
	require.ErrorIs(t, be.CompleteWorkflowWorkItem(ctx, wi), backend.ErrChildExecutionProvenanceRequired)
	var count int
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM History").Scan(&count))
	require.Zero(t, count, "failed completion must roll back history")
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM NewEvents WHERE InstanceID='child'").Scan(&count))
	require.Equal(t, 1, count, "failed completion must retain owned inbound batch")
}

func TestIndependentOriginLateChildCompletionIsFenced(t *testing.T) {
	ctx := context.Background()
	be := newChildGenerationBackend(t, time.Second, time.Second)
	_, err := be.db.Exec(context.Background(), "INSERT INTO Instances (InstanceID,ExecutionID,Name,RuntimeStatus) VALUES ('parent','g1','parent','RUNNING')")
	require.NoError(t, err)
	start := &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{Name: "child", WorkflowInstance: &protos.WorkflowInstance{InstanceId: "child", ExecutionId: wrapperspb.String("child-g0")}, ParentInstance: &protos.ParentInstanceInfo{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent", ExecutionId: wrapperspb.String("g0")}, TaskScheduledId: 0}}}}
	require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: start}))
	wi, err := be.GetWorkflowWorkItem(ctx)
	require.NoError(t, err)
	wi.State, err = be.GetWorkflowRuntimeState(ctx, wi)
	require.NoError(t, err)
	runtimestate.AddEvents(wi.State, wi.NewEvents)
	_, err = runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, Result: wrapperspb.String("old-child-result")}}}}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
	parentWI, err := be.GetWorkflowWorkItem(ctx)
	require.ErrorIs(t, err, errNoWorkItems, "old child result must not reach parent's new execution: %#v", parentWI)
}

func TestIndependentOriginAutoChildAcrossGenerations(t *testing.T) {
	ids := make([]string, 0, 2)
	for _, generation := range []string{"g0", "g1"} {
		start := &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{Name: "parent", WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent", ExecutionId: wrapperspb.String(generation)}}}}
		state := runtimestate.NewWorkflowRuntimeState("parent", nil, []*protos.HistoryEvent{start})
		_, err := runtimestate.NewApplier("test", "").Actions(state, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{CreateChildWorkflow: &protos.CreateChildWorkflowAction{Name: "child"}}}}, nil, nil)
		require.NoError(t, err)
		ids = append(ids, state.PendingMessages[0].TargetInstanceId)
	}
	require.NotEqual(t, ids[0], ids[1], "two generations require distinct auto-generated child identities")
}

func TestIndependentOriginAutoChildFullActionIdentity(t *testing.T) {
	ids := make([]string, 0, 3)
	for _, actionID := range []int32{0, 65536, 0} {
		start := independentOriginStart("parent", "g0", nil)
		state := runtimestate.NewWorkflowRuntimeState("parent", nil, []*protos.HistoryEvent{start})
		_, err := runtimestate.NewApplier("test", "").Actions(state, nil, []*protos.WorkflowAction{{Id: actionID, WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{CreateChildWorkflow: &protos.CreateChildWorkflowAction{Name: "child"}}}}, nil, nil)
		require.NoError(t, err)
		ids = append(ids, state.PendingMessages[0].TargetInstanceId)
	}
	require.NotEqual(t, ids[0], ids[1], "full action ID must distinguish different children")
	require.Equal(t, ids[0], ids[2], "same execution and action must retain deterministic identity")
}

func TestIndependentOriginExternalDeliveryCannotBypassChildFence(t *testing.T) {
	be := newChildGenerationBackend(t, time.Second, time.Second)
	e := &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceCompleted{ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{TaskScheduledId: 0, Result: wrapperspb.String("unqualified-child-result")}}}
	err := be.AddNewWorkflowEventWithExternalDelivery(context.Background(), "parent", "fake-name", "fake-delivery", e)
	require.Error(t, err, "external receipt ingress must not accept a child result without parent generation")
	var count int
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM NewEvents").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM ExternalEventDeliveries").Scan(&count))
	require.Zero(t, count, "reject before committing receipt too")
}

func independentOriginStart(instance, execution string, parent *protos.ParentInstanceInfo) *protos.HistoryEvent {
	return &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{Name: instance, WorkflowInstance: &protos.WorkflowInstance{InstanceId: instance, ExecutionId: wrapperspb.String(execution)}, ParentInstance: parent}}}
}

func independentOriginLoad(t *testing.T, be *postgresBackend, expected string) *backend.WorkflowWorkItem {
	t.Helper()
	wi, err := be.GetWorkflowWorkItem(context.Background())
	require.NoError(t, err)
	require.Equal(t, expected, string(wi.InstanceID))
	wi.State, err = be.GetWorkflowRuntimeState(context.Background(), wi)
	require.NoError(t, err)
	runtimestate.AddEvents(wi.State, wi.NewEvents)
	return wi
}

func independentOriginCompleteChild(t *testing.T, be *postgresBackend, wi *backend.WorkflowWorkItem) {
	t.Helper()
	_, err := runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 0, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, Result: wrapperspb.String("old-child-result")}}}}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, be.CompleteWorkflowWorkItem(context.Background(), wi))
}

func TestIndependentOriginChildCompletionBoundaryBothOrders(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(map[bool]string{false: "after-CAN", true: "before-CAN"}[before], func(t *testing.T) {
			ctx := context.Background()
			be := newChildGenerationBackend(t, time.Second, time.Second)
			require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("parent", "g0", nil)}))
			require.NoError(t, be.CompleteWorkflowWorkItem(ctx, independentOriginLoad(t, be, "parent")))
			parent := &protos.ParentInstanceInfo{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent", ExecutionId: wrapperspb.String("g0")}, TaskScheduledId: 0}
			require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("child", "child-g0", parent)}))
			child := independentOriginLoad(t, be, "child")
			if before {
				independentOriginCompleteChild(t, be, child)
			}
			require.NoError(t, be.AddNewWorkflowEvent(ctx, "parent", &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EventRaised{EventRaised: &protos.EventRaisedEvent{Name: "CAN"}}}))
			wi := independentOriginLoad(t, be, "parent")
			_, err := runtimestate.NewApplier("test", "").Actions(wi.State, nil, []*protos.WorkflowAction{{Id: 1, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW}}}}, nil, nil)
			require.NoError(t, err)
			require.NoError(t, be.CompleteWorkflowWorkItem(ctx, wi))
			if !before {
				independentOriginCompleteChild(t, be, child)
			}
			next, err := be.GetWorkflowWorkItem(ctx)
			require.ErrorIs(t, err, errNoWorkItems, "obsolete completion leaked: %#v", next)
		})
	}
}

func TestIndependentOriginChildCompletionAfterDestinationPurge(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "recreated"}[recreate], func(t *testing.T) {
			ctx := context.Background()
			be := newChildGenerationBackend(t, time.Second, time.Second)
			require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("parent", "g0", nil)}))
			require.NoError(t, be.CompleteWorkflowWorkItem(ctx, independentOriginLoad(t, be, "parent")))
			parent := &protos.ParentInstanceInfo{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent", ExecutionId: wrapperspb.String("g0")}, TaskScheduledId: 0}
			require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("child", "child-g0", parent)}))
			child := independentOriginLoad(t, be, "child")
			require.NoError(t, be.AddNewWorkflowEvent(ctx, "parent", &protos.HistoryEvent{Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EventRaised{EventRaised: &protos.EventRaisedEvent{Name: "complete"}}}))
			parentWI := independentOriginLoad(t, be, "parent")
			_, err := runtimestate.NewApplier("test", "").Actions(parentWI.State, nil, []*protos.WorkflowAction{{Id: 1, WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED}}}}, nil, nil)
			require.NoError(t, err)
			require.NoError(t, be.CompleteWorkflowWorkItem(ctx, parentWI))
			_, err = be.PurgeWorkflowState(ctx, api.InstanceID("parent"), nil, false, false)
			require.NoError(t, err)
			if recreate {
				require.NoError(t, be.CreateWorkflowInstance(ctx, &backend.CreateWorkflowInstanceRequest{StartEvent: independentOriginStart("parent", "g1", nil)}))
				require.NoError(t, be.CompleteWorkflowWorkItem(ctx, independentOriginLoad(t, be, "parent")))
			}
			independentOriginCompleteChild(t, be, child)
			var count int
			require.NoError(t, be.db.QueryRow(context.Background(), "SELECT COUNT(*) FROM NewEvents WHERE InstanceID='parent'").Scan(&count))
			require.Zero(t, count, "child completion must not outlive or target a recreated parent")
		})
	}
}

func newChildGenerationBackend(t *testing.T, workflowLease, activityLease time.Duration) *postgresBackend {
	t.Helper()
	be := newDurabilityBackend(t, workflowLease, activityLease)
	resetDurabilityTables(t, context.Background(), be)
	return be
}
