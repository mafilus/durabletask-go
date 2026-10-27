package local

import (
	"context"
	"errors"
	"sync"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
)

// ErrTaskAlreadyPending rejects a second waiter without replacing the current
// registration. The rejected waiter owns no registry entry.
var ErrTaskAlreadyPending = errors.New("a completion waiter is already pending for this task")

type pendingWorkflow struct {
	response *protos.WorkflowResponse
	complete chan struct{}
}

type pendingActivity struct {
	response *protos.ActivityResponse
	complete chan struct{}
}

type TasksBackend struct {
	pendingWorkflows  *sync.Map
	pendingActivities *sync.Map
}

func NewTasksBackend() *TasksBackend {
	return &TasksBackend{
		pendingWorkflows:  &sync.Map{},
		pendingActivities: &sync.Map{},
	}
}

func (be *TasksBackend) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) error {
	if be.deletePendingActivityTask(response.GetInstanceId(), response.GetTaskId(), response) {
		return nil
	}

	return api.NewUnknownTaskIDError(response.GetInstanceId(), response.GetTaskId())
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	if be.deletePendingActivityTask(string(instanceID), taskID, nil) {
		return nil
	}
	return api.NewUnknownTaskIDError(instanceID.String(), taskID)
}

func (be *TasksBackend) WaitForActivityCompletion(request *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	wait, err := be.RegisterActivityCompletion(request)
	if err != nil {
		return func(context.Context) (*protos.ActivityResponse, error) { return nil, err }
	}
	return wait
}

// RegisterActivityCompletion reserves the completion key before work is dispatched.
func (be *TasksBackend) RegisterActivityCompletion(request *protos.ActivityRequest) (func(context.Context) (*protos.ActivityResponse, error), error) {
	key := backend.GetActivityExecutionKey(request.GetWorkflowInstance().GetInstanceId(), request.GetTaskId())
	pending := &pendingActivity{
		response: nil,
		complete: make(chan struct{}, 1),
	}
	if _, loaded := be.pendingActivities.LoadOrStore(key, pending); loaded {
		return nil, ErrTaskAlreadyPending
	}

	return func(ctx context.Context) (*protos.ActivityResponse, error) {
		// A canceled waiter must only remove its own attempt. Another attempt
		// with the same logical task key may already have replaced it.
		defer be.pendingActivities.CompareAndDelete(key, pending)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.complete:
			if pending.response == nil {
				return nil, api.ErrTaskCancelled
			}
			return pending.response, nil
		}
	}, nil
}

func (be *TasksBackend) CompleteWorkflowTask(ctx context.Context, response *protos.WorkflowResponse) error {
	if be.deletePendingWorkflow(response.GetInstanceId(), response) {
		return nil
	}
	return api.NewUnknownInstanceIDError(response.GetInstanceId())
}

func (be *TasksBackend) CancelWorkflowTask(ctx context.Context, instanceID api.InstanceID) error {
	if be.deletePendingWorkflow(string(instanceID), nil) {
		return nil
	}
	return api.NewUnknownInstanceIDError(instanceID.String())
}

func (be *TasksBackend) WaitForWorkflowTaskCompletion(request *protos.WorkflowRequest) func(context.Context) (*protos.WorkflowResponse, error) {
	wait, err := be.RegisterWorkflowTaskCompletion(request)
	if err != nil {
		return func(context.Context) (*protos.WorkflowResponse, error) { return nil, err }
	}
	return wait
}

// RegisterWorkflowTaskCompletion reserves the completion key before work is dispatched.
func (be *TasksBackend) RegisterWorkflowTaskCompletion(request *protos.WorkflowRequest) (func(context.Context) (*protos.WorkflowResponse, error), error) {
	pending := &pendingWorkflow{
		response: nil,
		complete: make(chan struct{}, 1),
	}
	if _, loaded := be.pendingWorkflows.LoadOrStore(request.GetInstanceId(), pending); loaded {
		return nil, ErrTaskAlreadyPending
	}

	return func(ctx context.Context) (*protos.WorkflowResponse, error) {
		defer be.pendingWorkflows.CompareAndDelete(request.GetInstanceId(), pending)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.complete:
			if pending.response == nil {
				return nil, api.ErrTaskCancelled
			}
			return pending.response, nil
		}
	}, nil
}

func (be *TasksBackend) deletePendingActivityTask(iid string, taskID int32, res *protos.ActivityResponse) bool {
	key := backend.GetActivityExecutionKey(iid, taskID)
	p, ok := be.pendingActivities.LoadAndDelete(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingActivity)
	pending.response = res
	close(pending.complete)
	return true
}

func (be *TasksBackend) deletePendingWorkflow(instanceID string, res *protos.WorkflowResponse) bool {
	p, ok := be.pendingWorkflows.LoadAndDelete(instanceID)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingWorkflow)
	pending.response = res
	close(pending.complete)
	return true
}
