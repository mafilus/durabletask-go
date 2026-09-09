package local

import (
	"context"
	"sync"
	"testing"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskCompletionBeforeWait(t *testing.T) {
	be := NewTasksBackend()
	waitWorkflow := be.WaitForWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "workflow"})
	waitActivity := be.WaitForActivityCompletion(&protos.ActivityRequest{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "workflow"}, TaskId: 42})
	workflowResponse := &protos.WorkflowResponse{InstanceId: "workflow"}
	activityResponse := &protos.ActivityResponse{InstanceId: "workflow", TaskId: 42}
	require.NoError(t, be.CompleteWorkflowTask(context.Background(), workflowResponse))
	require.NoError(t, be.CompleteActivityTask(context.Background(), activityResponse))
	gotWorkflow, err := waitWorkflow(context.Background())
	require.NoError(t, err)
	require.Same(t, workflowResponse, gotWorkflow)
	gotActivity, err := waitActivity(context.Background())
	require.NoError(t, err)
	require.Same(t, activityResponse, gotActivity)
}

func TestCanceledTaskWaiterRemovesOnlyItsAttempt(t *testing.T) {
	for _, kind := range []string{"workflow", "activity"} {
		for _, replacement := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/cleanup", true: "/replacement"}[replacement], func(t *testing.T) {
				be := NewTasksBackend()
				var pending *sync.Map
				var key string
				var register func() func(context.Context) error
				var complete func() error
				if kind == "workflow" {
					pending, key = be.pendingWorkflows, "workflow"
					register = func() func(context.Context) error {
						wait := be.WaitForWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "workflow"})
						return func(ctx context.Context) error { _, err := wait(ctx); return err }
					}
					complete = func() error {
						return be.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "workflow"})
					}
				} else {
					pending, key = be.pendingActivities, "workflow/42"
					register = func() func(context.Context) error {
						wait := be.WaitForActivityCompletion(&protos.ActivityRequest{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "workflow"}, TaskId: 42})
						return func(ctx context.Context) error { _, err := wait(ctx); return err }
					}
					complete = func() error {
						return be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "workflow", TaskId: 42})
					}
				}
				oldWait := register()
				var newWait func(context.Context) error
				if replacement {
					newWait = register()
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, oldWait(ctx), context.Canceled)
				_, exists := pending.Load(key)
				require.Equal(t, replacement, exists)
				if replacement {
					require.NoError(t, complete())
					require.NoError(t, newWait(context.Background()))
				}
			})
		}
	}
}

func TestTaskCompletionRacesCancellation(t *testing.T) {
	for range 100 {
		be := NewTasksBackend()
		waitWorkflow := be.WaitForWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "workflow"})
		waitActivity := be.WaitForActivityCompletion(&protos.ActivityRequest{WorkflowInstance: &protos.WorkflowInstance{InstanceId: "workflow"}, TaskId: 42})
		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			cancel()
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = be.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "workflow"})
			_ = be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "workflow", TaskId: 42})
		}()
		go func() {
			defer wg.Done()
			<-start
			workflow, err := waitWorkflow(ctx)
			if err != nil {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.Equal(t, "workflow", workflow.GetInstanceId())
			}
			activity, err := waitActivity(ctx)
			if err != nil {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.Equal(t, int32(42), activity.GetTaskId())
			}
		}()
		close(start)
		wg.Wait()
		_, exists := be.pendingWorkflows.Load("workflow")
		require.False(t, exists)
		_, exists = be.pendingActivities.Load("workflow/42")
		require.False(t, exists)
	}
}
