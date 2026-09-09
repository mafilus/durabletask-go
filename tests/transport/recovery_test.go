package transport_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api"
	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/sqlite"
	workerclient "github.com/mafilus/durabletask-go/client"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type observedBackend struct {
	backend.Backend
	workflowAbandoned chan error
	activityAbandoned chan error
}

func (b *observedBackend) AbandonWorkflowWorkItem(ctx context.Context, wi *backend.WorkflowWorkItem) error {
	err := b.Backend.AbandonWorkflowWorkItem(ctx, wi)
	b.workflowAbandoned <- err
	return err
}

func (b *observedBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	err := b.Backend.AbandonActivityWorkItem(ctx, wi)
	b.activityAbandoned <- err
	return err
}

// This exercises durable abandonment and reacquisition, not only cancellation of
// an executor waiter: both leases are released by the worker into SQLite before
// a fresh stream receives the same work with a different completion token.
func TestDisconnectedStreamRedeliversWorkflowAndActivity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logger := backend.DefaultLogger()
	be := &observedBackend{
		Backend:           sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(filepath.Join(t.TempDir(), "recovery.sqlite")), logger),
		workflowAbandoned: make(chan error, 8), activityAbandoned: make(chan error, 8),
	}
	executor, register := backend.NewGrpcExecutor(be, logger, backend.WithRequireCompletionTokens())
	worker := backend.NewTaskHubWorker(be,
		backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{Backend: be, Executor: executor, Logger: logger, AppID: "recovery"}),
		backend.NewActivityTaskWorker(be, executor, logger), logger)
	require.NoError(t, worker.Start(ctx))
	server := grpc.NewServer()
	register(server)
	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///recovery", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, executor.Shutdown(shutdownCtx))
		require.NoError(t, worker.Shutdown(shutdownCtx))
		server.Stop()
		_ = conn.Close()
		_ = listener.Close()
	})
	client := protos.NewTaskHubSidecarServiceClient(conn)
	newStream := func() (protos.TaskHubSidecarService_GetWorkItemsClient, context.CancelFunc) {
		streamCtx, closeStream := context.WithCancel(ctx)
		s, err := client.GetWorkItems(streamCtx, &protos.GetWorkItemsRequest{Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY}})
		require.NoError(t, err)
		return s, closeStream
	}
	receive := func(s protos.TaskHubSidecarService_GetWorkItemsClient) *protos.WorkItem {
		wi, err := s.Recv()
		require.NoError(t, err)
		require.NotEmpty(t, wi.GetCompletionToken())
		return wi
	}
	waitAbandon := func(ch <-chan error) {
		select {
		case err := <-ch:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("durable work item was not abandoned:", ctx.Err())
		}
	}
	registry := task.NewTaskRegistry()
	registry.AddWorkflowN("Recovery", func(ctx *task.WorkflowContext) (any, error) {
		var result string
		err := ctx.CallActivity("RecoverActivity").Await(&result)
		return result, err
	})
	taskExecutor := task.NewTaskExecutor(registry)
	completeWorkflow := func(wi *protos.WorkItem) {
		req := wi.GetWorkflowRequest()
		require.NotNil(t, req)
		require.Nil(t, req.CachedHistory, "fresh stream must receive full history")
		resp, err := taskExecutor.ExecuteWorkflow(ctx, api.InstanceID(req.InstanceId), req.PastEvents, req.NewEvents, backend.ExecuteOptions{})
		require.NoError(t, err)
		resp.InstanceId, resp.CompletionToken = req.InstanceId, wi.CompletionToken
		_, err = client.CompleteWorkflowTask(ctx, resp)
		require.NoError(t, err)
	}

	streamA, closeA := newStream()
	defer closeA()
	_, err = backend.NewTaskHubClient(be).ScheduleNewWorkflow(ctx, "Recovery", api.WithInstanceID("recovery"))
	require.NoError(t, err)
	workflowA := receive(streamA)
	require.NotNil(t, workflowA.GetWorkflowRequest())
	closeA()
	waitAbandon(be.workflowAbandoned)
	streamB, closeB := newStream()
	defer closeB()
	workflowB := receive(streamB)
	require.NotEqual(t, workflowA.CompletionToken, workflowB.CompletionToken)
	require.Equal(t, workflowA.GetWorkflowRequest().InstanceId, workflowB.GetWorkflowRequest().InstanceId)
	_, err = client.CompleteWorkflowTask(ctx, &protos.WorkflowResponse{InstanceId: "recovery", CompletionToken: workflowA.CompletionToken})
	require.Error(t, err, "old attempt must not complete the reacquired workflow")
	completeWorkflow(workflowB)

	activityA := receive(streamB)
	require.NotNil(t, activityA.GetActivityRequest())
	closeB()
	waitAbandon(be.activityAbandoned)
	streamC, closeC := newStream()
	defer closeC()
	activityB := receive(streamC)
	require.NotNil(t, activityB.GetActivityRequest())
	require.NotEqual(t, activityA.CompletionToken, activityB.CompletionToken)
	require.Equal(t, activityA.GetActivityRequest().TaskId, activityB.GetActivityRequest().TaskId)
	response := &protos.ActivityResponse{InstanceId: "recovery", TaskId: activityB.GetActivityRequest().TaskId, CompletionToken: activityA.CompletionToken, Result: wrapperspb.String(`"recovered"`)}
	_, err = client.CompleteActivityTask(ctx, response)
	require.Error(t, err, "old attempt must not complete the reacquired activity")
	response.CompletionToken = activityB.CompletionToken
	_, err = client.CompleteActivityTask(ctx, response)
	require.NoError(t, err)
	completeWorkflow(receive(streamC))
	metadata, err := backend.NewTaskHubClient(be).WaitForWorkflowCompletion(ctx, "recovery", api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.True(t, api.WorkflowMetadataIsComplete(metadata))
	require.Equal(t, `"recovered"`, metadata.Output.GetValue())

	// Run the same workflow through the actual worker listener as well: the
	// strict server verifies that Recv forwards tokens into both response paths.
	closeC()
	registry.AddActivityN("RecoverActivity", func(task.ActivityContext) (any, error) { return "client-recovered", nil })
	listenerCtx, stopListener := context.WithCancel(ctx)
	defer stopListener()
	workerClient := workerclient.NewTaskHubGrpcClient(conn, logger)
	require.NoError(t, workerClient.StartWorkItemListener(listenerCtx, registry))
	_, err = backend.NewTaskHubClient(be).ScheduleNewWorkflow(ctx, "Recovery", api.WithInstanceID("real-client"))
	require.NoError(t, err)
	metadata, err = backend.NewTaskHubClient(be).WaitForWorkflowCompletion(ctx, "real-client", api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.True(t, api.WorkflowMetadataIsComplete(metadata))
	require.Equal(t, `"client-recovered"`, metadata.Output.GetValue())
}
