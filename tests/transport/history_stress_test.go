package transport_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"
)

type historyStressInput struct{ Generation, Activity int }

type historyStressRPCKey struct{}

type historyStressStats struct {
	nextRPC    atomic.Int64
	deltas     atomic.Int64
	mu         sync.Mutex
	streams    map[string]map[int64]bool
	executions map[string]map[string]bool
}

func (s *historyStressStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, historyStressRPCKey{}, s.nextRPC.Add(1))
}
func (s *historyStressStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *historyStressStats) HandleConn(context.Context, stats.ConnStats) {}
func (s *historyStressStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	payload, ok := event.(*stats.OutPayload)
	if !ok {
		return
	}
	item, ok := payload.Payload.(*protos.WorkItem)
	if !ok || item.GetWorkflowRequest() == nil {
		return
	}
	request := item.GetWorkflowRequest()
	if request.CachedHistory != nil {
		s.deltas.Add(1)
	}
	stream, _ := ctx.Value(historyStressRPCKey{}).(int64)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams[request.InstanceId] == nil {
		s.streams[request.InstanceId] = make(map[int64]bool)
		s.executions[request.InstanceId] = make(map[string]bool)
	}
	s.streams[request.InstanceId][stream] = true
	s.executions[request.InstanceId][request.GetExecutionId().GetValue()] = true
}

func TestStressMultipleStreamsContinueAsNewDuringDisconnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const workflows, generations, fanout, streams, rotations = 32, 6, 4, 4, 12
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(filepath.Join(t.TempDir(), "history-stress.sqlite")), logger)
	executor, register := backend.NewGrpcExecutor(be, logger, backend.WithRequireCompletionTokens())
	worker := backend.NewTaskHubWorker(be,
		backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{Backend: be, Executor: executor, Logger: logger, AppID: "history-stress"}, backend.WithMaxParallelism(32)),
		backend.NewActivityTaskWorker(be, executor, logger, backend.WithMaxParallelism(64)), logger)
	require.NoError(t, worker.Start(ctx))
	observed := &historyStressStats{streams: make(map[string]map[int64]bool), executions: make(map[string]map[string]bool)}
	server := grpc.NewServer(grpc.StatsHandler(observed))
	register(server)
	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///history-stress", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		server.Stop()
		_ = conn.Close()
		_ = listener.Close()
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		require.NoError(t, executor.Shutdown(shutdown))
		require.NoError(t, worker.Shutdown(shutdown))
	})
	registry := task.NewTaskRegistry()
	var calls atomic.Int64
	workStarted := make(chan struct{})
	var started sync.Once
	require.NoError(t, registry.AddActivityN("GenerationValue", func(a task.ActivityContext) (any, error) {
		var input historyStressInput
		if err := a.GetInput(&input); err != nil {
			return nil, err
		}
		calls.Add(1)
		started.Do(func() { close(workStarted) })
		timer := time.NewTimer(time.Millisecond)
		defer timer.Stop()
		select {
		case <-a.Context().Done():
			return nil, a.Context().Err()
		case <-timer.C:
		}
		return input.Generation*100 + input.Activity, nil
	}))
	require.NoError(t, registry.AddWorkflowN("HistoryGenerations", func(w *task.WorkflowContext) (any, error) {
		var generation int
		if err := w.GetInput(&generation); err != nil {
			return nil, err
		}
		tasks := make([]task.Task, fanout)
		for i := range tasks {
			tasks[i] = w.CallActivity("GenerationValue", task.WithActivityInput(historyStressInput{Generation: generation, Activity: i}))
		}
		for i, activity := range tasks {
			var value int
			if err := activity.Await(&value); err != nil {
				return nil, err
			}
			if value != generation*100+i {
				return nil, fmt.Errorf("history crossed execution boundary: generation=%d activity=%d value=%d", generation, i, value)
			}
		}
		// A further turn in this generation exercises a non-empty cached
		// prefix before the next ContinueAsNew replaces it.
		if err := w.CreateTimer(time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		if generation+1 < generations {
			w.ContinueAsNew(generation + 1)
			return nil, nil
		}
		return generation, nil
	}))
	openWorker := func() (context.CancelFunc, error) {
		streamCtx, stop := context.WithCancel(ctx)
		client := workerclient.NewTaskHubGrpcClient(conn, logger)
		if err := client.StartWorkItemListener(streamCtx, registry); err != nil {
			stop()
			return nil, err
		}
		return stop, nil
	}
	stops := make([]context.CancelFunc, streams)
	for i := range stops {
		stops[i], err = openWorker()
		require.NoError(t, err)
	}
	// Only this goroutine changes stops. Its completion joins all restarts before
	// the test finishes; final cleanup stops the remaining listener contexts.
	churnDone := make(chan error, 1)
	go func() {
		defer func() {
			for _, stop := range stops {
				if stop != nil {
					stop()
				}
			}
		}()
		select {
		case <-workStarted:
		case <-ctx.Done():
			churnDone <- ctx.Err()
			return
		}
		for rotation := 0; rotation < rotations; rotation++ {
			timer := time.NewTimer(5 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				churnDone <- ctx.Err()
				return
			}
			index := rotation % streams
			stops[index]()
			stop, err := openWorker()
			if err != nil {
				churnDone <- err
				return
			}
			stops[index] = stop
		}
		// Keep the remaining streams available until the workflows are done.
		<-ctx.Done()
		churnDone <- nil
	}()
	cl := backend.NewTaskHubClient(be)
	ids := make([]api.InstanceID, workflows)
	for i := range ids {
		ids[i], err = cl.ScheduleNewWorkflow(ctx, "HistoryGenerations", api.WithInput(0), api.WithInstanceID(api.InstanceID(fmt.Sprintf("history-stress-%02d", i))))
		require.NoError(t, err)
	}
	errs := make(chan error, workflows)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			metadata, err := cl.WaitForWorkflowCompletion(ctx, id, api.WithFetchPayloads(true))
			if err != nil {
				errs <- fmt.Errorf("%s: %w", id, err)
				return
			}
			if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED || metadata.Output.GetValue() != fmt.Sprint(generations-1) {
				errs <- fmt.Errorf("%s: status=%s output=%s failure=%s", id, metadata.RuntimeStatus, metadata.Output.GetValue(), metadata.GetFailureDetails().GetErrorMessage())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	cancel()
	require.NoError(t, <-churnDone)
	require.GreaterOrEqual(t, calls.Load(), int64(workflows*generations*fanout))
	require.Positive(t, observed.deltas.Load(), "workload never exercised cached history")
	observed.mu.Lock()
	defer observed.mu.Unlock()
	switched := 0
	for _, id := range ids {
		if len(observed.streams[string(id)]) > 1 {
			switched++
		}
		require.GreaterOrEqual(t, len(observed.executions[string(id)]), generations)
	}
	require.Positive(t, switched, "no workflow changed streams")
	t.Logf("%d workflows completed %d generations on %d streams with %d restarts; %d changed streams, %d delta histories, %d activity executions including permitted redeliveries", workflows, generations, streams, rotations, switched, observed.deltas.Load(), calls.Load())
}
