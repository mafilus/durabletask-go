package transport_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/mafilus/durabletask-go/backend"
	"github.com/mafilus/durabletask-go/backend/sqlite"
	workerclient "github.com/mafilus/durabletask-go/client"
	"github.com/mafilus/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type receiveBudgetLogger struct {
	backend.Logger
	once    sync.Once
	stopped chan string
}

func (l *receiveBudgetLogger) Errorf(format string, args ...any) {
	l.Logger.Errorf(format, args...)
	if strings.Contains(format, "work item listener stopped") {
		l.once.Do(func() { l.stopped <- format })
	}
}

func TestLargeWorkItemReceiveBudget(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "default-allows-five-MiB", true: "configured-limit-stops-after-bounded-retries"}[limited], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			logger := &receiveBudgetLogger{Logger: backend.DefaultLogger(), stopped: make(chan string, 1)}
			be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
			executor, register := backend.NewGrpcExecutor(be, logger, backend.WithRequireCompletionTokens())
			worker := backend.NewTaskHubWorker(be,
				backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{Backend: be, Executor: executor, Logger: logger}),
				backend.NewActivityTaskWorker(be, executor, logger), logger)
			require.NoError(t, worker.Start(ctx))
			var streams atomic.Int32
			streamEnded := make(chan struct{}, 4)
			server := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if strings.HasSuffix(info.FullMethod, "/GetWorkItems") {
					streams.Add(1)
					defer func() { streamEnded <- struct{}{} }()
				}
				return handler(srv, stream)
			}))
			register(server)
			listener := bufconn.Listen(1 << 20)
			go func() { _ = server.Serve(listener) }()
			conn, err := grpc.NewClient("passthrough:///large-item", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() {
				cancel()
				stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				require.NoError(t, executor.Shutdown(stopCtx))
				require.NoError(t, worker.Shutdown(stopCtx))
				server.Stop()
				_ = conn.Close()
				_ = listener.Close()
			})
			listenerErrors := make(chan error, 1)
			opts := []workerclient.TaskHubGrpcClientOption{workerclient.WithWorkItemListenerErrorHandler(func(err error) { listenerErrors <- err })}
			if limited {
				opts = append(opts, workerclient.WithWorkItemMaxReceiveBytes(4<<20))
			}
			cl := workerclient.NewTaskHubGrpcClient(conn, logger, opts...)
			r := task.NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", func(ctx *task.WorkflowContext) (any, error) {
				var out string
				err := ctx.CallActivity("large").Await(&out)
				return len(out), err
			}))
			require.NoError(t, r.AddActivityN("large", func(task.ActivityContext) (any, error) { return strings.Repeat("x", 5<<20), nil }))
			require.NoError(t, cl.StartWorkItemListener(ctx, r))
			id, err := cl.ScheduleNewWorkflow(ctx, "wf")
			require.NoError(t, err)
			if limited {
				select {
				case err := <-listenerErrors:
					require.ErrorIs(t, err, workerclient.ErrWorkItemStreamExhausted)
				case <-ctx.Done():
					t.Fatal("oversized work item did not stop the listener")
				}
				for range 3 {
					select {
					case <-streamEnded:
					case <-ctx.Done():
						t.Fatal("failed listener did not release its streams")
					}
				}
				require.Equal(t, int32(3), streams.Load(), "persistent rejection must exhaust a finite recovery budget")
			} else {
				metadata, err := cl.WaitForWorkflowCompletion(ctx, id)
				require.NoError(t, err)
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
				require.Equal(t, "5242880", metadata.Output.GetValue())
			}
		})
	}
}
