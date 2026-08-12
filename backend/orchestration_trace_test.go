package backend

import (
	"context"
	"testing"
	"time"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestStartOrResumeWorkflowSpanUsesCurrentTurnStartTime(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
		otel.SetTracerProvider(previousProvider)
	})

	workflowStartedAt := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	turnStartedAt := workflowStartedAt.Add(5 * time.Minute)
	wi := &WorkflowWorkItem{State: &protos.WorkflowRuntimeState{
		StartEvent: &protos.ExecutionStartedEvent{
			Name: "workflow",
			WorkflowInstance: &protos.WorkflowInstance{
				InstanceId: "instance",
			},
			ParentTraceContext: &protos.TraceContext{
				TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			},
		},
	}}

	processor := &workflowProcessor{logger: DefaultLogger()}
	_, span := processor.startOrResumeWorkflowSpan(context.Background(), wi, turnStartedAt)
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, turnStartedAt, ended[0].StartTime())
	require.NotEqual(t, workflowStartedAt, ended[0].StartTime())
}
