package helpers

import (
	"context"
	"testing"

	"github.com/mafilus/durabletask-go/api/protos"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSpanContextFromTraceContextRejectsInvalidTraceFlags(t *testing.T) {
	const prefix = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-"
	for _, flags := range []string{"", "0", "zz", "000"} {
		t.Run(flags, func(t *testing.T) {
			_, err := SpanContextFromTraceContext(&protos.TraceContext{TraceParent: prefix + flags})
			if err == nil {
				t.Fatalf("TraceParent flags %q accepted", flags)
			}
		})
	}
}

func TestStartNewActivitySpanUsesPersistedTraceContextAsParent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
		otel.SetTracerProvider(previousProvider)
	})

	parentTraceContext := &protos.TraceContext{
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	parent, err := SpanContextFromTraceContext(parentTraceContext)
	require.NoError(t, err)
	ctx, err := ContextFromTraceContext(context.Background(), parentTraceContext)
	require.NoError(t, err)

	_, span := StartNewActivitySpan(ctx, "activity", "", "instance", 1)
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, parent.TraceID(), ended[0].SpanContext().TraceID())
	require.Equal(t, parent.SpanID(), ended[0].Parent().SpanID())
	require.NotEqual(t, parent.SpanID(), ended[0].SpanContext().SpanID())
}
