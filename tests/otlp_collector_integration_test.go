//go:build integration && otlpcollector

package tests

import (
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/mafilus/durabletask-go/api/helpers"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/trace"
)

func TestIntegrationOTLPCollectorReceivesWorkflowSpan(t *testing.T) {
	if os.Getenv("STRIX_TEST_OTLP_COLLECTOR") != "1" {
		t.Skip("OTLP Collector integration; set STRIX_TEST_OTLP_COLLECTOR=1")
	}

	received := make(chan *collectortrace.ExportTraceServiceRequest, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		reader := io.Reader(r.Body)
		if r.Header.Get("Content-Encoding") == "gzip" {
			gzipReader, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer gzipReader.Close()
			reader = gzipReader
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		request := &collectortrace.ExportTraceServiceRequest{}
		var decodeErr error
		if strings.Contains(r.Header.Get("Content-Type"), "json") {
			decodeErr = protojson.Unmarshal(body, request)
		} else {
			decodeErr = proto.Unmarshal(body, request)
		}
		if decodeErr != nil {
			http.Error(w, decodeErr.Error(), http.StatusBadRequest)
			return
		}
		select {
		case received <- request:
		default:
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})}
	// The Collector runs in Docker and reaches the host through its gateway.
	listener, err := net.Listen("tcp", ":4319")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		require.NoError(t, server.Shutdown(context.Background()))
	})

	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("127.0.0.1:4318"),
		otlptracehttp.WithInsecure(),
	)
	require.NoError(t, err)
	provider := trace.NewTracerProvider(trace.WithBatcher(exporter))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
		otel.SetTracerProvider(previousProvider)
	})

	_, span := helpers.StartNewCreateWorkflowSpan(context.Background(), "otlp-e2e", "", "instance")
	span.End()
	require.NoError(t, provider.ForceFlush(context.Background()))

	select {
	case request := <-received:
		require.True(t, containsSpan(request, "create_orchestration||otlp-e2e"), "Collector did not forward the workflow span")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the Collector to forward OTLP traces")
	}
}

func containsSpan(request *collectortrace.ExportTraceServiceRequest, name string) bool {
	for _, resourceSpans := range request.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				if span.Name == name {
					return true
				}
			}
		}
	}
	return false
}
