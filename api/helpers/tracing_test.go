package helpers

import (
	"testing"

	"github.com/mafilus/durabletask-go/api/protos"
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
