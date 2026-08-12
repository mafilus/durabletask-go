package backend

import (
	"context"
	"testing"

	"github.com/mafilus/durabletask-go/api/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGrpcExecutorStartInstanceRejectsInvalidTraceParent(t *testing.T) {
	const prefix = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-"
	for _, flags := range []string{"", "0", "zz", "000"} {
		t.Run(flags, func(t *testing.T) {
			g := &grpcExecutor{}
			_, err := g.StartInstance(context.Background(), &protos.CreateInstanceRequest{
				ParentTraceContext: &protos.TraceContext{TraceParent: prefix + flags},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("StartInstance error code=%s, want %s (error=%v)", status.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}
