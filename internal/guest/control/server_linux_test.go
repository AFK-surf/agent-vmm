//go:build linux

package control

import (
	"os"
	"syscall"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsMissingFilesystemEntry(t *testing.T) {
	if code := status.Code(rpcError(os.ErrNotExist)); code != codes.NotFound {
		t.Fatalf("rpcError(os.ErrNotExist) code = %v, want %v", code, codes.NotFound)
	}
}

func TestRPCErrorMapsPIDLimit(t *testing.T) {
	err := rpcError(syscall.EAGAIN)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("rpcError(EAGAIN) code = %v, want %v", status.Code(err), codes.ResourceExhausted)
	}
	for _, detail := range status.Convert(err).Details() {
		if value, ok := detail.(*errdetails.ErrorInfo); ok && value.GetReason() == "PID_LIMIT_EXCEEDED" {
			return
		}
	}
	t.Fatalf("rpcError(EAGAIN) did not include PID_LIMIT_EXCEEDED: %v", err)
}
