//go:build linux

package control

import (
	"os"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsMissingFilesystemEntry(t *testing.T) {
	if code := status.Code(rpcError(os.ErrNotExist)); code != codes.NotFound {
		t.Fatalf("rpcError(os.ErrNotExist) code = %v, want %v", code, codes.NotFound)
	}
}
