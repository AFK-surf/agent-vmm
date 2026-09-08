//go:build linux

package control

import (
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsMissingFilesystemEntry(t *testing.T) {
	if code := status.Code(rpcError(os.ErrNotExist)); code != codes.NotFound {
		t.Fatalf("rpcError(os.ErrNotExist) code = %v, want %v", code, codes.NotFound)
	}
}

func TestRPCErrorReportsSafeRuntimeStageForMissingResource(t *testing.T) {
	err := rpcError(atRuntimeStage("rootfs_snapshot", status.Error(codes.NotFound, "/state/private/snapshot")))
	if status.Code(err) != codes.NotFound || strings.Contains(err.Error(), "/state/") {
		t.Fatalf("staged missing resource error = %v", err)
	}
	for _, detail := range status.Convert(err).Details() {
		if value, ok := detail.(*errdetails.ErrorInfo); ok && value.GetReason() == "RUNTIME_RESOURCE_NOT_FOUND" && value.GetMetadata()["stage"] == "rootfs_snapshot" {
			return
		}
	}
	t.Fatalf("staged missing resource error omitted safe stage: %v", err)
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

func TestPhysicalCapacityErrorCarriesFiniteOwnerDimension(t *testing.T) {
	for _, dimension := range []resourcev2.CapacityDimension{resourcev2.CapacityStorageHeadroom, resourcev2.CapacityImportSlot} {
		err := rpcError(resourceCapacityExhausted(dimension, "admission", "capacity unavailable"))
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("dimension %q code=%v", dimension, status.Code(err))
		}
		found := false
		for _, detail := range status.Convert(err).Details() {
			if value, ok := detail.(*errdetails.ErrorInfo); ok && value.GetMetadata()["resource_dimension"] == string(dimension) {
				found = true
			}
		}
		if !found {
			t.Fatalf("dimension %q omitted structured capacity owner: %v", dimension, err)
		}
	}
}

func TestPhysicalCapacityErrorCarriesStageAndApplicableBytes(t *testing.T) {
	err := rpcError(resourceCapacityExhaustedWithBytes(resourcev2.CapacityStorageHeadroom, "import_admission", "capacity unavailable", 1024, 2048))
	for _, detail := range status.Convert(err).Details() {
		if value, ok := detail.(*errdetails.ErrorInfo); ok {
			metadata := value.GetMetadata()
			if value.GetReason() == "RESOURCE_CAPACITY_EXHAUSTED" && metadata["stage"] == "import_admission" && metadata["resource_dimension"] == "storage_headroom" && metadata["available_bytes"] == "1024" && metadata["required_bytes"] == "2048" {
				return
			}
		}
	}
	t.Fatalf("capacity error omitted canonical stage or byte bounds: %v", err)
}
