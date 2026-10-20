//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	containerd "github.com/containerd/containerd/v2/client"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsMissingFilesystemEntry(t *testing.T) {
	source := fmt.Errorf("read namespace image manifest: %w", os.ErrNotExist)
	if err := rpcError(source); status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), source.Error()) {
		t.Fatalf("missing source error = %v", err)
	}
}

func TestRPCErrorReportsRuntimeStageAndSourceForMissingResource(t *testing.T) {
	source := "/state/private/snapshot" + strings.Repeat("/segment", 100)
	err := rpcError(atRuntimeStage("rootfs_snapshot", status.Error(codes.NotFound, source)))
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), source) {
		t.Fatalf("staged missing resource error = %v", err)
	}
	for _, detail := range status.Convert(err).Details() {
		if value, ok := detail.(*errdetails.ErrorInfo); ok && value.GetReason() == "RUNTIME_RESOURCE_NOT_FOUND" && value.GetMetadata()["stage"] == "rootfs_snapshot" {
			return
		}
	}
	t.Fatalf("staged missing resource error omitted stage: %v", err)
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

func TestRPCErrorPreservesRealStorageCapacityOwnerFailures(t *testing.T) {
	requireGuestRoot(t)
	budget := new(guestdisk.Budget)
	admission := budget.ReserveImportHeadroom(func(*guestdisk.Transaction) (guestdisk.Snapshot, error) {
		return guestdisk.Snapshot{CapacityBytes: guestdisk.MinimumImportBytes - 1}, nil
	}, func(uint64) error { t.Fatal("rejected capacity must not commit"); return nil })
	_, overflow := namespaceQuotaBytes(math.MaxUint64)
	facts := &operationNamespaces{labels: map[string]map[string]string{}}
	client, err := containerd.New("", containerd.WithServices(containerd.WithNamespaceService(facts)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := os.MkdirAll(stateRoot, 0700); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{client: client}
	snapshot, err := runtime.guestDiskSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	namespaceAdmission := runtime.admitNamespaceDiskGrowth(context.Background(), "capacity-fixture", snapshot.CapacityBytes+max(snapshot.AllocationUnitBytes, uint64(1024)), nil)
	for _, source := range []error{namespaceAdmission, admission, fmt.Errorf("namespace admission: %w", namespaceAdmission), atRuntimeStage("namespace_admission", overflow)} {
		if !errors.Is(source, guestdisk.ErrCapacityUnavailable) {
			t.Fatalf("producer error = %v", source)
		}
		mapped := rpcError(source)
		if status.Code(mapped) != codes.ResourceExhausted || !strings.Contains(mapped.Error(), source.Error()) {
			t.Fatalf("storage capacity lost owner/cause: %v", mapped)
		}
		found := false
		for _, detail := range status.Convert(mapped).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() == "RESOURCE_CAPACITY_EXHAUSTED" && info.GetDomain() == "agent-vmm" && info.GetMetadata()["resource_dimension"] == "storage_headroom" {
				stage := "storage_admission"
				var staged runtimeStageError
				if errors.As(source, &staged) {
					stage = staged.stage
				}
				if info.GetMetadata()["stage"] != stage {
					t.Fatalf("storage stage = %v", info.GetMetadata())
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("storage capacity omitted typed detail: %v", mapped)
		}
	}
	for _, source := range []error{errors.New(guestdisk.ErrCapacityUnavailable.Error()), guestdisk.ErrGrowthSealed} {
		if mapped := rpcError(source); status.Code(mapped) != codes.Internal {
			t.Fatalf("non-capacity owner promoted to wait: %v", mapped)
		}
	}
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

func TestServiceProjectionCleanupDeduplicationUsesCurrentRouteIdentity(t *testing.T) {
	server := new(Server)
	ctx := context.Background()
	active, deletions := false, 0
	apply := func(revision, generation uint64) {
		t.Helper()
		request := &vmmdv1.ApplyServiceProjectionRequest{RequestId: fmt.Sprintf("apply-r%d-g%d", revision, generation), Projection: &vmmdv1.ServiceProjection{Id: "import"}}
		if _, err := server.mutate(ctx, "apply-service-projection", request, func() (any, error) {
			active = true
			return new(vmmdv1.ApplyServiceProjectionResponse), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	cleanup := func(revision, generation uint64) {
		t.Helper()
		request := &vmmdv1.DeleteServiceProjectionRequest{RequestId: fmt.Sprintf("cleanup-r%d-g%d", revision, generation), ProjectionId: "import"}
		if _, err := server.mutate(ctx, "delete-service-projection", request, func() (any, error) {
			active = false
			deletions++
			return new(vmmdv1.DeleteServiceProjectionResponse), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(1, 11)
	cleanup(2, 12)
	cleanup(2, 12)
	if active || deletions != 1 {
		t.Fatalf("lost cleanup ACK repeated mutation: active=%t deletions=%d", active, deletions)
	}
	apply(3, 21)
	cleanup(2, 12)
	if !active || deletions != 1 {
		t.Fatal("old successful cleanup receipt changed replacement projection")
	}
	cleanup(4, 22)
	cleanup(4, 22)
	if active || deletions != 2 {
		t.Fatalf("current route cleanup reused historical receipt: active=%t deletions=%d", active, deletions)
	}
}

func TestStreamDeadlinePreservesConnectionAndOperationLifetimes(t *testing.T) {
	for _, method := range []string{vmmdv1.VMMDService_OpenSystemStream_FullMethodName, vmmdv1.VMMDService_Exec_FullMethodName, vmmdv1.VMMDService_Logs_FullMethodName} {
		for _, callerDeadline := range []bool{false, true} {
			t.Run(method+"/caller="+fmt.Sprint(callerDeadline), func(t *testing.T) {
				ctx := context.Background()
				expected := time.Now().Add(2 * time.Hour)
				if callerDeadline {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, expected)
					defer cancel()
				}
				stream := &contextServerStream{ctx: ctx}
				before := time.Now()
				err := deadlineStreamInterceptor(nil, stream, &grpc.StreamServerInfo{FullMethod: method}, func(_ any, got grpc.ServerStream) error {
					deadline, bounded := got.Context().Deadline()
					if callerDeadline {
						if !bounded || !deadline.Equal(expected) {
							t.Fatalf("caller deadline changed: %v %v", deadline, bounded)
						}
					} else if method == vmmdv1.VMMDService_OpenSystemStream_FullMethodName {
						if bounded {
							t.Fatalf("connection acquired lifetime deadline: %v", deadline)
						}
					} else {
						duration := 30 * time.Minute
						if method == vmmdv1.VMMDService_Exec_FullMethodName {
							duration = 8 * time.Hour
						}
						if !bounded || deadline.Before(before.Add(duration)) || deadline.After(time.Now().Add(duration)) {
							t.Fatalf("operation deadline changed: %v %v", deadline, bounded)
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func requireGuestRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires Guest root for fixed state paths and numeric ownership")
	}
}
