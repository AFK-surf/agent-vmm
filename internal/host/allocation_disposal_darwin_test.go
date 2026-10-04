//go:build darwin

package host

import (
	"context"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAllocationDeletionRequiresExactConnectionAndKeepsPendingReceipt(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "registration", Endpoint: "controller", ProtocolVersion: "2", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetRemoteConnection(ctx, registration.ID, "current", int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED)); err != nil {
		t.Fatal(err)
	}
	target, err := database.BeginWorkloadAdmission(ctx, store.Environment{ID: "target", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID, AllocationID: "allocation", AllocationGeneration: 7, AllocationState: 2, CPUMillis: 1000, MemoryBytes: 1 << 30, PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1}, resourcev2.Contract{CPUMaxMillis: 1000, MemoryMaxBytes: 1 << 30, PIDMax: 128, WritableQuotaBytes: 4 << 30})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database, localDisposalWake: make(chan struct{}, 1)}
	server := &remoteComputeServer{controller: controller}
	request := &hostv1.DisposeAllocationRequest{RegistrationId: registration.ID, ConnectionInstance: "current", AllocationId: target.AllocationID, ExpectedGeneration: target.AllocationGeneration, ExpectedPolicyRevision: target.PolicyRevision, RequestId: "delete-request"}
	call := func(identity string) (*hostv1.AllocationResponse, error) {
		value, err := auth.UnaryInterceptor(auth.Identity{Role: auth.RemoteConnector, RegistrationID: identity})(ctx, request, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) { return server.DisposeAllocation(ctx, request) })
		if err != nil {
			return nil, err
		}
		return value.(*hostv1.AllocationResponse), nil
	}
	if _, err := call("other"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong owner=%v", err)
	}
	request.ConnectionInstance = "stale"
	if _, err := call(registration.ID); status.Code(err) != codes.Aborted {
		t.Fatalf("stale connection=%v", err)
	}
	request.ConnectionInstance = "current"
	request.ExpectedPolicyRevision = target.PolicyRevision + 1
	if _, err := call(registration.ID); status.Code(err) != codes.Aborted || status.Convert(err).Message() != "allocation revision changed before deletion acceptance" {
		t.Fatalf("pre-accept revision=%v", err)
	}
	if _, err := database.AllocationDisposal(ctx, registration.ID, target.AllocationID, target.AllocationGeneration); err == nil {
		t.Fatal("stale revision accepted a disposal")
	}
	_, rejected := call(registration.ID)
	var metadata map[string]string
	for _, detail := range status.Convert(rejected).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "DISPOSAL_REVISION_NOT_ACCEPTED" {
			metadata = info.Metadata
		}
	}
	if metadata["allocation_id"] != target.AllocationID || metadata["generation"] != "7" || metadata["revision"] == "" {
		t.Fatalf("scope metadata=%v", metadata)
	}
	request.ExpectedPolicyRevision = target.PolicyRevision
	if _, err := call(registration.ID); status.Code(err) != codes.Unavailable {
		t.Fatalf("pending=%v", err)
	}
	root, err := database.AllocationDisposal(ctx, registration.ID, target.AllocationID, target.AllocationGeneration)
	if err != nil {
		t.Fatal(err)
	}
	request.RequestId = "another-request"
	if _, err := call(registration.ID); status.Code(err) == codes.OK {
		t.Fatal("replaced original deletion")
	}
	request.RequestId = "delete-request"
	if err := database.FinishDisposalEnvironment(ctx, root); err != nil {
		t.Fatal(err)
	}
	if _, err := call(registration.ID); status.Code(err) != codes.Unavailable {
		t.Fatalf("resource absence reported success before receipt completion: %v", err)
	}
	if err := database.FinishLocalDisposal(ctx, root); err != nil {
		t.Fatal(err)
	}
	result, err := call(registration.ID)
	if err != nil || result.GetAllocation().GetState() != hostv1.AllocationState_ALLOCATION_STATE_DISCARDED {
		t.Fatalf("completed=%+v, %v", result, err)
	}
}
