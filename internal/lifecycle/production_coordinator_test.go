//go:build darwin || linux

package lifecycle

import (
	"context"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistInstallMaterialUsesPrivateDurableWorkspace(t *testing.T) {
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", "operation-1")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	material := validTestInstallMaterial()
	planPath, err := persistOperationHandoff(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), true)
	if err != nil {
		t.Fatal(err)
	}
	if planPath == "" || !strings.HasPrefix(planPath, filepath.Join(value.applicationData, "install-operations", "operation-1")) {
		t.Fatalf("plan path escaped helper-owned operation workspace: %q", planPath)
	}
	info, err := os.Stat(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("plan mode = %o, want 600", info.Mode().Perm())
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "plan.json" {
		t.Fatalf("unexpected operation state: %v %v", entries, err)
	}
	plan, err := os.ReadFile(filepath.Join(directory, "plan.json"))
	if err != nil || strings.Contains(string(plan), `"manifest"`) {
		t.Fatalf("operation plan retained a Server manifest: %s err=%v", plan, err)
	}
	if _, err := newProductionLifecycleAdapterForResume(value, "operation-1"); err != nil {
		t.Fatalf("acknowledged plan was not resumable: %v", err)
	}
	if err := cleanupInstallPlan(value, "operation-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(planPath); !os.IsNotExist(err) {
		t.Fatalf("operation workspace was not removed: %v", err)
	}
}

// No ApplianceAdmin service or MCP bundle exists in this fixture.
func TestManagedInstallAndStatusWithoutStandaloneAppliance(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "vmm-managed-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	value := testPaths(root)
	material := validTestInstallMaterial()
	material.RemoteEnrollment.EnrollmentToken = base64.StdEncoding.EncodeToString(make([]byte, 32))
	material.RemoteEnrollment.TrustBundle = base64.StdEncoding.EncodeToString([]byte("fixture CA"))
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if _, err := persistOperationHandoff(value, directory, material, time.Now().Add(-time.Minute).UnixMilli(), true); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	operator := &managedInstallOperator{registrationID: material.RegistrationID}
	hostv1.RegisterOperatorServiceServer(server, operator)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	production, err := productionLifecycleAdapterForResume(value, material.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	adapter := lifecycleAdapterFor(production).(*helperLifecycleAdapter)
	adapter.status = func(context.Context, paths) serviceStatus {
		return serviceStatus{HostInstalled: true, HostLoaded: true, HostReadable: true, HostHealthy: true}
	}
	adapter.reconcile = func(context.Context, paths, bool) error { t.Fatal("healthy Host must not be reinstalled"); return nil }
	coordinator, err := NewLifecycleCoordinator(adapter, LifecycleCoordinatorConfig{OperationTimeout: time.Second, StepTimeout: time.Second, InitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Run(context.Background(), LifecycleInstall, LifecycleRequest{RequestID: "managed-install"}); err != nil {
		t.Fatal(err)
	}
	if err := persistCompletedInstall(value, production); err != nil {
		t.Fatal(err)
	}
	if err := cleanupInstallPlan(value, material.OperationID); err != nil {
		t.Fatal(err)
	}
	if completed, err := completedInstallExists(value, material.OperationID); err != nil || !completed {
		t.Fatalf("completion=%t err=%v", completed, err)
	}
	for _, id := range []string{"", material.RegistrationID, "other-registration"} {
		facts, err := readOperatorFactsContext(context.Background(), filepath.Join(value.applicationData, "operator.sock"), id)
		if (err != nil) != (id == "other-registration") || !facts.hostReadable || !facts.hostHealthy || facts.connectorConnected != (id == material.RegistrationID) {
			t.Fatalf("id=%q facts=%+v err=%v", id, facts, err)
		}
	}
	operator.mu.Lock()
	defer operator.mu.Unlock()
	if operator.applyCalls != 1 || operator.request.GetRegistration().GetRegistrationId() != material.RegistrationID {
		t.Fatalf("enrollment calls=%d request=%v", operator.applyCalls, operator.request)
	}
}

type managedInstallOperator struct {
	hostv1.UnimplementedOperatorServiceServer
	mu             sync.Mutex
	registrationID string
	applyCalls     int
	request        *hostv1.ApplyManagedRemoteEnrollmentRequest
}

func (server *managedInstallOperator) GetVM(context.Context, *hostv1.GetVMRequest) (*hostv1.VMResponse, error) {
	return &hostv1.VMResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING}}, nil
}
func (server *managedInstallOperator) GetSystemSnapshot(context.Context, *hostv1.GetSystemSnapshotRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	return nil, status.Error(codes.Internal, "rich snapshot must not be used by managed lifecycle")
}
func (server *managedInstallOperator) GetSystemHealthSummary(context.Context, *hostv1.GetSystemHealthSummaryRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	return &hostv1.GetSystemSnapshotResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING}, Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}}, nil
}
func (server *managedInstallOperator) ApplyManagedRemoteEnrollment(_ context.Context, request *hostv1.ApplyManagedRemoteEnrollmentRequest) (*hostv1.RemoteRegistrationResponse, error) {
	server.mu.Lock()
	server.applyCalls++
	server.request = request
	server.mu.Unlock()
	return &hostv1.RemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: request.GetRegistration().GetRegistrationId()}}, nil
}
func (server *managedInstallOperator) GetRemoteRegistration(_ context.Context, request *hostv1.GetRemoteRegistrationRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if request.GetRegistrationId() == "other-registration" {
		return nil, status.Error(codes.NotFound, "registration not found")
	}
	state := hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED
	server.mu.Lock()
	if request.GetRegistrationId() == server.registrationID && server.applyCalls > 0 {
		state = hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED
	}
	server.mu.Unlock()
	return &hostv1.RemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: request.GetRegistrationId(), EnrollmentState: hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY, ConnectionState: state}}, nil
}
