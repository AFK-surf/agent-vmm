//go:build darwin || linux

package lifecycle

import (
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// No ApplianceAdmin service or MCP bundle exists in this fixture.
func TestRegistrationClaimCompletesBeforeControlConnection(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "vmm-managed-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	value := testPaths(root)
	material := validTestInstallMaterial()
	material.RemoteConnection.TrustBundle = base64.StdEncoding.EncodeToString([]byte("fixture CA"))
	plan := testClaimPlan("managed-install", "https://api.example.test")
	plan.Material = &material
	if err := persistClaimPlan(value, plan); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	operator := &managedInstallOperator{registrationID: material.RegistrationID, keepDisconnected: true}
	hostv1.RegisterOperatorServiceServer(server, operator)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	production, err := productionRegistrationAdapterForResume(value, "managed-install")
	if err != nil {
		t.Fatal(err)
	}
	adapter := lifecycleAdapterFor(production).(*helperLifecycleAdapter)
	adapter.status = func(context.Context, paths) serviceStatus {
		return serviceStatus{HostInstalled: true, HostLoaded: true, HostReadable: true, HostHealthy: true}
	}
	adapter.reconcile = func(context.Context, paths, bool) error { t.Fatal("healthy Host must not be reinstalled"); return nil }
	if err := runRegistrationLifecycle(adapter, "registration-claim", LifecycleRequest{RequestID: "managed-install"}); err != nil {
		t.Fatalf("applied Registration failed before its asynchronous connection: %v", err)
	}
	facts, err := readOperatorFactsContext(context.Background(), filepath.Join(value.applicationData, "operator.sock"), material.RegistrationID)
	if err != nil || !facts.hostHealthy || facts.connectorConnected {
		t.Fatalf("claim completion changed disconnected observation: %+v %v", facts, err)
	}
	operator.mu.Lock()
	operator.keepDisconnected = false
	operator.mu.Unlock()
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
	mu               sync.Mutex
	registrationID   string
	applyCalls       int
	keepDisconnected bool
	vmStartErr       error
	applyErr         error
	request          *hostv1.ApplyRemoteRegistrationRequest
}

func (server *managedInstallOperator) GetVM(context.Context, *hostv1.GetVMRequest) (*hostv1.GetVMResponse, error) {
	if server.vmStartErr != nil {
		return &hostv1.GetVMResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}}, nil
	}
	return &hostv1.GetVMResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING}}, nil
}
func (server *managedInstallOperator) GetVMImpactSummary(context.Context, *hostv1.GetVMImpactSummaryRequest) (*hostv1.GetVMImpactSummaryResponse, error) {
	return &hostv1.GetVMImpactSummaryResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}}, nil
}
func (server *managedInstallOperator) StartVM(context.Context, *hostv1.StartVMRequest) (*hostv1.StartVMResponse, error) {
	return nil, server.vmStartErr
}
func (server *managedInstallOperator) GetSystemSnapshot(context.Context, *hostv1.GetSystemSnapshotRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	return nil, status.Error(codes.Internal, "rich snapshot must not be used by managed lifecycle")
}
func (server *managedInstallOperator) GetSystemHealthSummary(context.Context, *hostv1.GetSystemHealthSummaryRequest) (*hostv1.GetSystemHealthSummaryResponse, error) {
	return &hostv1.GetSystemHealthSummaryResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING}, Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}}, nil
}
func (server *managedInstallOperator) ApplyRemoteRegistration(_ context.Context, request *hostv1.ApplyRemoteRegistrationRequest) (*hostv1.ApplyRemoteRegistrationResponse, error) {
	server.mu.Lock()
	server.applyCalls++
	server.request = request
	server.mu.Unlock()
	if server.applyErr != nil {
		return nil, server.applyErr
	}
	return &hostv1.ApplyRemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: request.GetRegistration().GetRegistrationId()}}, nil
}

func TestRegistrationClaimPreservesInitAndApplyFailures(t *testing.T) {
	for _, stage := range []string{"init", "register"} {
		t.Run(stage, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "vmm-claim-failure-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(root) })
			value := testPaths(root)
			plan := testClaimPlan("claim-failure", "https://api.example.test")
			material := validTestInstallMaterial()
			plan.Material = &material
			if err := persistClaimPlan(value, plan); err != nil {
				t.Fatal(err)
			}
			operator := &managedInstallOperator{registrationID: material.RegistrationID, keepDisconnected: true}
			refusal := status.Error(codes.FailedPrecondition, "fixture mutation refused")
			wantCalls := 1
			if stage == "init" {
				operator.vmStartErr = refusal
				wantCalls = 0
			} else {
				operator.applyErr = refusal
			}
			listener, err := net.Listen("unix", filepath.Join(value.applicationData, "operator.sock"))
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			hostv1.RegisterOperatorServiceServer(server, operator)
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			production, err := productionRegistrationAdapterForResume(value, plan.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			adapter := lifecycleAdapterFor(production).(*helperLifecycleAdapter)
			adapter.status = func(context.Context, paths) serviceStatus {
				return serviceStatus{HostInstalled: true, HostLoaded: true, HostReadable: true, HostHealthy: true}
			}
			err = runRegistrationLifecycle(adapter, "registration-claim", LifecycleRequest{RequestID: plan.RequestID})
			if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "lifecycle "+stage+":") {
				t.Fatalf("claim did not preserve %s refusal: %v", stage, err)
			}
			operator.mu.Lock()
			defer operator.mu.Unlock()
			if operator.applyCalls != wantCalls {
				t.Fatalf("Apply calls=%d want %d", operator.applyCalls, wantCalls)
			}
		})
	}
}
func (server *managedInstallOperator) GetRemoteRegistration(_ context.Context, request *hostv1.GetRemoteRegistrationRequest) (*hostv1.GetRemoteRegistrationResponse, error) {
	if request.GetRegistrationId() == "other-registration" {
		return nil, status.Error(codes.NotFound, "registration not found")
	}
	state := hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED
	server.mu.Lock()
	if request.GetRegistrationId() == server.registrationID && server.applyCalls > 0 && !server.keepDisconnected {
		state = hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED
	}
	server.mu.Unlock()
	return &hostv1.GetRemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: request.GetRegistrationId(), State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING, EnrollmentState: hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY, ConnectionState: state}}, nil
}

func TestCredentialReplacementCompletesForStoppedDisconnectedRegistration(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "vmm-recover-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	value := testPaths(root)
	material := validTestInstallMaterial()
	plan := testClaimPlan("recover", "https://api.example.test")
	plan.RecoveryURL = "https://api.example.test/v1/comma/workspaces/workspace/compute-nodes/agent-vmm/registrations/registration-1/recovery"
	plan.Material = &material
	if err := persistClaimPlan(value, plan); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	operator := &managedInstallOperator{registrationID: material.RegistrationID, keepDisconnected: true}
	hostv1.RegisterOperatorServiceServer(server, operator)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	production, err := productionRegistrationAdapterForResume(value, "recover")
	if err != nil {
		t.Fatal(err)
	}
	adapter := lifecycleAdapterFor(production).(*helperLifecycleAdapter)
	adapter.status = func(context.Context, paths) serviceStatus {
		return serviceStatus{HostInstalled: true, HostLoaded: true, HostReadable: true, HostHealthy: true}
	}
	adapter.reconcile = func(context.Context, paths, bool) error {
		t.Fatal("credential replacement reinstalled software")
		return nil
	}
	if err := runRegistrationLifecycle(adapter, "registration-recover", LifecycleRequest{RequestID: "recover"}); err != nil {
		t.Fatalf("stopped registration could not replace credential: %v", err)
	}
	remote, err := hostv1.NewOperatorServiceClient(mustOperatorConnection(t, filepath.Join(value.applicationData, "operator.sock"))).GetRemoteRegistration(context.Background(), &hostv1.GetRemoteRegistrationRequest{RegistrationId: material.RegistrationID})
	if err != nil || remote.GetRegistration().GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING || remote.GetRegistration().GetConnectionState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED {
		t.Fatalf("replacement changed stopped connection: %+v %v", remote, err)
	}
	operator.mu.Lock()
	defer operator.mu.Unlock()
	if operator.applyCalls != 1 || !bytes.Equal(operator.request.GetCredential(), plan.CandidateCredential) {
		t.Fatal("replacement did not apply the saved candidate exactly once")
	}
}
func mustOperatorConnection(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	connection, err := operatorConnection(context.Background(), socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return connection
}
