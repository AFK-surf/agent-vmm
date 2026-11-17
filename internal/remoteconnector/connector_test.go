package remoteconnector

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type memoryCredentials struct {
	mu           sync.Mutex
	value        []byte
	replacements [][]byte
	deleted      bool
}

func (store *memoryCredentials) Load(context.Context, string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]byte(nil), store.value...), nil
}
func (store *memoryCredentials) Replace(_ context.Context, _ string, value []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.value = append(store.value[:0], value...)
	store.replacements = append(store.replacements, append([]byte(nil), value...))
	return nil
}
func (store *memoryCredentials) Delete(context.Context, string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	wipe(store.value)
	store.value = nil
	store.deleted = true
	return nil
}

type fakeLocal struct {
	connectorv1.UnimplementedRemoteComputeServiceServer
	mu                    sync.Mutex
	registration          *hostv1.RemoteRegistration
	states                []hostv1.RemoteConnectionState
	instance              string
	ensureCalls           int
	connectedRegistration *hostv1.RemoteRegistration
	admissionRegistration *hostv1.RemoteRegistration
	reports               []*hostv1.ConnectorStatus
}

func (server *fakeLocal) GetRemoteState(context.Context, *connectorv1.GetRemoteStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	return &connectorv1.GetRemoteStateResponse{Registration: server.registration}, nil
}
func (server *fakeLocal) SetConnectionState(_ context.Context, request *connectorv1.SetConnectionStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if request.GetState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED && server.instance != request.GetConnectionInstance() {
		return nil, status.Error(codes.Aborted, "stale")
	}
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
		server.instance = request.GetConnectionInstance()
	}
	server.states = append(server.states, request.GetState())
	registration := server.registration
	if request.GetState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED && server.connectedRegistration != nil {
		registration = server.connectedRegistration
	}
	return &connectorv1.GetRemoteStateResponse{Registration: registration}, nil
}
func (server *fakeLocal) ReportConnectorStatus(_ context.Context, request *connectorv1.ReportConnectorStatusRequest) (*connectorv1.GetRemoteStateResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if request.GetConnectionInstance() != server.instance {
		return nil, status.Error(codes.Aborted, "stale")
	}
	server.reports = append(server.reports, request.GetStatus())
	return &connectorv1.GetRemoteStateResponse{Registration: server.registration}, nil
}
func (server *fakeLocal) SetRemoteAdmissionState(_ context.Context, request *connectorv1.SetRemoteAdmissionStateRequest) (*connectorv1.GetRemoteStateResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if request.GetConnectionInstance() != server.instance {
		return nil, status.Error(codes.Aborted, "stale")
	}
	registration := server.registration
	if server.admissionRegistration != nil {
		registration = server.admissionRegistration
	} else if server.connectedRegistration != nil {
		registration = server.connectedRegistration
	}
	if request.GetExpectedInventoryRevision() != 0 && registration.GetInventoryRevision() != request.GetExpectedInventoryRevision() {
		return nil, status.Error(codes.Aborted, "inventory changed")
	}
	if request.GetExpectedInventoryRevision() != 0 && registration.GetState() != request.GetState() {
		return nil, status.Error(codes.Aborted, "admission state changed")
	}
	if request.GetExpectedPoolPolicyRevision() != 0 && registration.GetPoolPolicyRevision() != request.GetExpectedPoolPolicyRevision() {
		return nil, status.Error(codes.Aborted, "pool policy changed")
	}
	return &connectorv1.GetRemoteStateResponse{Registration: registration}, nil
}
func (server *fakeLocal) EnsureAllocation(_ context.Context, request *connectorv1.EnsureAllocationRequest) (*connectorv1.AllocationResponse, error) {
	server.mu.Lock()
	server.ensureCalls++
	server.mu.Unlock()
	return &connectorv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), DisplayLabel: request.GetDisplayLabel(), Generation: request.GetGeneration(), Limits: request.GetLimits(), EgressMode: request.GetEgressMode(), State: hostv1.AllocationState_ALLOCATION_STATE_READY}}, nil
}

type fakeController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	t           *testing.T
	hello       *remotev1.HostHello
	key         *ecdsa.PrivateKey
	reject      codes.Code
	rejectError error
}

type fakeEnrollmentTrust struct {
	connectorv1.UnimplementedTrustServiceServer
	identity  *trustv1.DeviceIdentity
	installed bool
	reject    string
}

func (server *fakeEnrollmentTrust) GetDeviceIdentity(context.Context, *trustv1.GetDeviceIdentityRequest) (*trustv1.DeviceIdentity, error) {
	return server.identity, nil
}

func (server *fakeEnrollmentTrust) SignEndpointObservation(_ context.Context, request *trustv1.SignEndpointObservationRequest) (*trustv1.EndpointObservation, error) {
	observation := proto.Clone(request.GetUnsignedObservation()).(*trustv1.EndpointObservation)
	observation.DeviceSignature = make([]byte, 64)
	return observation, nil
}

func (server *fakeEnrollmentTrust) InstallManagedTrust(_ context.Context, request *trustv1.InstallManagedTrustRequest) (*trustv1.InstallManagedTrustResponse, error) {
	if server.reject != "" {
		return nil, status.Error(codes.PermissionDenied, server.reject)
	}
	if request.GetMembershipCredential().GetSubjectDeviceId() != server.identity.GetDeviceId() {
		return nil, status.Error(codes.PermissionDenied, "wrong subject")
	}
	server.installed = true
	return &trustv1.InstallManagedTrustResponse{AuthorityId: request.GetTrustAnchor().GetAuthority().GetAuthorityId(), AnchorRevision: request.GetTrustAnchor().GetRevision(), PolicyRevision: request.GetMembershipCredential().GetPolicyRevision()}, nil
}

func configureEnrollmentTrust(t *testing.T, config *Config) *fakeEnrollmentTrust {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "avm-trust-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "trust.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	root, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	implementation := &fakeEnrollmentTrust{identity: &trustv1.DeviceIdentity{DeviceId: "device-a", RootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), root.X, root.Y), RootKeyRevision: 1, SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}}
	connectorv1.RegisterTrustServiceServer(server, implementation)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	external := config.external()
	external.Trust = &TrustBoundary{Socket: socket}
	config.Transport = external
	return implementation
}
func (server *fakeController) Connect(stream grpc.BidiStreamingServer[remotev1.ConnectRequest, remotev1.ConnectResponse]) error {
	if server.rejectError != nil {
		return server.rejectError
	}
	if server.reject != codes.OK {
		return status.Error(server.reject, "rejected")
	}
	values := metadata.ValueFromIncomingContext(stream.Context(), "authorization")
	if len(values) != 1 || values[0] != "Bearer Y3JlZGVudGlhbC1vbmU=" {
		return status.Error(codes.Unauthenticated, "rejected")
	}
	session, err := controllerremote.AcceptControl(stream, "registration-a")
	if err != nil {
		return err
	}
	server.hello = session.Hello()
	if err := session.Ready(); err != nil {
		return err
	}
	command := &remotev1.ControllerCommand{CommandId: "ensure-1", Sequence: 1, ConnectionEpoch: session.Hello().GetConnectionEpoch(), TargetRevision: 7, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b", Generation: 1, DisplayLabel: "remote", Limits: &remotev1.ResourceLimits{Pids: proto.Uint32(64), DiskBytes: proto.Uint64(256 << 20)}, EgressMode: remotev1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: 7}}}
	commandContext, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
	defer cancel()
	transcript, err := session.Dispatch(commandContext, command)
	if err != nil || transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED || transcript.Result.GetAllocation().GetAllocationId() != "allocation-b" {
		return status.Error(codes.Internal, "bad result")
	}
	return status.Error(codes.Unavailable, "controller disconnected")
}

func TestClaimedCredentialControlTranscript(t *testing.T) {
	local := &fakeLocal{registration: testRegistration()}
	socket := startLocalServer(t, local)
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	store := &memoryCredentials{value: []byte("credential-one")}
	config := Config{RegistrationID: "registration-a", RemoteComputeSocket: socket, Transport: ExternalTransport{Endpoint: endpoint, Credentials: store, Roots: roots}}
	ready := make(chan struct{})
	admission := make(chan struct{})
	config.OnReady = func() { close(ready) }
	config.CommandAdmission = admission
	configureEnrollmentTrust(t, &config)
	connected := make(chan error, 1)
	go func() { connected <- runOneAttempt(context.Background(), config) }()
	select {
	case <-ready:
	case err := <-connected:
		t.Fatalf("connector stopped before ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("connector did not reach the ready admission barrier")
	}
	local.mu.Lock()
	ensureCalls := local.ensureCalls
	local.mu.Unlock()
	if ensureCalls != 0 {
		t.Fatal("controller command ran before admission")
	}
	close(admission)
	if err := <-connected; err == nil {
		t.Fatalf("connectOnce error = %v", err)
	}
	if controller.hello.GetRegistrationId() != "registration-a" || controller.hello.GetPoolRevision() != 7 || len(controller.hello.GetInventory()) != 1 {
		t.Fatalf("unexpected hello: %+v", controller.hello)
	}
	if directory := os.Getenv("AGENT_VMM_CONTRACT_OUTPUT_DIR"); directory != "" {
		data, err := protojson.Marshal(controller.hello)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "hello.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.replacements) != 0 || string(store.value) != "credential-one" || store.deleted {
		t.Fatal("connection changed the credential")
	}

	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.states) != 2 || local.states[0] != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED || local.states[1] != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED {
		t.Fatalf("connection states = %v", local.states)
	}
}

func TestConnectionFenceRejectsInventoryMutationBeforeAdmission(t *testing.T) {
	initial := testRegistration()
	updated := proto.Clone(initial).(*hostv1.RemoteRegistration)
	updated.InventoryRevision++
	local := &fakeLocal{registration: initial, connectedRegistration: updated}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{
		RegistrationID:      "registration-a",
		RemoteComputeSocket: startLocalServer(t, local), Transport: ExternalTransport{Endpoint: endpoint, Credentials: &memoryCredentials{value: []byte("credential-one")}, Roots: roots},
	}
	ready := make(chan struct{})
	config.OnReady = func() { close(ready) }
	err := runOneAttempt(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "changed registration snapshot") {
		t.Fatalf("connectOnce error = %v", err)
	}
	select {
	case <-ready:
		t.Fatal("connector admitted before inventory settlement")
	default:
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.states) != 2 || local.states[1] != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED {
		t.Fatalf("connection states = %v", local.states)
	}
}

func TestAdmissionFenceRejectsInventoryMutationAfterConnectionFence(t *testing.T) {
	initial := testRegistration()
	updated := proto.Clone(initial).(*hostv1.RemoteRegistration)
	updated.InventoryRevision++
	local := &fakeLocal{registration: initial, admissionRegistration: updated}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{
		RegistrationID:      "registration-a",
		RemoteComputeSocket: startLocalServer(t, local), Transport: ExternalTransport{Endpoint: endpoint, Credentials: &memoryCredentials{value: []byte("credential-one")}, Roots: roots},
	}
	ready := make(chan struct{})
	config.OnReady = func() { close(ready) }
	err := runOneAttempt(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "local admission fence failed") {
		t.Fatalf("connectOnce error = %v", err)
	}
	select {
	case <-ready:
		t.Fatal("connector admitted after the inventory changed")
	default:
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.states) != 2 || local.states[1] != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED {
		t.Fatalf("connection states = %v", local.states)
	}
}

func TestAdmissionFenceDoesNotReopenOperatorDrain(t *testing.T) {
	initial := testRegistration()
	draining := proto.Clone(initial).(*hostv1.RemoteRegistration)
	draining.State = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING
	local := &fakeLocal{registration: initial, admissionRegistration: draining}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{
		RegistrationID:      "registration-a",
		RemoteComputeSocket: startLocalServer(t, local), Transport: ExternalTransport{Endpoint: endpoint, Credentials: &memoryCredentials{value: []byte("credential-one")}, Roots: roots},
	}
	ready := make(chan struct{})
	config.OnReady = func() { close(ready) }
	err := runOneAttempt(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "local admission fence failed") {
		t.Fatalf("connectOnce error = %v", err)
	}
	select {
	case <-ready:
		t.Fatal("connector reopened operator drain")
	default:
	}
}

func TestTLSHostnameMismatch(t *testing.T) {
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "different.test", controller)
	config := Config{RegistrationID: "registration-a", RemoteComputeSocket: "/tmp/not-used.sock", Transport: ExternalTransport{Endpoint: endpoint, Credentials: &memoryCredentials{}, Roots: roots}}
	configureEnrollmentTrust(t, &config)
	if err := runOneAttempt(context.Background(), config); err == nil {
		t.Fatal("TLS hostname mismatch succeeded")
	}
}

func TestBearerAuthenticationFailure(t *testing.T) {
	local := &fakeLocal{registration: testRegistration()}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{RegistrationID: "registration-a", RemoteComputeSocket: startLocalServer(t, local), Transport: ExternalTransport{Endpoint: endpoint, Credentials: &memoryCredentials{value: []byte("wrong")}, Roots: roots}}
	if err := runOneAttempt(context.Background(), config); err == nil {
		t.Fatal("invalid bearer credential succeeded")
	}
}

type unavailableCredentials struct{ memoryCredentials }

func (*unavailableCredentials) Load(context.Context, string) ([]byte, error) {
	return nil, ErrCredentialUnavailable
}

func TestRunRetriesAuthenticationAndConfigurationAndStopsOnlyOnContextCancellation(t *testing.T) {
	tests := []struct {
		name        string
		reject      codes.Code
		rejectError error
		store       CredentialStore
		state       hostv1.RemoteRegistrationState
		worker      bool
		phase       hostv1.ConnectorPhase
		code        string
	}{
		{name: "unauthenticated", reject: codes.Unauthenticated, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CONTROLLER_AUTH_FAILED"},
		{name: "permission denied", reject: codes.PermissionDenied, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CONTROLLER_AUTH_FAILED"},
		{name: "credential unavailable", store: &unavailableCredentials{}, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR, code: "CREDENTIAL_UNAVAILABLE"},
		{name: "revoked", state: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CONTROLLER_AUTH_FAILED"},
		{name: "data worker unavailable", worker: true, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, code: "DATA_WORKER_UNAVAILABLE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			registration := testRegistration()
			if test.state != 0 {
				registration.State = test.state
			}
			local := &fakeLocal{registration: registration}
			endpoint, roots := startTLSServer(t, "localhost", &fakeController{t: t, reject: test.reject})
			store := test.store
			if store == nil {
				store = &memoryCredentials{value: []byte("credential-one")}
			}
			transport := ExternalTransport{Endpoint: endpoint, Credentials: store, Roots: roots}
			if test.worker {
				directory := t.TempDir()
				transport.Worker = &WorkerSockets{Socket: filepath.Join(directory, "worker.sock"), ServiceTunnelSocket: filepath.Join(directory, "service.sock"), MeshControlSocket: filepath.Join(directory, "mesh.sock")}
			}
			config := Config{RegistrationID: "registration-a", RemoteComputeSocket: startLocalServer(t, local), Transport: transport}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Run(ctx, config) }()
			// A retrying connector reports the failure twice with a next retry
			// time: once for the first attempt and once after its backoff.
			deadline := time.Now().Add(5 * time.Second)
			for {
				local.mu.Lock()
				var failures []*hostv1.ConnectorStatus
				for _, report := range local.reports {
					if report.GetPhase() == test.phase && report.GetFailureCode() == test.code {
						failures = append(failures, report)
					}
				}
				local.mu.Unlock()
				if len(failures) >= 2 {
					if failures[0].GetNextRetryAt() == nil || failures[1].GetReconnectAttempt() != 2 {
						t.Fatalf("retry reports = %v", failures)
					}
					break
				}
				select {
				case err := <-done:
					t.Fatalf("connector stopped on a retryable failure: %v", err)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatalf("connector did not retry; reports = %v", local.reports)
				}
				time.Sleep(20 * time.Millisecond)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("cancelled Run error = %v", err)
			}
		})
	}
}

func TestConnectorHeartbeatStopsRevokedRegistration(t *testing.T) {
	local := &fakeLocal{instance: "connection", registration: testRegistration()}
	local.registration.EnrollmentState = hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING
	socket := startLocalServer(t, local)
	connection, err := newLocalClient(socket, "registration-a")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.connection.Close()
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	cancelled := make(chan struct{})
	done := make(chan struct{})
	connectorHeartbeat(ctx, connection.client, "registration-a", "connection", 10*time.Millisecond, func(error) { close(cancelled) }, func() *hostv1.ControllerDomainMetrics { return nil }, done)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("revoked registration did not stop the connector")
	}
}

func TestBoundsAndBackoff(t *testing.T) {
	for _, delay := range []time.Duration{time.Second, 4 * time.Second, MaxReconnectDelay} {
		value := jitter(delay)
		if value < delay/2 || value > delay {
			t.Fatalf("jitter(%s) = %s", delay, value)
		}
	}
	result := commandHandler{logger: slog.Default()}.handle(context.Background(), &remotev1.ControllerCommand{CommandId: string(make([]byte, 129))})
	if result.GetReason() != remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT {
		t.Fatalf("reason = %s", result.GetReason())
	}
}

func TestConnectorFailureStatusIsBoundedAndActionable(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		phase hostv1.ConnectorPhase
		code  string
	}{
		{name: "retry", err: errors.New("secret endpoint detail"), phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, code: "CONNECTION_FAILED"},
		{name: "authentication", err: status.Error(codes.Unauthenticated, "credential contents"), phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CONTROLLER_AUTH_FAILED"},
		{name: "configuration", err: errConnectorConfiguration, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR, code: "CONNECTOR_CONFIG_INVALID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			phase, code, message := connectorFailure(test.err)
			if phase != test.phase || code != test.code || strings.Contains(message, "secret") || strings.Contains(message, "credential contents") {
				t.Fatalf("status=%s/%s/%q", phase, code, message)
			}
		})
	}
}

func testRegistration() *hostv1.RemoteRegistration {
	return &hostv1.RemoteRegistration{Id: "registration-a", Revision: 7, PoolPolicyRevision: 7, InventoryRevision: 11, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, Pool: &hostv1.RemotePoolPolicy{PerEnvironmentLimits: &hostv1.ResourceLimits{}}, Allocations: []*hostv1.RemoteAllocation{{AllocationId: "allocation-a", State: hostv1.AllocationState_ALLOCATION_STATE_READY}}}
}

func TestStoredCredentialBindsBearerToRegistrationMetadata(t *testing.T) {
	metadata, err := (storedCredential{store: &memoryCredentials{value: []byte("0123456789abcdef")}, account: "registration-a"}).GetRequestMetadata(context.Background())
	if err != nil || metadata["x-agent-vmm-registration"] != "registration-a" || metadata["authorization"] == "" {
		t.Fatalf("metadata=%v err=%v", metadata, err)
	}
}

func startLocalServer(t *testing.T, service connectorv1.RemoteComputeServiceServer) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "agent-vmm-connect-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "remote.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	connectorv1.RegisterRemoteComputeServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	return path
}

func startTLSServer(t *testing.T, name string, service remotev1.RemoteControllerServiceServer) (string, *x509.CertPool) {
	t.Helper()
	socket, roots := startTLSSocketServer(t, name, service)
	_, port, _ := net.SplitHostPort(socket)
	return net.JoinHostPort("localhost", port), roots
}

func startTLSSocketServer(t *testing.T, name string, service remotev1.RemoteControllerServiceServer) (string, *x509.CertPool) {
	t.Helper()
	certificate, roots := certificateFor(t, name)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&certificate)))
	remotev1.RegisterRemoteControllerServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	return listener.Addr().String(), roots
}

func certificateFor(t *testing.T, name string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return certificate, roots
}

type unavailableAllocationClient struct {
	connectorv1.RemoteComputeServiceClient
}

func (unavailableAllocationClient) EnsureAllocation(context.Context, *connectorv1.EnsureAllocationRequest, ...grpc.CallOption) (*connectorv1.AllocationResponse, error) {
	return nil, status.Error(codes.Unavailable, "local remote-compute socket connection refused")
}

func TestAllocationFailureRetainsRPCMessage(t *testing.T) {
	handler := commandHandler{logger: slog.Default(), client: unavailableAllocationClient{}}
	result := handler.handle(context.Background(), &remotev1.ControllerCommand{CommandId: "failed-ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation"}}})
	if result.GetReason() != remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE || !strings.Contains(result.GetMessage(), "local remote-compute socket connection refused") {
		t.Fatalf("allocation failure lost RPC context: %v", result)
	}
}

func TestRunnerDeadlineSurvivesLocalGRPC(t *testing.T) {
	local := &fakeLocal{registration: testRegistration()}
	connection, err := newLocalClient(startLocalServer(t, local), "registration-a")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.connection.Close()
	for _, deadline := range []int64{time.Now().Add(time.Minute).UnixMilli(), 9223372036854775807} {
		t.Run(strconv.FormatInt(deadline, 10), func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), time.UnixMilli(deadline))
			defer cancel()
			if err := ctx.Err(); err != nil {
				t.Fatalf("deadline context expired: %v", err)
			}
			handler := commandHandler{logger: slog.Default(), client: connection.client, registrationID: "registration-a"}
			result := handler.handle(ctx, &remotev1.ControllerCommand{CommandId: "deadline-ensure", TimeoutMillis: 60000, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b"}}})
			if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
				t.Fatalf("real local gRPC rejected deadline: %v", result)
			}
		})
	}
}

type rejectedAllocationLocal struct {
	fakeLocal
	failure error
}

func (server *rejectedAllocationLocal) EnsureAllocation(context.Context, *connectorv1.EnsureAllocationRequest) (*connectorv1.AllocationResponse, error) {
	return nil, server.failure
}

func TestUnknownAllocationRetainsPartialAdmissionAcrossLocalGRPC(t *testing.T) {
	for _, dimension := range []string{"storage_headroom", "import_slot"} {
		t.Run(dimension, func(t *testing.T) {
			allocation := &hostv1.RemoteAllocation{AllocationId: "allocation-b", Generation: 2, Revision: 7, State: hostv1.AllocationState_ALLOCATION_STATE_DRAINING}
			failure, err := status.New(codes.Unavailable, "guest projection rejected").WithDetails(
				&errdetails.ErrorInfo{Domain: hostv1.ErrorDomain, Reason: "RESOURCE_CAPACITY_EXHAUSTED", Metadata: map[string]string{"resource_dimension": dimension}}, allocation)
			if err != nil {
				t.Fatal(err)
			}
			local := &rejectedAllocationLocal{failure: failure.Err()}
			connection, err := newLocalClient(startLocalServer(t, local), "registration-a")
			if err != nil {
				t.Fatal(err)
			}
			defer connection.connection.Close()
			handler := commandHandler{logger: slog.Default(), client: connection.client, registrationID: "registration-a"}
			result := handler.handle(context.Background(), &remotev1.ControllerCommand{CommandId: "partial-ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b", Generation: 2}}})
			if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN || result.GetReason() != remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE {
				t.Fatalf("rejection changed: %v", result)
			}
			if got := result.GetAllocation(); got.GetAllocationId() != "allocation-b" || got.GetGeneration() != 2 || got.GetRevision() != 7 || got.GetState() != remotev1.AllocationState_ALLOCATION_STATE_DRAINING {
				t.Fatalf("partial admission lost: %v", result)
			}
			if result.GetCapacityDimension() != capacityDimensionFor(failure.Err()) {
				t.Fatalf("capacity dimension lost: %v", result)
			}
		})
	}
}

func TestReconnectBackoffResetsOnlyAfterAStableConnection(t *testing.T) {
	var backoff reconnectBackoff
	var waits []time.Duration
	for range 7 {
		// Each attempt connects and then fails at once.
		waits = append(waits, backoff.next(true, time.Second))
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, MaxReconnectDelay, MaxReconnectDelay}
	if !slices.Equal(waits, want) {
		t.Fatalf("connect-then-fail waits = %v, want %v", waits, want)
	}
	if wait := backoff.next(true, MaxReconnectDelay); wait != time.Second {
		t.Fatalf("wait after a stable connection = %s, want 1s", wait)
	}
}

// helloController records each hello and keeps the stream open until the
// connector ends it.
type helloController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	hellos chan *remotev1.HostHello
}

func (server *helloController) Connect(stream grpc.BidiStreamingServer[remotev1.ConnectRequest, remotev1.ConnectResponse]) error {
	session, err := controllerremote.AcceptControl(stream, "registration-a")
	if err != nil {
		return err
	}
	if err := session.Ready(); err != nil {
		return err
	}
	server.hellos <- session.Hello()
	<-stream.Context().Done()
	return status.Error(codes.Unavailable, "controller disconnected")
}

// startStatusWorker serves the data worker status command with the current
// endpoint identity.
func startStatusWorker(t *testing.T, endpoint *atomic.Pointer[dataworker.Status]) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "avm-worker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "worker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = json.NewEncoder(connection).Encode(endpoint.Load())
			_ = connection.Close()
		}
	}()
	return socket
}

func TestConnectorReconnectsWithObservationOfReplacedDataWorker(t *testing.T) {
	var endpoint atomic.Pointer[dataworker.Status]
	endpoint.Store(&dataworker.Status{EndpointID: "endpoint-a", EndpointGeneration: 1, Accepting: true})
	workerSocket := startStatusWorker(t, &endpoint)
	controller := &helloController{hellos: make(chan *remotev1.HostHello, 4)}
	address, roots := startTLSServer(t, "localhost", controller)
	transport := ExternalTransport{Endpoint: address, Credentials: &memoryCredentials{value: []byte("credential-one")}, Roots: roots,
		Worker: &WorkerSockets{Socket: workerSocket, ServiceTunnelSocket: workerSocket, MeshControlSocket: workerSocket}}
	registration := testRegistration()
	registration.EnrollmentState = hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY
	config := Config{RegistrationID: "registration-a", RemoteComputeSocket: startLocalServer(t, &fakeLocal{registration: registration}), Transport: transport}
	configureEnrollmentTrust(t, &config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, config) }()
	next := func() *trustv1.EndpointObservation {
		t.Helper()
		select {
		case hello := <-controller.hellos:
			return hello.GetEndpointObservation()
		case <-time.After(15 * time.Second):
			t.Fatal("connector did not reconnect")
			return nil
		}
	}
	if observation := next(); string(observation.GetEndpointNodeId()) != "endpoint-a" || observation.GetEndpointGeneration() != 1 {
		t.Fatalf("first observation = %v", observation)
	}
	// The Host replaced the worker while the controller stream stays open. The
	// sent observation is still unexpired but names an endpoint that no longer
	// exists, so the connector must reconnect well before renewal.
	endpoint.Store(&dataworker.Status{EndpointID: "endpoint-b", EndpointGeneration: 2, Accepting: true})
	for {
		observation := next()
		if string(observation.GetEndpointNodeId()) == "endpoint-b" && observation.GetEndpointGeneration() == 2 {
			break
		}
		if string(observation.GetEndpointNodeId()) != "endpoint-a" {
			t.Fatalf("unexpected observation = %v", observation)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func runOneAttempt(parent context.Context, config Config) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var failure error
	config.OnAttemptError = func(err error) { failure = err; cancel() }
	if err := Run(ctx, config); err != nil {
		return err
	}
	return failure
}

func TestRunProtocolRefusalUsesCappedBackoffAndPreservesRegistration(t *testing.T) {
	failure, err := status.New(codes.FailedPrecondition, "unsupported Host protocol").WithDetails(&errdetails.ErrorInfo{
		Domain: hostv1.ErrorDomain, Reason: "REMOTE_PROTOCOL_UNSUPPORTED", Metadata: map[string]string{"host_protocol": "4", "min_protocol": "5", "max_protocol": "5", "host_side": "too_old"},
	})
	if err != nil {
		t.Fatal(err)
	}
	local := &fakeLocal{registration: testRegistration()}
	endpoint, roots := startTLSServer(t, "localhost", &fakeController{t: t, rejectError: failure.Err()})
	credentials := &memoryCredentials{value: []byte("credential-one")}
	config := Config{RegistrationID: "registration-a", RemoteComputeSocket: startLocalServer(t, local), Transport: ExternalTransport{Endpoint: endpoint, Credentials: credentials, Roots: roots}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, config) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		local.mu.Lock()
		var refusal *hostv1.ConnectorStatus
		for _, report := range local.reports {
			if report.GetFailureCode() == "REMOTE_PROTOCOL_UNSUPPORTED" {
				refusal = proto.Clone(report).(*hostv1.ConnectorStatus)
			}
		}
		states, ensures := len(local.states), local.ensureCalls
		local.mu.Unlock()
		if refusal != nil {
			delay := refusal.GetNextRetryAt().AsTime().Sub(refusal.GetObservedAt().AsTime())
			if delay < MaxReconnectDelay/2-time.Second || delay > MaxReconnectDelay || refusal.GetRemoteProtocol().GetHostSide() != "too_old" || refusal.GetRemoteProtocol().GetMinProtocol() != 5 || refusal.GetReconnectAttempt() != 1 || states != 0 || ensures != 0 {
				t.Fatalf("refusal=%v delay=%s states=%d ensures=%d", refusal, delay, states, ensures)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("protocol refusal was not reported")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	credentials.mu.Lock()
	defer credentials.mu.Unlock()
	if credentials.deleted || len(credentials.replacements) != 0 || string(credentials.value) != "credential-one" || local.registration.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
		t.Fatal("protocol refusal changed registration authority")
	}
}
