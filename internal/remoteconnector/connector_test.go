package remoteconnector

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"github.com/AFK-surf/agent-vmm/pkg/signing"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	hostv1.UnimplementedRemoteComputeServiceServer
	mu                    sync.Mutex
	registration          *hostv1.RemoteRegistration
	states                []hostv1.RemoteConnectionState
	instance              string
	ensureCalls           int
	connectedRegistration *hostv1.RemoteRegistration
	admissionRegistration *hostv1.RemoteRegistration
	reports               []*hostv1.ConnectorStatus
}

func (server *fakeLocal) GetRemoteState(context.Context, *hostv1.GetRemoteStateRequest) (*hostv1.GetRemoteStateResponse, error) {
	return &hostv1.GetRemoteStateResponse{Registration: server.registration}, nil
}
func (server *fakeLocal) SetConnectionState(_ context.Context, request *hostv1.SetConnectionStateRequest) (*hostv1.GetRemoteStateResponse, error) {
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
	return &hostv1.GetRemoteStateResponse{Registration: registration}, nil
}
func (server *fakeLocal) ReportConnectorStatus(_ context.Context, request *hostv1.ReportConnectorStatusRequest) (*hostv1.GetRemoteStateResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if request.GetConnectionInstance() != server.instance {
		return nil, status.Error(codes.Aborted, "stale")
	}
	server.reports = append(server.reports, request.GetStatus())
	return &hostv1.GetRemoteStateResponse{Registration: server.registration}, nil
}
func (server *fakeLocal) SetRemoteAdmissionState(_ context.Context, request *hostv1.SetRemoteAdmissionStateRequest) (*hostv1.GetRemoteStateResponse, error) {
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
	return &hostv1.GetRemoteStateResponse{Registration: registration}, nil
}
func (server *fakeLocal) EnsureAllocation(_ context.Context, request *hostv1.EnsureAllocationRequest) (*hostv1.AllocationResponse, error) {
	server.mu.Lock()
	server.ensureCalls++
	server.mu.Unlock()
	return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), DisplayLabel: request.GetDisplayLabel(), Generation: request.GetGeneration(), ResourcesV2: request.GetResourcesV2(), EgressMode: request.GetEgressMode(), State: hostv1.AllocationState_ALLOCATION_STATE_READY}}, nil
}

type fakeController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	t      *testing.T
	hello  *remotev1.HostHello
	key    *ecdsa.PrivateKey
	reject codes.Code
}

func (server *fakeController) Enroll(_ context.Context, request *remotev1.EnrollRequest) (*remotev1.EnrollResponse, error) {
	if string(request.GetEnrollmentToken()) != "one-time" || request.GetProtocolVersion() != ProtocolVersion || !containsAllFeatures(request.GetSupportedFeatures(), protocolFeatures) || !containsAllFeatures(request.GetSupportedFeatures(), runtimeCapabilityFeatures) || !containsAllFeatures(request.GetRequiredFeatures(), requiredProtocolFeatures) || containsAllFeatures(request.GetRequiredFeatures(), append([]string{remoteObservationFeature}, runtimeCapabilityFeatures...)) || request.GetDeviceIdentity().GetDeviceId() == "" {
		return nil, status.Error(codes.Unauthenticated, "rejected")
	}
	if server.key == nil {
		server.key, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	}
	now := time.Now()
	authority := &trustv1.AuthorityDescriptor{AuthorityId: "managed-test", AuthorityClass: trustv1.AuthorityClass_AUTHORITY_CLASS_MANAGED_CONTROLLER, KeyId: "managed-key", Revision: 1}
	anchor := &trustv1.TrustAnchor{Authority: authority, Revision: 1, NotBefore: timestamppb.New(now.Add(-time.Minute)), NotAfter: timestamppb.New(now.Add(time.Hour)),
		VerificationKeys:       []*trustv1.VerificationKey{{KeyId: "managed-key", PublicKey: elliptic.MarshalCompressed(elliptic.P256(), server.key.X, server.key.Y), SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}},
		AllowedCapabilityKinds: []trustv1.CapabilityKind{trustv1.CapabilityKind_CAPABILITY_KIND_SERVICE_ROUTE}}
	claims := sha256.Sum256([]byte(request.GetRegistrationId()))
	credential := &trustv1.MembershipCredential{Authority: authority, SubjectDeviceId: request.GetDeviceIdentity().GetDeviceId(), SubjectRootKeyRevision: request.GetDeviceIdentity().GetRootKeyRevision(), SubjectRootPublicKey: request.GetDeviceIdentity().GetRootPublicKey(), OpaqueClaimsDigest: claims[:], PolicyRevision: 1, ExpiresAt: timestamppb.New(now.Add(30 * time.Minute))}
	input, _ := signing.CanonicalInput(credential)
	digest := sha256.Sum256(input)
	credential.Signature, _ = ecdsa.SignASN1(rand.Reader, server.key, digest[:])
	return &remotev1.EnrollResponse{ProtocolVersion: ProtocolVersion, RegistrationId: request.GetRegistrationId(), ControllerDisplayName: "test", Credential: []byte("credential-one"), EnabledFeatures: append([]string(nil), requiredProtocolFeatures...), TrustAnchor: anchor, MembershipCredential: credential}, nil
}

type fakeEnrollmentTrust struct {
	trustv1.UnimplementedTrustServiceServer
	identity  *trustv1.DeviceIdentity
	installed bool
	reject    string
}

func (server *fakeEnrollmentTrust) GetDeviceIdentity(context.Context, *trustv1.GetDeviceIdentityRequest) (*trustv1.DeviceIdentity, error) {
	return server.identity, nil
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
	capability := filepath.Join(directory, "trust.cap")
	if err := os.WriteFile(capability, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	root, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	implementation := &fakeEnrollmentTrust{identity: &trustv1.DeviceIdentity{DeviceId: "device-a", RootPublicKey: elliptic.MarshalCompressed(elliptic.P256(), root.X, root.Y), RootKeyRevision: 1, SignatureSuite: trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256}}
	trustv1.RegisterTrustServiceServer(server, implementation)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	config.TrustSocket = socket
	config.TrustCapabilityFile = capability
	return implementation
}
func (server *fakeController) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
	if server.reject != codes.OK {
		return status.Error(server.reject, "rejected")
	}
	values := metadata.ValueFromIncomingContext(stream.Context(), "authorization")
	if len(values) != 1 || values[0] != "Bearer Y3JlZGVudGlhbC1vbmU=" {
		return status.Error(codes.Unauthenticated, "rejected")
	}
	session, err := controllerremote.AcceptControl(stream, "registration-a")
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	server.hello = session.Hello()
	if err := session.Ready(); err != nil {
		return err
	}
	command := &remotev1.ControllerCommand{CommandId: "ensure-1", Sequence: 1, ConnectionEpoch: session.Hello().GetConnectionEpoch(), TargetRevision: 7, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b", Generation: 1, DisplayLabel: "remote", ResourcesV2: &hostv1.WorkloadResourcesV2{CpuMaxMillis: 100, MemoryMaxBytes: 128 << 20, PidMax: 64, WritableQuotaBytes: 256 << 20}, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: 7}}}
	commandContext, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
	defer cancel()
	transcript, err := session.Dispatch(commandContext, command)
	if err != nil || transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED || transcript.Result.GetAllocation().GetAllocationId() != "allocation-b" {
		return status.Error(codes.Internal, "bad result")
	}
	if err := stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_RotateCredential{RotateCredential: &remotev1.RotateCredential{Credential: []byte("credential-two")}}}); err != nil {
		return err
	}
	return stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_RevokeCredential{RevokeCredential: &remotev1.RevokeCredential{Reason: "test"}}})
}

func TestEnrollKeepsManagedTrustFailureReason(t *testing.T) {
	local := &fakeLocal{registration: testRegistration(ProtocolVersion)}
	socket := startLocalServer(t, local)
	endpoint, roots := startTLSServer(t, "localhost", &fakeController{t: t})
	store := &memoryCredentials{}
	config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: socket, CredentialStore: store, TLSRoots: roots}
	trust := configureEnrollmentTrust(t, &config)
	trust.reject = "managed trust bundle is invalid: managed trust stage=anchor_time reason=not_yet_valid excess_ms=360000"
	_, err := Enroll(context.Background(), config, []byte("one-time"))
	if err == nil || !strings.Contains(err.Error(), "reason=not_yet_valid") {
		t.Fatalf("enrollment error lost the trust failure reason: %v", err)
	}
}

func TestEnrollAndControlTranscript(t *testing.T) {
	local := &fakeLocal{registration: testRegistration(ProtocolVersion)}
	socket := startLocalServer(t, local)
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	store := &memoryCredentials{}
	config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: socket, CredentialStore: store, TLSRoots: roots}
	ready := make(chan struct{})
	admission := make(chan struct{})
	config.OnReady = func() { close(ready) }
	config.CommandAdmission = admission
	trust := configureEnrollmentTrust(t, &config)
	token := []byte("one-time")
	if _, err := Enroll(context.Background(), config, token); err != nil {
		t.Fatal(err)
	}
	if !trust.installed {
		t.Fatal("managed trust bundle was not installed")
	}
	for _, value := range token {
		if value != 0 {
			t.Fatal("enrollment token was not cleared")
		}
	}
	connected := make(chan error, 1)
	go func() { connected <- connectOnce(context.Background(), config) }()
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
	if err := <-connected; err != ErrCredentialRevoked {
		t.Fatalf("connectOnce error = %v", err)
	}
	if controller.hello.GetRegistrationId() != "registration-a" || controller.hello.GetPoolRevision() != 7 || len(controller.hello.GetInventory()) != 1 {
		t.Fatalf("unexpected hello: %+v", controller.hello)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.replacements) != 2 || string(store.replacements[1]) != "credential-two" || !store.deleted {
		t.Fatal("credential rotation/revoke was not atomic")
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.states) != 2 || local.states[0] != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED || local.states[1] != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED {
		t.Fatalf("connection states = %v", local.states)
	}
}

func TestConnectionFenceRejectsInventoryMutationBeforeAdmission(t *testing.T) {
	initial := testRegistration(ProtocolVersion)
	updated := proto.Clone(initial).(*hostv1.RemoteRegistration)
	updated.InventoryRevision++
	local := &fakeLocal{registration: initial, connectedRegistration: updated}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{
		Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a",
		RemoteComputeSocket: startLocalServer(t, local), CredentialStore: &memoryCredentials{value: []byte("credential-one")}, TLSRoots: roots,
	}
	ready := make(chan struct{})
	config.OnReady = func() { close(ready) }
	err := connectOnce(context.Background(), config)
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
	initial := testRegistration(ProtocolVersion)
	updated := proto.Clone(initial).(*hostv1.RemoteRegistration)
	updated.InventoryRevision++
	local := &fakeLocal{registration: initial, admissionRegistration: updated}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{
		Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a",
		RemoteComputeSocket: startLocalServer(t, local), CredentialStore: &memoryCredentials{value: []byte("credential-one")}, TLSRoots: roots,
	}
	ready := make(chan struct{})
	config.OnReady = func() { close(ready) }
	err := connectOnce(context.Background(), config)
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
	initial := testRegistration(ProtocolVersion)
	draining := proto.Clone(initial).(*hostv1.RemoteRegistration)
	draining.State = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING
	local := &fakeLocal{registration: initial, admissionRegistration: draining}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{
		Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a",
		RemoteComputeSocket: startLocalServer(t, local), CredentialStore: &memoryCredentials{value: []byte("credential-one")}, TLSRoots: roots,
	}
	ready := make(chan struct{})
	config.OnReady = func() { close(ready) }
	err := connectOnce(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "local admission fence failed") {
		t.Fatalf("connectOnce error = %v", err)
	}
	select {
	case <-ready:
		t.Fatal("connector reopened operator drain")
	default:
	}
}

func TestTLSAndVersionFailures(t *testing.T) {
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "different.test", controller)
	config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: "/tmp/not-used.sock", CredentialStore: &memoryCredentials{}, TLSRoots: roots}
	configureEnrollmentTrust(t, &config)
	if _, err := Enroll(context.Background(), config, []byte("one-time")); err == nil {
		t.Fatal("TLS hostname mismatch succeeded")
	}
	local := &fakeLocal{registration: testRegistration("2")}
	config.RemoteComputeSocket = startLocalServer(t, local)
	config.Endpoint, config.TLSRoots = startTLSServer(t, "localhost", controller)
	config.CredentialStore = &memoryCredentials{value: []byte("credential-one")}
	if err := connectOnce(context.Background(), config); err == nil {
		t.Fatal("version mismatch succeeded")
	}
}

func TestBearerAuthenticationFailure(t *testing.T) {
	local := &fakeLocal{registration: testRegistration(ProtocolVersion)}
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: startLocalServer(t, local), CredentialStore: &memoryCredentials{value: []byte("wrong")}, TLSRoots: roots}
	if err := connectOnce(context.Background(), config); err == nil {
		t.Fatal("invalid bearer credential succeeded")
	}
}

type unavailableCredentials struct{ memoryCredentials }

func (*unavailableCredentials) Load(context.Context, string) ([]byte, error) {
	return nil, ErrCredentialUnavailable
}

func TestRunRetriesAuthenticationAndConfigurationAndStopsOnlyOnRevocation(t *testing.T) {
	tests := []struct {
		name     string
		reject   codes.Code
		store    CredentialStore
		state    hostv1.RemoteRegistrationState
		version  string
		phase    hostv1.ConnectorPhase
		code     string
		terminal bool
	}{
		{name: "unauthenticated", reject: codes.Unauthenticated, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CONTROLLER_AUTH_FAILED"},
		{name: "permission denied", reject: codes.PermissionDenied, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CONTROLLER_AUTH_FAILED"},
		{name: "credential unavailable", store: &unavailableCredentials{}, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR, code: "CREDENTIAL_UNAVAILABLE"},
		{name: "protocol version", version: "1", phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR, code: "CONNECTOR_CONFIG_INVALID"},
		{name: "revoked", state: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED, phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, code: "CREDENTIAL_REVOKED", terminal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			version := ProtocolVersion
			if test.version != "" {
				version = test.version
			}
			registration := testRegistration(version)
			if test.state != 0 {
				registration.State = test.state
			}
			local := &fakeLocal{registration: registration}
			endpoint, roots := startTLSServer(t, "localhost", &fakeController{t: t, reject: test.reject})
			store := test.store
			if store == nil {
				store = &memoryCredentials{value: []byte("credential-one")}
			}
			config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: startLocalServer(t, local), CredentialStore: store, TLSRoots: roots}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Run(ctx, config) }()
			if test.terminal {
				select {
				case err := <-done:
					if !errors.Is(err, ErrCredentialRevoked) {
						t.Fatalf("Run error = %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("revocation did not stop the connector")
				}
			}
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
				if test.terminal {
					if len(failures) != 1 || failures[0].GetNextRetryAt() != nil {
						t.Fatalf("revocation reports = %v", failures)
					}
					return
				}
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
	local := &fakeLocal{instance: "connection", registration: testRegistration(ProtocolVersion)}
	local.registration.EnrollmentState = hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING
	socket := startLocalServer(t, local)
	connection, err := newLocalClient(context.Background(), socket, "registration-a")
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
	result := commandHandler{}.handle(context.Background(), &remotev1.ControllerCommand{CommandId: string(make([]byte, 129))})
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

func testRegistration(version string) *hostv1.RemoteRegistration {
	return &hostv1.RemoteRegistration{Id: "registration-a", ProtocolVersion: version, Revision: 7, PoolPolicyRevision: 7, InventoryRevision: 11, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, Pool: &hostv1.RemotePoolPolicy{PerEnvironmentLimits: &hostv1.ResourceLimits{}}, Allocations: []*hostv1.RemoteAllocation{{AllocationId: "allocation-a", State: hostv1.AllocationState_ALLOCATION_STATE_READY}}}
}

func TestObserveRegistrationRequiresHostObservedReadyConnection(t *testing.T) {
	registration := testRegistration(ProtocolVersion)
	registration.EnrollmentState = hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY
	registration.ConnectionState = hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED
	registration.ConnectorStatus = &hostv1.ConnectorStatus{Phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED}
	observation, err := ObserveRegistration(context.Background(), startLocalServer(t, &fakeLocal{registration: registration}), "registration-a")
	if err != nil || !observation.Ready || observation.Revoked {
		t.Fatalf("ready observation=%+v err=%v", observation, err)
	}
	registration.State = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED
	observation, err = ObserveRegistration(context.Background(), startLocalServer(t, &fakeLocal{registration: registration}), "registration-a")
	if err != nil || observation.Ready || !observation.Revoked {
		t.Fatalf("revoked observation=%+v err=%v", observation, err)
	}
}

func TestStoredCredentialBindsBearerToRegistrationMetadata(t *testing.T) {
	metadata, err := (storedCredential{store: &memoryCredentials{value: []byte("0123456789abcdef")}, account: "registration-a"}).GetRequestMetadata(context.Background())
	if err != nil || metadata["x-agent-vmm-registration"] != "registration-a" || metadata["authorization"] == "" {
		t.Fatalf("metadata=%v err=%v", metadata, err)
	}
}

func startLocalServer(t *testing.T, service hostv1.RemoteComputeServiceServer) string {
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
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	hostv1.RegisterRemoteComputeServiceServer(server, service)
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

func TestLoadTLSRootsAddsConfiguredCA(t *testing.T) {
	certificate, _ := certificateFor(t, "controller.test")
	path := filepath.Join(t.TempDir(), "controller-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	roots, err := LoadTLSRoots(path)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: "controller.test", Roots: roots}); err != nil {
		t.Fatal(err)
	}
}

func uint32Pointer(value uint32) *uint32 { return &value }

type unavailableAllocationClient struct {
	hostv1.RemoteComputeServiceClient
}

func (unavailableAllocationClient) EnsureAllocation(context.Context, *hostv1.EnsureAllocationRequest, ...grpc.CallOption) (*hostv1.AllocationResponse, error) {
	return nil, status.Error(codes.Unavailable, "local remote-compute socket connection refused")
}

func TestAllocationFailureRetainsRPCMessage(t *testing.T) {
	handler := commandHandler{client: unavailableAllocationClient{}}
	result := handler.handle(context.Background(), &remotev1.ControllerCommand{CommandId: "failed-ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation"}}})
	if result.GetReason() != remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE || !strings.Contains(result.GetMessage(), "local remote-compute socket connection refused") {
		t.Fatalf("allocation failure lost RPC context: %v", result)
	}
}

func TestRunnerDeadlineSurvivesLocalGRPC(t *testing.T) {
	local := &fakeLocal{registration: testRegistration(ProtocolVersion)}
	connection, err := newLocalClient(context.Background(), startLocalServer(t, local), "registration-a")
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
			handler := commandHandler{client: connection.client, registrationID: "registration-a"}
			result := handler.handle(ctx, &remotev1.ControllerCommand{CommandId: "deadline-ensure", DeadlineUnixMillis: deadline, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b"}}})
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

func (server *rejectedAllocationLocal) EnsureAllocation(context.Context, *hostv1.EnsureAllocationRequest) (*hostv1.AllocationResponse, error) {
	return nil, server.failure
}

func TestRejectedAllocationRetainsPartialAdmissionAcrossLocalGRPC(t *testing.T) {
	for _, dimension := range []string{"storage_headroom", "import_slot"} {
		t.Run(dimension, func(t *testing.T) {
			allocation := &hostv1.RemoteAllocation{AllocationId: "allocation-b", Generation: 2, Revision: 7, State: hostv1.AllocationState_ALLOCATION_STATE_DRAINING}
			failure, err := status.New(codes.ResourceExhausted, "guest projection rejected").WithDetails(
				&errdetails.ErrorInfo{Reason: "RESOURCE_CAPACITY_EXHAUSTED", Metadata: map[string]string{"resource_dimension": dimension}}, allocation)
			if err != nil {
				t.Fatal(err)
			}
			local := &rejectedAllocationLocal{failure: failure.Err()}
			connection, err := newLocalClient(context.Background(), startLocalServer(t, local), "registration-a")
			if err != nil {
				t.Fatal(err)
			}
			defer connection.connection.Close()
			handler := commandHandler{client: connection.client, registrationID: "registration-a"}
			result := handler.handle(context.Background(), &remotev1.ControllerCommand{CommandId: "partial-ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b", Generation: 2}}})
			if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED || result.GetReason() != remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED {
				t.Fatalf("rejection changed: %v", result)
			}
			if got := result.GetAllocation(); got.GetAllocationId() != "allocation-b" || got.GetGeneration() != 2 || got.GetRevision() != 7 || got.GetState() != hostv1.AllocationState_ALLOCATION_STATE_DRAINING {
				t.Fatalf("partial admission lost: %v", result)
			}
			if result.GetCapacityDimension() != capacityDimensionFor(failure.Err()) {
				t.Fatalf("capacity dimension lost: %v", result)
			}
		})
	}
}
