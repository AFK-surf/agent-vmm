package remoteconnector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	mu           sync.Mutex
	registration *hostv1.RemoteRegistration
	states       []hostv1.RemoteConnectionState
	instance     string
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
	return &hostv1.GetRemoteStateResponse{Registration: server.registration}, nil
}
func (server *fakeLocal) ReportConnectorStatus(_ context.Context, request *hostv1.ReportConnectorStatusRequest) (*hostv1.GetRemoteStateResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if request.GetConnectionInstance() != server.instance {
		return nil, status.Error(codes.Aborted, "stale")
	}
	return &hostv1.GetRemoteStateResponse{Registration: server.registration}, nil
}
func (server *fakeLocal) EnsureAllocation(_ context.Context, request *hostv1.EnsureAllocationRequest) (*hostv1.AllocationResponse, error) {
	return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), DisplayLabel: request.GetDisplayLabel(), Limits: request.GetLimits(), EgressMode: request.GetEgressMode(), State: hostv1.AllocationState_ALLOCATION_STATE_READY}}, nil
}

type fakeController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	t     *testing.T
	hello *remotev1.HostHello
}

func (server *fakeController) Enroll(_ context.Context, request *remotev1.EnrollRequest) (*remotev1.EnrollResponse, error) {
	if string(request.GetEnrollmentToken()) != "one-time" || request.GetProtocolVersion() != ProtocolVersion {
		return nil, status.Error(codes.Unauthenticated, "rejected")
	}
	return &remotev1.EnrollResponse{ProtocolVersion: ProtocolVersion, RegistrationId: request.GetRegistrationId(), ControllerDisplayName: "test", Credential: []byte("credential-one")}, nil
}
func (server *fakeController) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
	values := metadata.ValueFromIncomingContext(stream.Context(), "authorization")
	if len(values) != 1 || values[0] != "Bearer credential-one" {
		return status.Error(codes.Unauthenticated, "rejected")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	server.hello = first.GetHello()
	if server.hello == nil {
		return status.Error(codes.InvalidArgument, "inventory must be first")
	}
	command := &remotev1.ControllerCommand{CorrelationId: "ensure-1", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "allocation-b", DisplayLabel: "remote", Limits: &hostv1.ResourceLimits{CpuMillis: uint32Pointer(100)}, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: 7}}}
	if err := stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_Command{Command: command}}); err != nil {
		return err
	}
	duplicate := &remotev1.ControllerCommand{CorrelationId: "ensure-2", Command: command.GetCommand()}
	if err := stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_Command{Command: duplicate}}); err != nil {
		return err
	}
	for _, correlation := range []string{"ensure-1", "ensure-2"} {
		result, err := stream.Recv()
		if err != nil {
			return err
		}
		if !result.GetResult().GetSuccess() || result.GetResult().GetCorrelationId() != correlation || result.GetResult().GetAllocation().GetAllocationId() != "allocation-b" {
			return status.Error(codes.Internal, "bad result")
		}
	}
	if err := stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_RotateCredential{RotateCredential: &remotev1.RotateCredential{Credential: []byte("credential-two")}}}); err != nil {
		return err
	}
	return stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_RevokeCredential{RevokeCredential: &remotev1.RevokeCredential{Reason: "test"}}})
}

func TestEnrollAndControlTranscript(t *testing.T) {
	local := &fakeLocal{registration: testRegistration(ProtocolVersion)}
	socket := startLocalServer(t, local)
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "localhost", controller)
	store := &memoryCredentials{}
	config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: socket, CredentialStore: store, TLSRoots: roots}
	token := []byte("one-time")
	if _, err := Enroll(context.Background(), config, token); err != nil {
		t.Fatal(err)
	}
	for _, value := range token {
		if value != 0 {
			t.Fatal("enrollment token was not cleared")
		}
	}
	if err := connectOnce(context.Background(), config); err != ErrCredentialRevoked {
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

func TestTLSAndVersionFailures(t *testing.T) {
	controller := &fakeController{t: t}
	endpoint, roots := startTLSServer(t, "different.test", controller)
	config := Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: "registration-a", RemoteComputeSocket: "/tmp/not-used.sock", CredentialStore: &memoryCredentials{}, TLSRoots: roots}
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

func TestConnectorHeartbeatStopsRevokedRegistration(t *testing.T) {
	local := &fakeLocal{instance: "connection", registration: testRegistration(ProtocolVersion)}
	local.registration.EnrollmentState = hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING
	socket := startLocalServer(t, local)
	connection, err := newLocalClient(context.Background(), socket)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.connection.Close()
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	cancelled := make(chan struct{})
	done := make(chan struct{})
	connectorHeartbeat(ctx, connection.client, "connection", 10*time.Millisecond, func() { close(cancelled) }, done)
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
	result := commandHandler{}.handle(context.Background(), &remotev1.ControllerCommand{CorrelationId: string(make([]byte, 129))})
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
	return &hostv1.RemoteRegistration{Id: "registration-a", ProtocolVersion: version, Revision: 7, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, Pool: &hostv1.RemotePoolPolicy{MaxEnvironments: 4, AggregateLimits: &hostv1.ResourceLimits{}, PerEnvironmentLimits: &hostv1.ResourceLimits{}}, Allocations: []*hostv1.RemoteAllocation{{AllocationId: "allocation-a", State: hostv1.AllocationState_ALLOCATION_STATE_READY}}}
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
	certificate, roots := certificateFor(t, name)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&certificate)))
	remotev1.RegisterRemoteControllerServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	return net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), roots
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

func uint32Pointer(value uint32) *uint32 { return &value }
