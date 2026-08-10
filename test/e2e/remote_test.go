//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type e2eCredentialStore struct {
	mu    sync.Mutex
	value []byte
}

func (store *e2eCredentialStore) Load(context.Context, string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]byte(nil), store.value...), nil
}
func (store *e2eCredentialStore) Replace(_ context.Context, _ string, value []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.value = append(store.value[:0], value...)
	return nil
}
func (store *e2eCredentialStore) Delete(context.Context, string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.value = nil
	return nil
}

type remoteE2EController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	registrationID string
	sessions       chan remoteE2ESession
	finish         chan struct{}
	connections    chan int
	failures       chan string
}
type remoteE2ESession struct {
	allocationID string
	connection   net.Conn
}

func (server *remoteE2EController) Enroll(context.Context, *remotev1.EnrollRequest) (*remotev1.EnrollResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (server *remoteE2EController) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
	if values := metadata.ValueFromIncomingContext(stream.Context(), "authorization"); len(values) != 1 || values[0] != "Bearer remote-e2e" {
		return status.Error(codes.Unauthenticated, "rejected")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil || hello.GetRegistrationId() != server.registrationID {
		return status.Error(codes.FailedPrecondition, "invalid initial inventory")
	}
	leases := make([]*remotev1.Lease, 0, 2)
	for _, allocation := range hello.GetInventory() {
		if allocation.GetLease() != nil {
			leases = append(leases, allocation.GetLease())
		}
	}
	for index, allocationID := range []string{"remote-a", "remote-b"} {
		if len(hello.GetInventory()) != 0 {
			continue
		}
		egress := hostv1.EgressMode_EGRESS_MODE_DENY_ALL
		if index == 0 {
			egress = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
		}
		limits := &hostv1.ResourceLimits{CpuMillis: optional(uint32(1000)), MemoryBytes: optional(uint64(1 << 30)), Pids: optional(uint32(256)), DiskBytes: optional(uint64(2 << 30))}
		result, err := server.command(stream, &remotev1.ControllerCommand{CorrelationId: "ensure-" + allocationID, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: allocationID, DisplayLabel: allocationID, Limits: limits, EgressMode: egress, PolicyRevision: hello.GetPoolRevision()}}})
		if err != nil {
			return err
		}
		if !result.GetSuccess() {
			server.failures <- "ensure " + result.GetReason().String()
			return status.Error(codes.Internal, "ensure failed")
		}
		result, err = server.command(stream, &remotev1.ControllerCommand{CorrelationId: "acquire-" + allocationID, Command: &remotev1.ControllerCommand_AcquireLease{AcquireLease: &remotev1.AcquireLease{AllocationId: allocationID, DurationSeconds: 300, WorkloadRef: "e2e-" + allocationID}}})
		if err != nil {
			return err
		}
		if !result.GetSuccess() {
			server.failures <- "acquire " + result.GetReason().String()
			return status.Error(codes.Internal, "acquire failed")
		}
		leases = append(leases, result.GetLease())
	}
	server.connections <- len(leases)
	for _, lease := range leases {
		result, err := server.command(stream, &remotev1.ControllerCommand{CorrelationId: "open-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: lease.GetAllocationId(), LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
		if err != nil {
			return err
		}
		if !result.GetSuccess() {
			server.failures <- "open " + lease.GetAllocationId() + " lease=" + lease.GetLeaseId() + " generation=" + strconv.FormatUint(lease.GetGeneration(), 10) + " " + result.GetReason().String() + " " + result.GetMessage()
			return status.Error(codes.Internal, "open failed")
		}
	}
	select {
	case <-server.finish:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for _, lease := range leases {
		_, _ = server.command(stream, &remotev1.ControllerCommand{CorrelationId: "release-lease-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_ReleaseLease{ReleaseLease: &remotev1.ReleaseLease{LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
		_, _ = server.command(stream, &remotev1.ControllerCommand{CorrelationId: "release-allocation-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: lease.GetAllocationId()}}})
		plan, err := server.command(stream, &remotev1.ControllerCommand{CorrelationId: "plan-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_PlanDiscardAllocation{PlanDiscardAllocation: &remotev1.PlanDiscardAllocation{AllocationId: lease.GetAllocationId()}}})
		if err != nil {
			return err
		}
		_, err = server.command(stream, &remotev1.ControllerCommand{CorrelationId: "discard-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_ExecuteDiscardAllocation{ExecuteDiscardAllocation: &remotev1.ExecuteDiscardAllocation{AllocationId: lease.GetAllocationId(), PlanRevision: plan.GetCleanupPlan().GetRevision(), ConfirmedVolumeIds: plan.GetCleanupPlan().GetVolumeIds()}}})
		if err != nil {
			return err
		}
	}
	return nil
}

func (server *remoteE2EController) command(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage], command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	if err := stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_Command{Command: command}}); err != nil {
		return nil, err
	}
	message, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if message.GetResult().GetCorrelationId() != command.GetCorrelationId() {
		return nil, status.Error(codes.Internal, "correlation mismatch")
	}
	return message.GetResult(), nil
}

func (server *remoteE2EController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || header.GetRegistrationId() != server.registrationID {
		return status.Error(codes.Unauthenticated, "bad session header")
	}
	connection := newRemoteStreamConn(stream)
	select {
	case server.sessions <- remoteE2ESession{allocationID: header.GetAllocationId(), connection: connection}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	select {
	case <-connection.closed:
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

type remoteStreamConn struct {
	stream  grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]
	readMu  sync.Mutex
	writeMu sync.Mutex
	buffer  []byte
	closed  chan struct{}
	once    sync.Once
}

func newRemoteStreamConn(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) *remoteStreamConn {
	return &remoteStreamConn{stream: stream, closed: make(chan struct{})}
}
func (connection *remoteStreamConn) Read(value []byte) (int, error) {
	connection.readMu.Lock()
	defer connection.readMu.Unlock()
	for len(connection.buffer) == 0 {
		frame, err := connection.stream.Recv()
		if err != nil {
			return 0, err
		}
		if frame.GetCloseWrite() {
			return 0, io.EOF
		}
		connection.buffer = frame.GetData()
	}
	count := copy(value, connection.buffer)
	connection.buffer = connection.buffer[count:]
	return count, nil
}
func (connection *remoteStreamConn) Write(value []byte) (int, error) {
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	total := len(value)
	for len(value) != 0 {
		count := min(len(value), remoteconnector.FrameBytes)
		if err := connection.stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_Data{Data: append([]byte(nil), value[:count]...)}}); err != nil {
			return total - len(value), err
		}
		value = value[count:]
	}
	return total, nil
}
func (connection *remoteStreamConn) Close() error {
	connection.once.Do(func() {
		connection.writeMu.Lock()
		_ = connection.stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_CloseWrite{CloseWrite: true}})
		connection.writeMu.Unlock()
		close(connection.closed)
	})
	return nil
}
func (*remoteStreamConn) LocalAddr() net.Addr              { return remoteAddr("controller") }
func (*remoteStreamConn) RemoteAddr() net.Addr             { return remoteAddr("host") }
func (*remoteStreamConn) SetDeadline(time.Time) error      { return nil }
func (*remoteStreamConn) SetReadDeadline(time.Time) error  { return nil }
func (*remoteStreamConn) SetWriteDeadline(time.Time) error { return nil }

type remoteAddr string

func (remoteAddr) Network() string        { return "agent-vmm-session" }
func (address remoteAddr) String() string { return string(address) }

func testRemoteWorkflow(t *testing.T, harness *hostHarness) {
	controller := &remoteE2EController{sessions: make(chan remoteE2ESession, 4), finish: make(chan struct{}), connections: make(chan int, 8), failures: make(chan string, 8)}
	endpoint, roots, stopController := startRemoteController(t, controller)
	defer stopController()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := &hostv1.RemotePoolPolicy{MaxEnvironments: 2, AggregateLimits: remoteLimits(2000, 2<<30, 512, 4<<30), PerEnvironmentLimits: remoteLimits(1000, 1<<30, 256, 2<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, MaxLeaseDuration: durationpb.New(5 * time.Minute), DisconnectGrace: durationpb.New(30 * time.Second), StopGrace: durationpb.New(2 * time.Second)}
	registered, err := harness.client.CreateRemoteRegistration(ctx, &hostv1.CreateRemoteRegistrationRequest{RequestId: "remote-e2e", Endpoint: endpoint, DisplayName: "E2E Controller", ProtocolVersion: remoteconnector.ProtocolVersion, Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	controller.registrationID = registered.GetRegistration().GetId()
	store := &e2eCredentialStore{value: []byte("remote-e2e")}
	startConnector := func() (context.CancelFunc, <-chan error) {
		connectorContext, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- remoteconnector.Run(connectorContext, remoteconnector.Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: controller.registrationID, RemoteComputeSocket: harness.remoteComputeSocket, CredentialStore: store, TLSRoots: roots})
		}()
		return stop, done
	}
	collectSessions := func() map[string]remoteE2ESession {
		sessions := make(map[string]remoteE2ESession)
		for len(sessions) != 2 {
			select {
			case session := <-controller.sessions:
				sessions[session.allocationID] = session
			case failure := <-controller.failures:
				current, _ := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{})
				t.Fatalf("controller command failed: %s state=%+v", failure, current.GetRegistration().GetAllocations())
			case <-ctx.Done():
				t.Fatal("remote sessions did not open")
			}
		}
		return sessions
	}
	stopConnector, connectorDone := startConnector()
	if count := <-controller.connections; count != 2 {
		t.Fatalf("first connection lease inventory=%d", count)
	}
	firstSessions := collectSessions()
	for _, session := range firstSessions {
		session.connection.Close()
	}
	stopConnector()
	select {
	case <-connectorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first connector did not stop")
	}
	stopConnector, connectorDone = startConnector()
	select {
	case count := <-controller.connections:
		if count != 2 {
			t.Fatalf("reconnect lease inventory=%d", count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("controller did not reconnect")
	}
	sessions := collectSessions()
	clients := make(map[string]*grpc.ClientConn)
	for allocationID, session := range sessions {
		connection, err := grpc.NewClient("passthrough:///remote-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return session.connection, nil }))
		if err != nil {
			t.Fatal(err)
		}
		clients[allocationID] = connection
		runtime := hostv1.NewAgentRuntimeServiceClient(connection)
		environment, err := runtime.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if environment.GetEnvironment().GetOrigin() != hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_REMOTE || environment.GetEnvironment().GetId() != "" || environment.GetEnvironment().GetNamespaceId() != "" {
			t.Fatalf("unsafe remote projection: %+v", environment)
		}
	}
	remoteClient := &productClient{sessions: map[string]*productSession{"remote-a": {connection: clients["remote-a"], client: hostv1.NewAgentRuntimeServiceClient(clients["remote-a"])}}}
	pullImage(t, remoteClient, "remote-a")
	contextArchive := dockerfileContext(t, buildShell(t))
	result := buildImage(t, remoteClient, "remote-a", "agent.local/remote-e2e:latest", contextArchive, []byte("remote-secret"))
	if result.digest == "" {
		t.Fatal("remote build returned no digest")
	}
	createVolume(t, remoteClient, "remote-a", "data")
	serverContainer := createContainerWithImageAndVolumes(t, remoteClient, "remote-a", "server", "agent.local/remote-e2e:latest", []string{"/bin/result", "--serve", "8080"}, []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	startContainer(t, remoteClient, "remote-a", serverContainer.GetId())
	time.Sleep(500 * time.Millisecond)
	if output, code := execContainer(t, remoteClient, "remote-a", serverContainer.GetId(), []string{"/bin/result", "--write", "/data/value", "remote-exec"}); code != 0 || string(output) != "remote-exec\n" {
		t.Fatalf("remote exec=%q code=%d", output, code)
	}
	if output, code := execContainer(t, remoteClient, "remote-a", serverContainer.GetId(), []string{"/bin/result", "--cpu"}); code != 0 || string(output) != "cpu-ok\n" {
		t.Fatalf("remote cpu exec=%q code=%d", output, code)
	}
	testRemoteForward(t, remoteClient, serverContainer.GetId())
	logged := createContainerWithImage(t, remoteClient, "remote-a", "logged", "agent.local/remote-e2e:latest", []string{"/bin/result"})
	startContainer(t, remoteClient, "remote-a", logged.GetId())
	waitContainer(t, remoteClient, "remote-a", logged.GetId(), 0)
	if output := readLogs(t, remoteClient, "remote-a", logged.GetId()); !bytes.Contains(output, []byte("build-ok")) {
		t.Fatalf("remote logs=%q", output)
	}
	for _, connection := range clients {
		connection.Close()
	}
	close(controller.finish)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		current, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{})
		if err == nil && len(current.GetRegistration().GetAllocations()) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stopConnector()
	select {
	case <-connectorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("connector did not stop")
	}
}

func testRemoteForward(t *testing.T, client *productClient, containerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.ForwardTCP(ctx, "remote-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Header{Header: &hostv1.ForwardTCPHeader{ContainerId: containerID, Port: 8080}}}); err != nil {
		t.Fatal(err)
	}
	payload := []byte("remote-forward")
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Data{Data: payload}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}}); err != nil {
		t.Fatal(err)
	}
	var received []byte
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) || response.GetCloseRead() {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, response.GetData()...)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("remote forward=%q", received)
	}
}

func remoteLimits(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.ResourceLimits {
	return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
}

func startRemoteController(t *testing.T, service remotev1.RemoteControllerServiceServer) (string, *x509.CertPool, func()) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&certificate)), grpc.InitialWindowSize(remoteconnector.StreamWindowBytes), grpc.InitialConnWindowSize(remoteconnector.ConnectionWindowBytes))
	remotev1.RegisterRemoteControllerServiceServer(server, service)
	go server.Serve(listener)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), roots, func() { server.Stop(); listener.Close() }
}
