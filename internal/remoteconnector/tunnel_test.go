package remoteconnector

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type sessionLocal struct {
	hostv1.UnimplementedRemoteComputeServiceServer
	socket    string
	sessionID string
	expiresAt time.Time
	revoked   chan *hostv1.RevokeRuntimeSessionRequest
}

func (server sessionLocal) OpenRuntimeSession(context.Context, *hostv1.OpenRuntimeSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	return &hostv1.CreateAgentSessionResponse{SocketPath: server.socket, SessionId: server.sessionID, ExpiresAt: timestamppb.New(server.expiresAt)}, nil

}

func (server sessionLocal) RevokeRuntimeSession(_ context.Context, request *hostv1.RevokeRuntimeSessionRequest) (*emptypb.Empty, error) {
	server.revoked <- request
	return &emptypb.Empty{}, nil
}

type sessionController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	received      chan string
	headerExpires chan int64
}

type closingSessionController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
}

func (closingSessionController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	_, err := stream.Recv()
	return err
}

func (server sessionController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHeader().GetFrameBytes() != FrameBytes || first.GetHeader().GetMaxInFlightBytes() != MaxInFlightBytes {
		return io.ErrUnexpectedEOF
	}
	server.headerExpires <- first.GetHeader().GetExpiresUnixMillis()
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	server.received <- string(frame.GetData())
	if err := stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_Data{Data: []byte("from-controller")}}); err != nil {
		return err
	}
	if err := stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_CloseWrite{CloseWrite: true}}); err != nil {
		return err
	}
	frame, err = stream.Recv()
	if err != nil {
		return err
	}
	if !frame.GetCloseWrite() {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func TestTunnelBridgeAndHalfClose(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "agent-vmm-tunnel-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	socket := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeReceived := make(chan string, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		unix := connection.(*net.UnixConn)
		_, _ = unix.Write([]byte("from-runtime"))
		_ = unix.CloseWrite()
		value, _ := io.ReadAll(unix)
		runtimeReceived <- string(value)
		unix.Close()
	}()
	revoked := make(chan *hostv1.RevokeRuntimeSessionRequest, 1)
	expiresAt := time.Now().Add(5 * time.Minute).Truncate(time.Millisecond)
	localConnection, localCleanup := localSessionClient(t, sessionLocal{socket: socket, sessionID: "private", expiresAt: expiresAt, revoked: revoked})
	defer localCleanup()
	controllerService := sessionController{received: make(chan string, 1), headerExpires: make(chan int64, 1)}
	controllerConnection, controllerCleanup := controllerSessionClient(t, controllerService)
	defer controllerCleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := newTunnelManager(ctx, remotev1.NewRemoteControllerServiceClient(controllerConnection), hostv1.NewRemoteComputeServiceClient(localConnection), "registration", "connection", 17, nil)
	ready, err := manager.Open(ctx, &remotev1.OpenSession{AllocationId: "allocation", AllocationGeneration: 1, ExecutionOwnerId: "runtime:allocation:1"})
	if err != nil {
		t.Fatal(err)
	}
	if ready.GetTunnelNonce() == "" || ready.GetExpiresUnixMillis() != expiresAt.UnixMilli() {
		t.Fatalf("session ready=%+v expected expiry=%d", ready, expiresAt.UnixMilli())
	}
	select {
	case got := <-controllerService.headerExpires:
		if got != expiresAt.UnixMilli() {
			t.Fatalf("session header expiry=%d want=%d", got, expiresAt.UnixMilli())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not receive session header")
	}
	select {
	case value := <-controllerService.received:
		if value != "from-runtime" {
			t.Fatalf("controller received %q", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not receive runtime bytes")
	}
	select {
	case value := <-runtimeReceived:
		if value != "from-controller" {
			t.Fatalf("runtime received %q", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not receive controller bytes")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		active, completed := manager.Metrics()
		if active == 0 && len(completed) == 1 {
			if completed[0].BytesToController == 0 || completed[0].BytesToRuntime == 0 {
				t.Fatal("byte metrics are empty")
			}
			domain := manager.DomainMetrics()
			if domain.GetActiveSessions() != 0 || domain.GetBytesToController() != completed[0].BytesToController || domain.GetBytesToRuntime() != completed[0].BytesToRuntime || domain.GetSampledAt() == nil {
				t.Fatalf("domain metrics=%+v completed=%+v", domain, completed[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tunnel metrics did not settle")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case request := <-revoked:
		if request.GetSessionId() != "private" || request.GetAllocationGeneration() != 1 {
			t.Fatalf("revoke request = %+v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("completed tunnel did not revoke its runtime session")
	}
}

func TestTunnelOpenFailureRevokesRuntimeSession(t *testing.T) {
	revoked := make(chan *hostv1.RevokeRuntimeSessionRequest, 1)
	localConnection, localCleanup := localSessionClient(t, sessionLocal{socket: "/missing/runtime.sock", sessionID: "opened-before-failure", revoked: revoked})
	defer localCleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := newTunnelManager(ctx, nil, hostv1.NewRemoteComputeServiceClient(localConnection), "registration", "connection", 17, nil)
	if _, err := manager.Open(ctx, &remotev1.OpenSession{AllocationId: "allocation", AllocationGeneration: 3, ExecutionOwnerId: "runtime:allocation:1"}); err == nil {
		t.Fatal("invalid runtime socket was accepted")
	}
	select {
	case request := <-revoked:
		if request.GetSessionId() != "opened-before-failure" || request.GetAllocationGeneration() != 3 {
			t.Fatalf("revoke request = %+v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed tunnel open did not revoke its runtime session")
	}
}

func TestControllerCloseRevokesRuntimeSession(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "agent-vmm-close-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	socket := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			accepted <- connection
		}
	}()
	revoked := make(chan *hostv1.RevokeRuntimeSessionRequest, 1)
	localConnection, localCleanup := localSessionClient(t, sessionLocal{socket: socket, sessionID: "controller-closed", revoked: revoked})
	defer localCleanup()
	controllerConnection, controllerCleanup := controllerSessionClient(t, closingSessionController{})
	defer controllerCleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := newTunnelManager(ctx, remotev1.NewRemoteControllerServiceClient(controllerConnection), hostv1.NewRemoteComputeServiceClient(localConnection), "registration", "connection", 17, nil)
	if _, err := manager.Open(ctx, &remotev1.OpenSession{AllocationId: "allocation", AllocationGeneration: 5, ExecutionOwnerId: "runtime:allocation:1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case connection := <-accepted:
		defer connection.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("runtime connection was not accepted")
	}
	select {
	case request := <-revoked:
		if request.GetSessionId() != "controller-closed" || request.GetAllocationGeneration() != 5 {
			t.Fatalf("revoke request = %+v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller close did not revoke its runtime session")
	}
}

func localSessionClient(t *testing.T, service hostv1.RemoteComputeServiceServer) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	hostv1.RegisterRemoteComputeServiceServer(server, service)
	go server.Serve(listener)
	connection, err := grpc.NewClient("passthrough:///local", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	return connection, func() { connection.Close(); server.Stop(); listener.Close() }
}
func controllerSessionClient(t *testing.T, service remotev1.RemoteControllerServiceServer) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	remotev1.RegisterRemoteControllerServiceServer(server, service)
	go server.Serve(listener)
	connection, err := grpc.NewClient("passthrough:///controller", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	return connection, func() { connection.Close(); server.Stop(); listener.Close() }
}
