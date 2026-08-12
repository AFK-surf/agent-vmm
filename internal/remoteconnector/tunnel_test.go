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
)

type sessionLocal struct {
	hostv1.UnimplementedRemoteComputeServiceServer
	socket string
}

func (server sessionLocal) OpenRuntimeSession(context.Context, *hostv1.OpenRuntimeSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	return &hostv1.CreateAgentSessionResponse{SocketPath: server.socket, SessionId: "private"}, nil
}

type sessionController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	received chan string
}

func (server sessionController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHeader().GetFrameBytes() != FrameBytes || first.GetHeader().GetMaxInFlightBytes() != MaxInFlightBytes {
		return io.ErrUnexpectedEOF
	}
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
	localConnection, localCleanup := localSessionClient(t, sessionLocal{socket: socket})
	defer localCleanup()
	controllerService := sessionController{received: make(chan string, 1)}
	controllerConnection, controllerCleanup := controllerSessionClient(t, controllerService)
	defer controllerCleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := newTunnelManager(ctx, remotev1.NewRemoteControllerServiceClient(controllerConnection), hostv1.NewRemoteComputeServiceClient(localConnection), "registration", 17, nil)
	ready, err := manager.Open(ctx, &remotev1.OpenSession{AllocationId: "allocation", LeaseId: "lease", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ready.GetTunnelNonce() == "" {
		t.Fatal("tunnel nonce is empty")
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
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tunnel metrics did not settle")
		}
		time.Sleep(10 * time.Millisecond)
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
