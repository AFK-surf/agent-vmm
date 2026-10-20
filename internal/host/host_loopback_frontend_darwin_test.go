//go:build darwin

package host

import (
	"context"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testHostFrontendStream struct {
	sent chan *hostv1.HostFrontendFrame
	recv chan *hostv1.ControllerFrontendFrame
}

func (stream *testHostFrontendStream) Send(frame *hostv1.HostFrontendFrame) error {
	stream.sent <- frame
	return nil
}
func (stream *testHostFrontendStream) Recv() (*hostv1.ControllerFrontendFrame, error) {
	frame, ok := <-stream.recv
	if !ok {
		return nil, io.EOF
	}
	return frame, nil
}

func TestHostLoopbackFrontendConsumesExactTokenBeforeQueueing(t *testing.T) {
	manager := newHostLoopbackFrontendManager(&Controller{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease := &hostLoopbackFrontendLease{manager: manager, ctx: ctx, cancel: cancel, connections: make(map[string]net.Conn), proto: &hostv1.HostLoopbackFrontendLease{
		RegistrationId: "registration", ExpiresAt: timestamppb.New(time.Now().Add(time.Minute)),
		EffectiveCapacity: &hostv1.HostLoopbackFrontendCapacity{MaxUseCount: 2, MaxConcurrentConnections: 2, MaxConcurrentConnectionsPerLease: 2, ConnectionIdleSeconds: 1},
	}}
	for index := range lease.token {
		lease.token[index] = byte(index + 1)
	}

	wrongHost, wrongClient := net.Pipe()
	go lease.authenticate(wrongHost)
	wrong := make([]byte, 32)
	if _, err := wrongClient.Write([]byte("AVFWD1 " + hex.EncodeToString(wrong) + "\n")); err != nil {
		t.Fatal(err)
	}
	_ = wrongClient.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := wrongClient.Read(make([]byte, 1)); err == nil {
		t.Fatal("wrong token connection remained open")
	}
	select {
	case <-manager.queue("registration", "", 0):
		t.Fatal("wrong token opened a frontend stream")
	default:
	}

	host, client := net.Pipe()
	go lease.authenticate(host)
	if _, err := client.Write([]byte("AVFWD1 " + hex.EncodeToString(lease.token[:]) + "\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case pending := <-manager.queue("registration", "", 0):
		if pending.connectionID == "" || pending.connection != host {
			t.Fatalf("pending connection = %+v", pending)
		}
		lease.drop(pending.connectionID)
	case <-time.After(time.Second):
		t.Fatal("valid token did not open a frontend stream")
	}
	client.Close()
	host, client = net.Pipe()
	defer client.Close()
	id, ok := manager.reserveConnection(lease, host)
	if !ok {
		t.Fatal("remaining lease use was rejected")
	}
	lease.drop(id)
	host, client = net.Pipe()
	defer host.Close()
	defer client.Close()
	if _, ok := manager.reserveConnection(lease, host); ok {
		t.Fatal("dynamic lease use count was reset after release")
	}
}

func TestHostLoopbackFrontendCloseIsSynchronousAndGenerationFenced(t *testing.T) {
	manager := newHostLoopbackFrontendManager(&Controller{})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lease := &hostLoopbackFrontendLease{manager: manager, listener: listener, ctx: ctx, cancel: cancel, connections: make(map[string]net.Conn),
		proto: &hostv1.HostLoopbackFrontendLease{FrontendLeaseId: "lease", Generation: 7, RegistrationId: "registration", AllocationId: "allocation",
			EffectiveCapacity: &hostv1.HostLoopbackFrontendCapacity{MaxActiveLeases: 1, MaxActiveLeasesPerAllocation: 1}}}
	if err := manager.add(lease, lease.proto.GetEffectiveCapacity()); err != nil {
		t.Fatal(err)
	}
	if manager.remove("lease", 6, hostv1.HostLoopbackFrontendEvent_KIND_RELEASED) {
		t.Fatal("stale generation released lease")
	}
	if !manager.remove("lease", 7, hostv1.HostLoopbackFrontendEvent_KIND_RELEASED) {
		t.Fatal("exact release failed")
	}
	if _, err := net.DialTimeout("tcp4", listener.Addr().String(), 100*time.Millisecond); err == nil {
		t.Fatal("listener remained open after release")
	}
	select {
	case event := <-manager.eventQueue("registration", "", 0):
		if event.GetKind() != hostv1.HostLoopbackFrontendEvent_KIND_RELEASED || event.GetFrontendLeaseId() != "lease" || event.GetGeneration() != 7 {
			t.Fatalf("release event = %+v", event)
		}
	default:
		t.Fatal("release did not emit a typed event")
	}
}

func TestHostLoopbackFrontendPendingQueuesAreConnectionFenced(t *testing.T) {
	manager := newHostLoopbackFrontendManager(&Controller{})
	pending := &hostLoopbackPendingConnection{connectionID: "connection"}
	manager.queue("registration", "current", 2) <- pending
	select {
	case <-manager.queue("registration", "current", 1):
		t.Fatal("stale connector consumed current pending connection")
	default:
	}
	if got := <-manager.queue("registration", "current", 2); got != pending {
		t.Fatalf("current connector received %+v", got)
	}
}

func TestHostLoopbackFrontendDiscardsDisconnectedEpochQueues(t *testing.T) {
	manager := newHostLoopbackFrontendManager(nil)
	manager.queue("registration", "instance", 7)
	manager.eventQueue("registration", "instance", 7)
	manager.discardEpoch("registration", "instance", 7)
	if len(manager.pending) != 0 || len(manager.events) != 0 {
		t.Fatalf("epoch queues remain: pending=%d events=%d", len(manager.pending), len(manager.events))
	}
}

func TestHostLoopbackFrontendPreservesTCPHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan *net.TCPConn, 1)
	go func() { connection, _ := listener.Accept(); accepted <- connection.(*net.TCPConn) }()
	clientValue, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client := clientValue.(*net.TCPConn)
	defer client.Close()
	host := <-accepted
	defer host.Close()
	stream := &testHostFrontendStream{sent: make(chan *hostv1.HostFrontendFrame, 4), recv: make(chan *hostv1.ControllerFrontendFrame, 4)}
	result := make(chan error, 1)
	capacity := &hostv1.HostLoopbackFrontendCapacity{MaxFrameBytes: 1024, MaxConnectionBytes: 4096, ConnectionDeadlineSeconds: 5, ConnectionIdleSeconds: 2}
	go func() {
		result <- bridgeHostFrontend(context.Background(), host, stream, capacity, time.Now().Add(5*time.Second))
	}()
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if frame := <-stream.sent; string(frame.GetData()) != "request" {
		t.Fatalf("request frame = %+v", frame)
	}
	if frame := <-stream.sent; !frame.GetCloseWrite() {
		t.Fatalf("half-close frame = %+v", frame)
	}
	stream.recv <- &hostv1.ControllerFrontendFrame{Payload: &hostv1.ControllerFrontendFrame_Data{Data: []byte("response")}}
	stream.recv <- &hostv1.ControllerFrontendFrame{Payload: &hostv1.ControllerFrontendFrame_CloseWrite{CloseWrite: true}}
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "response" {
		t.Fatalf("response = %q, %v", response, err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
