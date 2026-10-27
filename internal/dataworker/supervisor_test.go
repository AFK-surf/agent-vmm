package dataworker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSupervisorRequiresAbsoluteBoundaries(t *testing.T) {
	_, err := Start(context.Background(), "worker", "socket", "service", "mesh")
	if err == nil {
		t.Fatal("relative process or socket path was accepted")
	}
}

func TestSupervisorStartsProbesDrainsAndStopsChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix sockets")
	}
	directory, err := os.MkdirTemp("/tmp", "avd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "worker.sock")
	script := filepath.Join(directory, "worker")
	body := `#!/bin/sh
exec python3 - "$2" <<'PY'
import json, os, signal, socket, sys
path=sys.argv[1]
def interrupt(*_):
 open(path+'.interrupted','w').close(); os._exit(0)
signal.signal(signal.SIGINT, interrupt)
try: os.unlink(path)
except FileNotFoundError: pass
s=socket.socket(socket.AF_UNIX); s.bind(path); s.listen()
accepting=True
while True:
 c,_=s.accept(); req=json.loads(c.makefile().readline())
 if req['command']=='drain': accepting=False
 c.sendall((json.dumps({'endpoint_id':'ep','endpoint_generation':1,'accepting':accepting,'active_streams':0,'bytes_in':0,'bytes_out':0})+'\n').encode()); c.close()
PY
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	supervisor, err := Start(ctx, script, socket, filepath.Join(directory, "service.sock"), filepath.Join(directory, "mesh.sock"))
	if err != nil {
		t.Fatal(err)
	}
	status, err := supervisor.Status(ctx)
	if err != nil || !status.Accepting {
		t.Fatalf("status = %+v, %v", status, err)
	}
	// The Host cancels the start context on SIGTERM before it closes the
	// worker. Close must still drain and interrupt it, not find it killed.
	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := supervisor.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket + ".interrupted"); err != nil {
		t.Fatalf("worker was not interrupted: %v", err)
	}
}

func TestRealIrohWorkerLifecycle(t *testing.T) {
	binary := os.Getenv("AGENT_VMM_DATA_WORKER_TEST_BINARY")
	if binary == "" {
		t.Skip("set AGENT_VMM_DATA_WORKER_TEST_BINARY to run the real Iroh worker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	directory, err := os.MkdirTemp("/tmp", "avd-real-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	serviceA := filepath.Join(directory, "service-a.sock")
	serviceB := filepath.Join(directory, "service-b.sock")
	startUnixEcho(t, serviceA)
	startUnixEcho(t, serviceB)
	supervisor, err := Start(ctx, binary, filepath.Join(directory, "worker-a.sock"), serviceA, serviceA)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := Start(ctx, binary, filepath.Join(directory, "worker-b.sock"), serviceB, serviceB)
	if err != nil {
		t.Fatal(err)
	}
	status, err := supervisor.Status(ctx)
	peerStatus, peerErr := peer.Status(ctx)
	if err != nil || peerErr != nil || status.EndpointID == "" || status.EndpointGeneration == 0 || !status.Accepting || len(peerStatus.EndpointAddr) == 0 {
		t.Fatalf("status = %+v, %v", status, err)
	}
	var endpointAddr struct {
		ID    string           `json:"id"`
		Addrs []map[string]any `json:"addrs"`
	}
	if err := json.Unmarshal(peerStatus.EndpointAddr, &endpointAddr); err != nil || len(endpointAddr.Addrs) == 0 {
		t.Fatalf("endpoint address = %s, %v", peerStatus.EndpointAddr, err)
	}
	_, port, err := net.SplitHostPort(endpointAddr.Addrs[0]["Ip"].(string))
	if err != nil {
		t.Fatal(err)
	}
	endpointAddr.Addrs = append(endpointAddr.Addrs, map[string]any{"Ip": net.JoinHostPort("127.0.0.1", port)})
	localEndpointAddr, err := json.Marshal(endpointAddr)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := supervisor.Probe(ctx, localEndpointAddr, "service-tunnel-probe")
	if err != nil || probe.ProbePayload != "service-tunnel-probe" || probe.BytesIn < 20 || probe.BytesOut < 20 {
		t.Fatalf("probe = %+v, %v", probe, err)
	}
	tunnel, err := supervisor.DialServiceTunnel(ctx, localEndpointAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tunnel.Write([]byte("transparent-service-stream")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len("transparent-service-stream"))
	if _, err := io.ReadFull(tunnel, buffer); err != nil || string(buffer) != "transparent-service-stream" {
		t.Fatalf("service stream = %q, %v", buffer, err)
	}
	_ = tunnel.Close()
	meshTunnel, err := DialMeshControlTunnel(ctx, filepath.Join(directory, "worker-a.sock"), localEndpointAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := meshTunnel.Write([]byte("mesh-control-route-request")); err != nil {
		t.Fatal(err)
	}
	meshBuffer := make([]byte, len("mesh-control-route-request"))
	if _, err := io.ReadFull(meshTunnel, meshBuffer); err != nil || string(meshBuffer) != "mesh-control-route-request" {
		t.Fatalf("mesh control stream = %q, %v", meshBuffer, err)
	}
	_ = meshTunnel.Close()
	oldPeerStatus := peerStatus
	if err := peer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	staleContext, staleCancel := context.WithTimeout(ctx, 2*time.Second)
	if staleTunnel, staleErr := supervisor.DialServiceTunnel(staleContext, localEndpointAddr); staleErr == nil {
		_ = staleTunnel.Close()
		staleCancel()
		t.Fatal("stale endpoint remained reachable after peer sleep/rotation")
	}
	staleCancel()
	peer, err = Start(ctx, binary, filepath.Join(directory, "worker-b.sock"), serviceB, serviceB)
	if err != nil {
		t.Fatal(err)
	}
	peerStatus, err = peer.Status(ctx)
	if err != nil || peerStatus.EndpointID == oldPeerStatus.EndpointID || peerStatus.EndpointGeneration <= oldPeerStatus.EndpointGeneration {
		t.Fatalf("endpoint did not rotate across wake: old=%+v new=%+v err=%v", oldPeerStatus, peerStatus, err)
	}
	rotatedEndpoint := loopbackEndpoint(t, peerStatus.EndpointAddr)
	rotatedTunnel, err := supervisor.DialServiceTunnel(ctx, rotatedEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rotatedTunnel.Write([]byte("after-network-wake")); err != nil {
		t.Fatal(err)
	}
	rotatedBuffer := make([]byte, len("after-network-wake"))
	if _, err := io.ReadFull(rotatedTunnel, rotatedBuffer); err != nil || string(rotatedBuffer) != "after-network-wake" {
		t.Fatalf("rotated service stream = %q, %v", rotatedBuffer, err)
	}
	_ = rotatedTunnel.Close()
	serviceC := filepath.Join(directory, "service-c.sock")
	startGRPCServiceEcho(t, serviceC)
	grpcPeer, err := Start(ctx, binary, filepath.Join(directory, "worker-c.sock"), serviceC, serviceC)
	if err != nil {
		t.Fatal(err)
	}
	grpcPeerStatus, err := grpcPeer.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	grpcEndpoint := loopbackEndpoint(t, grpcPeerStatus.EndpointAddr)
	clientConnection, err := grpc.NewClient("passthrough:///service", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(dialContext context.Context, _ string) (net.Conn, error) {
			return supervisor.DialServiceTunnel(dialContext, grpcEndpoint)
		}))
	if err != nil {
		t.Fatal(err)
	}
	forward, err := servicev1.NewServiceRouteServiceClient(clientConnection).OpenServiceForward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := forward.Send(&servicev1.OpenServiceForwardRequest{Payload: &servicev1.OpenServiceForwardRequest_Header{Header: &servicev1.OpenServiceForwardHeader{RouteId: "route", LeaseId: "lease", RouteGeneration: 7}}}); err != nil {
		t.Fatal(err)
	}
	if err := forward.Send(&servicev1.OpenServiceForwardRequest{Payload: &servicev1.OpenServiceForwardRequest_Data{Data: []byte("grpc-over-iroh")}}); err != nil {
		t.Fatal(err)
	}
	response, err := forward.Recv()
	if err != nil || string(response.GetData()) != "grpc-over-iroh" {
		t.Fatalf("gRPC service response=%q err=%v", response.GetData(), err)
	}
	_ = forward.CloseSend()
	_ = clientConnection.Close()
	if err := supervisor.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := grpcPeer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func loopbackEndpoint(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()
	var endpoint struct {
		ID    string           `json:"id"`
		Addrs []map[string]any `json:"addrs"`
	}
	if err := json.Unmarshal(raw, &endpoint); err != nil || len(endpoint.Addrs) == 0 {
		t.Fatalf("endpoint address = %s, %v", raw, err)
	}
	_, port, err := net.SplitHostPort(endpoint.Addrs[0]["Ip"].(string))
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Addrs = append(endpoint.Addrs, map[string]any{"Ip": net.JoinHostPort("127.0.0.1", port)})
	value, err := json.Marshal(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func startUnixEcho(t *testing.T, path string) {
	t.Helper()
	listener, err := net.Listen("unix", path)
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
			go func() {
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()
}

type grpcServiceEcho struct {
	servicev1.UnimplementedServiceRouteServiceServer
}

func (grpcServiceEcho) OpenServiceForward(stream servicev1.ServiceRouteService_OpenServiceForwardServer) error {
	first, err := stream.Recv()
	if err != nil || first.GetHeader().GetRouteGeneration() != 7 {
		return errors.New("complete route header is required")
	}
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if data := request.GetData(); len(data) > 0 {
			return stream.Send(&servicev1.OpenServiceForwardResponse{Payload: &servicev1.OpenServiceForwardResponse_Data{Data: data}})
		}
	}
}

func startGRPCServiceEcho(t *testing.T, path string) {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	servicev1.RegisterServiceRouteServiceServer(server, grpcServiceEcho{})
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	go func() { _ = server.Serve(listener) }()
}
