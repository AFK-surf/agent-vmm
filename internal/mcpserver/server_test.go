package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeRuntime struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	mu         sync.Mutex
	requestIDs []string
}

func (*fakeRuntime) GetEnvironment(context.Context, *hostv1.GetOwnEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	return &hostv1.GetEnvironmentResponse{Environment: &hostv1.AgentEnvironment{Origin: hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL}}, nil
}

func (*fakeRuntime) ListContainers(context.Context, *hostv1.ListContainersRequest) (*hostv1.ListContainersResponse, error) {
	return &hostv1.ListContainersResponse{Containers: []*hostv1.Container{{Id: "z", State: hostv1.ContainerState_CONTAINER_STATE_STOPPED}, {Id: "a", State: hostv1.ContainerState_CONTAINER_STATE_RUNNING}}}, nil
}

func (*fakeRuntime) WaitContainer(ctx context.Context, _ *hostv1.ContainerRequest) (*hostv1.WaitContainerResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (server *fakeRuntime) record(id string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.requestIDs = append(server.requestIDs, id)
}
func (server *fakeRuntime) PullImage(request *hostv1.PullImageRequest, stream grpc.ServerStreamingServer[hostv1.PullImageResponse]) error {
	server.record(request.GetRequestId())
	digest := "sha256:test"
	return stream.Send(&hostv1.PullImageResponse{Status: "complete", Digest: &digest, LogicalBytes: 42})
}
func (server *fakeRuntime) CreateContainer(_ context.Context, request *hostv1.CreateContainerRequest) (*hostv1.ContainerResponse, error) {
	server.record(request.GetRequestId())
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: request.GetContainerId(), Image: request.GetImage(), State: hostv1.ContainerState_CONTAINER_STATE_CREATED}}, nil
}
func (server *fakeRuntime) StartContainer(_ context.Context, request *hostv1.ContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	server.record(request.GetRequestId())
	if request.GetContainerId() == "partial" {
		return nil, status.Error(codes.FailedPrecondition, "cannot start")
	}
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: request.GetContainerId(), State: hostv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}
func (server *fakeRuntime) StopContainer(_ context.Context, request *hostv1.StopContainerRequest) (*hostv1.ContainerResponse, error) {
	server.record(request.GetRequestId())
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: request.GetContainerId(), State: hostv1.ContainerState_CONTAINER_STATE_STOPPED}}, nil
}
func (server *fakeRuntime) SignalContainer(_ context.Context, request *hostv1.SignalContainerRequest) (*emptypb.Empty, error) {
	server.record(request.GetRequestId())
	return &emptypb.Empty{}, nil
}
func (server *fakeRuntime) DeleteContainer(_ context.Context, request *hostv1.ContainerMutationRequest) (*emptypb.Empty, error) {
	server.record(request.GetRequestId())
	return &emptypb.Empty{}, nil
}
func (server *fakeRuntime) CreateVolume(_ context.Context, request *hostv1.CreateVolumeRequest) (*hostv1.VolumeResponse, error) {
	server.record(request.GetRequestId())
	return &hostv1.VolumeResponse{Volume: &hostv1.Volume{Id: request.GetVolumeId()}}, nil
}
func (server *fakeRuntime) DeleteVolume(_ context.Context, request *hostv1.DeleteVolumeRequest) (*emptypb.Empty, error) {
	server.record(request.GetRequestId())
	return &emptypb.Empty{}, nil
}
func (server *fakeRuntime) Exec(stream grpc.BidiStreamingServer[hostv1.ExecRequest, hostv1.ExecResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	server.record(first.GetStart().GetRequestId())
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if err := stream.Send(&hostv1.ExecResponse{Event: &hostv1.ExecResponse_Stdout{Stdout: []byte{'a', 0xff, 'b'}}}); err != nil {
		return err
	}
	return stream.Send(&hostv1.ExecResponse{Event: &hostv1.ExecResponse_ExitCode{ExitCode: 7}})
}

func (*fakeRuntime) ForwardTCP(stream grpc.BidiStreamingServer[hostv1.ForwardTCPRequest, hostv1.ForwardTCPResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if data := request.GetData(); len(data) > 0 {
			if err := stream.Send(&hostv1.ForwardTCPResponse{Event: &hostv1.ForwardTCPResponse_Data{Data: data}}); err != nil {
				return err
			}
		}
		if request.GetCloseWrite() {
			return stream.Send(&hostv1.ForwardTCPResponse{Event: &hostv1.ForwardTCPResponse_CloseRead{CloseRead: true}})
		}
	}
}

func (*fakeRuntime) PlanResetEnvironment(context.Context, *hostv1.GetOwnEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	return &hostv1.CleanupPlanResponse{Plan: &hostv1.CleanupPlan{Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 7}}, nil
}
func (*fakeRuntime) PlanDiscardEnvironment(context.Context, *hostv1.GetOwnEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	return &hostv1.CleanupPlanResponse{Plan: &hostv1.CleanupPlan{Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, Revision: 8, Impacts: []*hostv1.CleanupImpact{{Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "b"}, Irreversible: true}, {Resource: &hostv1.ResourceReference{Kind: "volume", ResourceId: "a"}, Irreversible: true}}}}, nil
}
func (server *fakeRuntime) RequestReset(_ context.Context, request *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error) {
	server.record(request.GetRequestId())
	return &hostv1.OperationResponse{Operation: &hostv1.Operation{Id: "operation", Kind: "reset", State: hostv1.OperationState_OPERATION_STATE_RUNNING}}, nil
}
func (server *fakeRuntime) RequestDiscard(_ context.Context, request *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error) {
	server.record(request.GetRequestId())
	return &hostv1.OperationResponse{Operation: &hostv1.Operation{Id: "operation", Kind: "discard", State: hostv1.OperationState_OPERATION_STATE_RUNNING}}, nil
}

func runtimeClient(t *testing.T) hostv1.AgentRuntimeServiceClient {
	return runtimeClientFor(t, &fakeRuntime{})
}

func runtimeClientFor(t *testing.T, runtime hostv1.AgentRuntimeServiceServer) hostv1.AgentRuntimeServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	hostv1.RegisterAgentRuntimeServiceServer(server, runtime)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return hostv1.NewAgentRuntimeServiceClient(connection)
}

func mcpClient(t *testing.T, adapter *Adapter) *mcp.ClientSession {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = adapter.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "contract-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func TestOfficialClientDiscoveryAndCalls(t *testing.T) {
	client := mcpClient(t, New(runtimeClient(t), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listed, err := client.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		names = append(names, tool.Name)
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(schema) {
			t.Fatalf("invalid schema for %s", tool.Name)
		}
		combined, err := json.Marshal([]any{tool.InputSchema, tool.OutputSchema})
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"environment_id", "namespace", "socket_path", "request_id", "session_id"} {
			if strings.Contains(string(combined), forbidden) {
				t.Fatalf("%s schema exposes %s", tool.Name, forbidden)
			}
		}
	}
	slices.Sort(names)
	want := []string{"cleanup_execute", "cleanup_plan", "container_delete", "container_exec", "container_get", "container_list", "container_logs", "container_run", "container_signal", "container_start", "container_stop", "container_wait", "environment_get", "image_pull", "port_forward_list", "port_forward_start", "port_forward_stop", "volume_create", "volume_delete", "volume_list"}
	if !slices.Equal(names, want) {
		t.Fatalf("tools = %v", names)
	}
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "environment_get", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool error: %#v", result.Content)
	}
	payload, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var envelope Envelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.OK {
		t.Fatalf("result = %s", payload)
	}
	page, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "container_list", Arguments: map[string]any{"limit": 1}})
	if err != nil || page.IsError {
		t.Fatalf("container_list: %v %#v", err, page)
	}
}

func TestInputSchemaRejectsUnknownAndOversizeFields(t *testing.T) {
	client := mcpClient(t, New(runtimeClient(t), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, arguments := range []map[string]any{{"unknown": true}, {"container_id": "bad/id"}} {
		name := "environment_get"
		if _, ok := arguments["container_id"]; ok {
			name = "container_get"
		}
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
		if err != nil {
			t.Fatal(err)
		}
		if !result.IsError {
			t.Fatalf("%s accepted invalid input %#v", name, arguments)
		}
	}
}

func TestErrorMappingDoesNotExposeStatusMessage(t *testing.T) {
	errorValue := mapError(context.DeadlineExceeded)
	if errorValue.Code != "TIMEOUT" || !errorValue.Retryable || errorValue.Message != "request timed out" {
		t.Fatalf("mapped = %#v", errorValue)
	}
}

func TestSerializedResultCapAndUnicodeSanitation(t *testing.T) {
	adapter := New(runtimeClient(t), nil)
	result, envelope, err := adapter.call(context.Background(), time.Second, false, func(context.Context) (any, error) {
		return strings.Repeat("x", maxResultBytes), nil
	})
	if err != nil || result == nil || !result.IsError || envelope.Error == nil || envelope.Error.Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("oversize result = %#v %#v %v", result, envelope, err)
	}
	invalid := string([]byte{'a', 0xff, 'b'})
	clean := strings.ToValidUTF8(invalid, "�")
	if !json.Valid([]byte(`"`+clean+`"`)) || validPrefix("世a", 2) != "" {
		t.Fatal("invalid UTF-8 sanitation")
	}
}

func TestOfficialClientCancellationAndShutdown(t *testing.T) {
	client := mcpClient(t, New(runtimeClient(t), nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "container_wait", Arguments: map[string]any{"container_id": "job"}})
		if err == nil && (result == nil || !result.IsError) {
			t.Errorf("canceled call returned success")
		}
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled call did not return")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMutationLifecycleAndGeneratedRequestIDs(t *testing.T) {
	runtime := &fakeRuntime{}
	client := mcpClient(t, New(runtimeClientFor(t, runtime), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := []struct {
		name      string
		arguments map[string]any
	}{
		{name: "image_pull", arguments: map[string]any{"reference": "example.invalid/image:test"}},
		{name: "container_run", arguments: map[string]any{"container_id": "job", "image": "example.invalid/image:test", "argv": []string{"/bin/true"}}},
		{name: "container_start", arguments: map[string]any{"container_id": "job"}},
		{name: "container_exec", arguments: map[string]any{"container_id": "job", "argv": []string{"printf", "ok"}}},
		{name: "container_stop", arguments: map[string]any{"container_id": "job", "timeout_seconds": 1}},
		{name: "container_signal", arguments: map[string]any{"container_id": "job", "signal": "TERM"}},
		{name: "container_delete", arguments: map[string]any{"container_id": "job"}},
		{name: "volume_create", arguments: map[string]any{"volume_id": "cache"}},
		{name: "volume_delete", arguments: map[string]any{"volume_id": "cache"}},
	}
	for _, call := range calls {
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: call.name, Arguments: call.arguments})
		if err != nil || result.IsError {
			t.Fatalf("%s: %v %#v", call.name, err, result)
		}
	}
	partial, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "container_run", Arguments: map[string]any{"container_id": "partial", "image": "image", "argv": []string{"true"}}})
	if err != nil || !partial.IsError {
		t.Fatalf("partial run = %v %#v", err, partial)
	}
	runtime.mu.Lock()
	ids := append([]string(nil), runtime.requestIDs...)
	runtime.mu.Unlock()
	seen := make(map[string]bool)
	for _, id := range ids {
		if !strings.HasPrefix(id, "mcp-") || seen[id] {
			t.Fatalf("invalid generated request IDs: %v", ids)
		}
		seen[id] = true
	}
}

func TestBoundedTextPreservesHeadTailAndSanitizes(t *testing.T) {
	buffer := newBoundedText(8)
	buffer.Write([]byte("abcd"))
	buffer.Write([]byte("middle"))
	buffer.Write([]byte("WXYZ"))
	if got := buffer.String(); got != "abcdWXYZ" || !buffer.truncated {
		t.Fatalf("bounded output = %q, %v", got, buffer.truncated)
	}
	buffer = newBoundedText(8)
	buffer.Write([]byte{'a', 0xff, 'b'})
	if strings.ContainsRune(buffer.String(), utf8.RuneError) == false {
		t.Fatal("binary output was not replaced")
	}
}

func TestLoopbackForwardLifecycleAndHalfClose(t *testing.T) {
	adapter := New(runtimeClient(t), nil)
	client := mcpClient(t, adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "port_forward_start", Arguments: map[string]any{"container_id": "service", "port": 8080, "ttl_seconds": 10}})
	if err != nil || started.IsError {
		t.Fatalf("start = %v %#v", err, started)
	}
	var envelope struct {
		OK   bool        `json:"ok"`
		Data ForwardData `json:"data"`
	}
	payload, _ := json.Marshal(started.StructuredContent)
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.OK || envelope.Data.LocalPort == 0 {
		t.Fatalf("start = %s", payload)
	}
	connection, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(envelope.Data.LocalPort))), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tcp := connection.(*net.TCPConn)
	if _, err := tcp.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := tcp.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(tcp)
	tcp.Close()
	if err != nil || string(response) != "ping" {
		t.Fatalf("forward response = %q, %v", response, err)
	}
	listed, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "port_forward_list", Arguments: map[string]any{}})
	if err != nil || listed.IsError {
		t.Fatalf("list = %v %#v", err, listed)
	}
	stopped, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "port_forward_stop", Arguments: map[string]any{"forward_id": envelope.Data.ID}})
	if err != nil || stopped.IsError {
		t.Fatalf("stop = %v %#v", err, stopped)
	}
	if _, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(envelope.Data.LocalPort))), 100*time.Millisecond); err == nil {
		t.Fatal("stopped forward still accepts connections")
	}
}

func TestForwardExpiresAndProcessCloseReclaimsListener(t *testing.T) {
	for _, test := range []struct {
		name   string
		expire bool
	}{{name: "ttl", expire: true}, {name: "process_close"}} {
		t.Run(test.name, func(t *testing.T) {
			adapter := New(runtimeClient(t), nil)
			client := mcpClient(t, adapter)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "port_forward_start", Arguments: map[string]any{"container_id": "service", "port": 8080, "ttl_seconds": 1}})
			if err != nil || result.IsError {
				t.Fatalf("start = %v %#v", err, result)
			}
			var envelope struct {
				Data ForwardData `json:"data"`
			}
			payload, _ := json.Marshal(result.StructuredContent)
			if err := json.Unmarshal(payload, &envelope); err != nil {
				t.Fatal(err)
			}
			if test.expire {
				time.Sleep(1200 * time.Millisecond)
			} else {
				adapter.Close()
			}
			address := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(envelope.Data.LocalPort)))
			if connection, err := net.DialTimeout("tcp4", address, 100*time.Millisecond); err == nil {
				connection.Close()
				t.Fatal("reclaimed listener accepted a connection")
			}
		})
	}
}

func TestCleanupReplansConfirmsAndDrains(t *testing.T) {
	client := mcpClient(t, New(runtimeClient(t), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, arguments := range []map[string]any{
		{"kind": "reset", "plan_revision": "6", "confirm_reset": true},
		{"kind": "discard", "plan_revision": "8", "confirm_irreversible": true, "confirmed_volume_ids": []string{"a"}},
	} {
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "cleanup_execute", Arguments: arguments})
		if err != nil || !result.IsError {
			t.Fatalf("invalid cleanup = %v %#v", err, result)
		}
	}
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "cleanup_execute", Arguments: map[string]any{"kind": "discard", "plan_revision": "8", "confirm_irreversible": true, "confirmed_volume_ids": []string{"b", "a"}}})
	if err != nil || result.IsError {
		t.Fatalf("cleanup = %v %#v", err, result)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := client.ListTools(ctx, nil); err == nil {
		t.Fatal("server did not drain after cleanup")
	}
}
