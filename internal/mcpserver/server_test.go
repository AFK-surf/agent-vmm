package mcpserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeRuntime struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	mu                sync.Mutex
	createdContainers []*hostv1.CreateContainerRequest
	requestIDs        []string
	containerState    hostv1.ContainerState
	containerInstance string
	containerError    error
	deletedGeneration string
	deletedImage      *hostv1.DeleteImageRequest
	createdExport     *hostv1.CreateServiceExportRequest
	createdImport     *hostv1.CreateServiceImportRequest
	createdPersonal   *hostv1.CreatePersonalServiceImportRequest
	revokedExport     *hostv1.RevokeOwnServiceExportRequest
	revokedImport     *hostv1.RevokeServiceImportRequest
}

type fakeCleanupOwner struct {
	mu         sync.Mutex
	executions []CleanupExecution
}

func (*fakeCleanupOwner) PlanCleanup(_ context.Context, kind string) (CleanupPlan, error) {
	if kind == "reset" {
		return CleanupPlan{Kind: "reset", Revision: 7}, nil
	}
	return CleanupPlan{Kind: "discard", Revision: 8, VolumeIDs: []string{"b", "a"}, Impacts: []CleanupImpact{{Resource: Resource{Kind: "volume", ID: "b"}, Effect: "delete", Irreversible: true}, {Resource: Resource{Kind: "volume", ID: "a"}, Effect: "delete", Irreversible: true}}}, nil
}

func (owner *fakeCleanupOwner) ExecuteCleanup(_ context.Context, execution CleanupExecution) error {
	owner.mu.Lock()
	owner.executions = append(owner.executions, execution)
	owner.mu.Unlock()
	return nil
}

func newTestAdapter(t *testing.T, runtime hostv1.AgentRuntimeServiceClient) *Adapter {
	t.Helper()
	return New(runtime, &fakeCleanupOwner{}, nil)
}

func (server *fakeRuntime) CreatePersonalServiceImport(_ context.Context, request *hostv1.CreatePersonalServiceImportRequest) (*servicev1.ServiceImport, error) {
	server.mu.Lock()
	server.createdPersonal = proto.Clone(request).(*hostv1.CreatePersonalServiceImportRequest)
	server.mu.Unlock()
	return &servicev1.ServiceImport{ImportId: "personal-import", SourceContainerId: request.GetSourceContainerId(), DestinationDeviceId: request.GetDestinationDeviceId(), DestinationExportId: request.GetDestinationExportId(), VirtualServiceName: request.GetVirtualServiceName(), Revision: 1}, nil
}

func (*fakeRuntime) ListServiceExports(context.Context, *hostv1.ListOwnServiceExportsRequest) (*servicev1.ListServiceExportsResponse, error) {
	return &servicev1.ListServiceExportsResponse{Exports: []*servicev1.ServiceExport{{ExportId: "export-current", ContainerId: "api", ContainerInstanceId: "api-v1", Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP, Revision: 3}}}, nil
}

func TestPersonalMeshServiceImportUsesHostOwnedRouteRequest(t *testing.T) {
	runtime := &fakeRuntime{}
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "service_import", Arguments: map[string]any{
		"source_container_id": "worker", "source_container_instance_id": "worker-v3", "destination_device_id": "device-c",
		"destination_export_id": "export-c", "virtual_service_name": "mesh-api", "mesh_id": "mesh-a",
		"connection_limit": 4, "byte_limit": 1048576, "concurrency_limit": 2,
	}})
	if err != nil || result.IsError {
		t.Fatalf("personal service_import: err=%v result=%#v", err, result)
	}
	runtime.mu.Lock()
	request := runtime.createdPersonal
	runtime.mu.Unlock()
	if request.GetRequestId() == "" || request.GetMeshId() != "mesh-a" || request.GetDestinationDeviceId() != "device-c" ||
		request.GetBudget().GetConnectionLimit() != 4 || request.GetBudget().GetByteLimit() != 1048576 || request.GetBudget().GetConcurrencyLimit() != 2 {
		t.Fatalf("personal import request = %#v", request)
	}
}

func (*fakeRuntime) ListServiceImports(context.Context, *hostv1.ListServiceImportsRequest) (*hostv1.ListServiceImportsResponse, error) {
	return &hostv1.ListServiceImportsResponse{Imports: []*servicev1.ServiceImport{{ImportId: "import-current", SourceContainerId: "worker", SourceContainerInstanceId: "worker-v1", DestinationDeviceId: "device-b", DestinationExportId: "export-b", VirtualServiceName: "peer-api", Revision: 5}}}, nil
}

func (server *fakeRuntime) CreateServiceExport(_ context.Context, request *hostv1.CreateServiceExportRequest) (*servicev1.ServiceExport, error) {
	server.mu.Lock()
	server.createdExport = proto.Clone(request).(*hostv1.CreateServiceExportRequest)
	server.mu.Unlock()
	return &servicev1.ServiceExport{ExportId: "export-new", ContainerId: request.GetContainerId(), ContainerInstanceId: request.GetContainerInstanceId(), Port: request.GetPort(), Protocol: request.GetProtocol(), Revision: 1}, nil
}

func (server *fakeRuntime) CreateServiceImport(_ context.Context, request *hostv1.CreateServiceImportRequest) (*servicev1.ServiceImport, error) {
	server.mu.Lock()
	server.createdImport = proto.Clone(request).(*hostv1.CreateServiceImportRequest)
	server.mu.Unlock()
	return &servicev1.ServiceImport{ImportId: "import-new", SourceContainerId: request.GetSourceContainerId(), SourceContainerInstanceId: request.GetSourceContainerInstanceId(), DestinationDeviceId: request.GetDestinationDeviceId(), DestinationExportId: request.GetDestinationExportId(), DestinationPort: request.GetDestinationPort(), VirtualServiceName: request.GetVirtualServiceName(), Revision: 1}, nil
}

func (server *fakeRuntime) RevokeServiceExport(_ context.Context, request *hostv1.RevokeOwnServiceExportRequest) (*emptypb.Empty, error) {
	server.mu.Lock()
	server.revokedExport = proto.Clone(request).(*hostv1.RevokeOwnServiceExportRequest)
	server.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (server *fakeRuntime) RevokeServiceImport(_ context.Context, request *hostv1.RevokeServiceImportRequest) (*emptypb.Empty, error) {
	server.mu.Lock()
	server.revokedImport = proto.Clone(request).(*hostv1.RevokeServiceImportRequest)
	server.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (*fakeRuntime) GetEnvironment(context.Context, *hostv1.GetOwnEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	return &hostv1.GetEnvironmentResponse{Environment: &hostv1.AgentEnvironment{Origin: hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL}}, nil
}

func (*fakeRuntime) ListContainers(context.Context, *hostv1.ListContainersRequest) (*hostv1.ListContainersResponse, error) {
	return &hostv1.ListContainersResponse{Containers: []*hostv1.Container{{Id: "z", State: hostv1.ContainerState_CONTAINER_STATE_STOPPED}, {Id: "a", State: hostv1.ContainerState_CONTAINER_STATE_RUNNING, InstanceId: "instance-a"}}}, nil
}

func (server *fakeRuntime) GetContainer(_ context.Context, request *hostv1.ContainerRequest) (*hostv1.ContainerResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.containerError != nil {
		return nil, server.containerError
	}
	state := server.containerState
	if state == hostv1.ContainerState_CONTAINER_STATE_UNSPECIFIED {
		state = hostv1.ContainerState_CONTAINER_STATE_RUNNING
	}
	instance := server.containerInstance
	if instance == "" && state == hostv1.ContainerState_CONTAINER_STATE_RUNNING {
		instance = "instance-" + request.GetContainerId()
	}
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: request.GetContainerId(), State: state, InstanceId: instance, GenerationId: "generation-" + request.GetContainerId()}}, nil
}

func (*fakeRuntime) ListImages(context.Context, *hostv1.ListImagesRequest) (*hostv1.ListImagesResponse, error) {
	return &hostv1.ListImagesResponse{Images: []*hostv1.Image{{Reference: "docker.io/library/test:latest", Digest: "sha256:test", GenerationId: "image-generation", LogicalBytes: 42}}}, nil
}

func (server *fakeRuntime) DeleteImage(_ context.Context, request *hostv1.DeleteImageRequest) (*emptypb.Empty, error) {
	server.record(request.GetRequestId())
	server.mu.Lock()
	server.deletedImage = request
	server.mu.Unlock()
	return &emptypb.Empty{}, nil
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
	server.mu.Lock()
	server.createdContainers = append(server.createdContainers, proto.Clone(request).(*hostv1.CreateContainerRequest))
	server.mu.Unlock()
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: request.GetContainerId(), Image: request.GetImage(), State: hostv1.ContainerState_CONTAINER_STATE_CREATED, GenerationId: "generation-" + request.GetContainerId()}}, nil
}
func (server *fakeRuntime) StartContainer(_ context.Context, request *hostv1.ContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	server.record(request.GetRequestId())
	// The Host starts only the exact generation that the caller observed.
	if request.GetExpectedGenerationId() != "generation-"+request.GetContainerId() {
		return nil, status.Error(codes.InvalidArgument, "request validation failed")
	}
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
	server.mu.Lock()
	server.deletedGeneration = request.GetExpectedGenerationId()
	server.mu.Unlock()
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
	// The Host admits only an exec activity bound to the current instance.
	if start := first.GetStart(); start.GetExecutionId() == "" || start.GetExpectedInstanceId() != "instance-"+start.GetContainerId() ||
		start.GetExecutionKind() != hostv1.ExecutionKind_EXECUTION_KIND_EXEC || start.GetExecutionDeadlineUnixNano() <= time.Now().UnixNano() {
		return status.Error(codes.InvalidArgument, "request validation failed")
	}
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
	client := mcpClient(t, newTestAdapter(t, runtimeClient(t)))
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
	want := []string{"cleanup_execute", "cleanup_plan", "container_delete", "container_exec", "container_get", "container_list", "container_logs", "container_run", "container_signal", "container_start", "container_stop", "container_wait", "environment_get", "image_delete", "image_list", "image_pull", "mesh_service_list", "service_export", "service_import", "service_route_revoke", "volume_create", "volume_delete", "volume_list"}
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

func TestServiceToolsUseOnlyBoundRuntimeAndPreserveCapability(t *testing.T) {
	runtime := &fakeRuntime{}
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, call := range []*mcp.CallToolParams{
		{Name: "mesh_service_list", Arguments: map[string]any{}},
		{Name: "service_export", Arguments: map[string]any{"container_id": "api", "container_instance_id": "api-v2", "port": 8443, "protocol": "http"}},
	} {
		result, err := client.CallTool(ctx, call)
		if err != nil || result.IsError {
			t.Fatalf("%s: err=%v result=%#v", call.Name, err, result)
		}
	}

	capability := &trustv1.RouteCapability{CapabilityId: "capability-current", RouteGeneration: 7}
	encoded, err := proto.Marshal(capability)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "service_import", Arguments: map[string]any{
		"source_container_id": "worker", "source_container_instance_id": "worker-v2",
		"destination_device_id": "device-b", "destination_export_id": "export-b", "destination_port": 8080,
		"virtual_service_name": "peer-api", "capability": base64.RawURLEncoding.EncodeToString(encoded),
	}})
	if err != nil || result.IsError {
		t.Fatalf("service_import: err=%v result=%#v", err, result)
	}
	for _, arguments := range []map[string]any{
		{"kind": "export", "id": "export-new", "expected_revision": 1},
		{"kind": "import", "id": "import-new", "expected_revision": 2},
	} {
		result, err = client.CallTool(ctx, &mcp.CallToolParams{Name: "service_route_revoke", Arguments: arguments})
		if err != nil || result.IsError {
			t.Fatalf("service_route_revoke: err=%v result=%#v", err, result)
		}
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.createdExport.GetRequestId() == "" || runtime.createdExport.GetContainerId() != "api" || runtime.createdExport.GetContainerInstanceId() != "api-v2" || runtime.createdExport.GetPort() != 8443 {
		t.Fatalf("created export = %#v", runtime.createdExport)
	}
	if runtime.createdImport.GetRequestId() == "" || runtime.createdImport.GetSourceContainerId() != "worker" || runtime.createdImport.GetSourceContainerInstanceId() != "worker-v2" || !proto.Equal(runtime.createdImport.GetCapability(), capability) {
		t.Fatalf("created import = %#v", runtime.createdImport)
	}
	if runtime.revokedExport.GetExportId() != "export-new" || runtime.revokedExport.GetExpectedRevision() != 1 || runtime.revokedExport.GetRequestId() == "" {
		t.Fatalf("revoked export = %#v", runtime.revokedExport)
	}
	if runtime.revokedImport.GetImportId() != "import-new" || runtime.revokedImport.GetExpectedRevision() != 2 || runtime.revokedImport.GetRequestId() == "" {
		t.Fatalf("revoked import = %#v", runtime.revokedImport)
	}
}

func TestServiceImportLocalShortcutOmitsCapability(t *testing.T) {
	runtime := &fakeRuntime{}
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "service_import", Arguments: map[string]any{
		"source_container_id": "worker", "source_container_instance_id": "worker-v2",
		"destination_device_id": "local", "destination_export_id": "export-local", "virtual_service_name": "local-api",
	}})
	if err != nil || result.IsError {
		t.Fatalf("local service_import: err=%v result=%#v", err, result)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.createdImport == nil || runtime.createdImport.GetDestinationDeviceId() != "local" || runtime.createdImport.GetCapability() != nil || runtime.createdImport.GetDestinationPort() != 0 {
		t.Fatalf("created local import = %#v", runtime.createdImport)
	}
}

func TestServiceImportRemoteRequiresCapability(t *testing.T) {
	runtime := &fakeRuntime{}
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "service_import", Arguments: map[string]any{
		"source_container_id": "worker", "source_container_instance_id": "worker-v2",
		"destination_device_id": "device-b", "destination_export_id": "export-b", "destination_port": 8080,
		"virtual_service_name": "peer-api",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("remote service_import result = %#v", result)
	}
	payload, marshalErr := json.Marshal(result.StructuredContent)
	if marshalErr != nil || !strings.Contains(string(payload), "capability is required") {
		t.Fatalf("remote service_import error = %s", payload)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.createdImport != nil {
		t.Fatalf("remote import was sent without capability: %#v", runtime.createdImport)
	}
}

func TestImageDeleteUsesCurrentFencedIdentity(t *testing.T) {
	runtime := &fakeRuntime{}
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "image_delete", Arguments: map[string]any{"reference": "docker.io/library/test:latest"}})
	if err != nil || result.IsError {
		t.Fatalf("image_delete: %v %#v", err, result)
	}
	runtime.mu.Lock()
	deleted := runtime.deletedImage
	runtime.mu.Unlock()
	if deleted == nil || deleted.GetExpectedGenerationId() != "image-generation" || deleted.GetExpectedDigest() != "sha256:test" || deleted.GetRequestId() == "" {
		t.Fatalf("delete request = %+v", deleted)
	}
}

func TestInputSchemaRejectsUnknownAndOversizeFields(t *testing.T) {
	client := mcpClient(t, newTestAdapter(t, runtimeClient(t)))
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

func TestErrorMappingIncludesInvalidArgumentFields(t *testing.T) {
	value, err := status.New(codes.InvalidArgument, "request validation failed").WithDetails(&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "destination_port", Description: "must be omitted for a local route"}}})
	if err != nil {
		t.Fatal(err)
	}
	mapped := mapError(value.Err())
	if mapped.Code != "INVALID_ARGUMENT" || mapped.Message != "request validation failed: destination_port: must be omitted for a local route" {
		t.Fatalf("mapped = %#v", mapped)
	}
}

func TestErrorMappingPreservesPIDLimitReason(t *testing.T) {
	value, err := status.New(codes.ResourceExhausted, "environment process limit was exhausted").WithDetails(&errdetails.ErrorInfo{Reason: "PID_LIMIT_EXCEEDED", Domain: "agent-vmm"})
	if err != nil {
		t.Fatal(err)
	}
	mapped := mapError(value.Err())
	if mapped.Code != "PID_LIMIT_EXCEEDED" || mapped.Message != "environment process limit was exhausted" {
		t.Fatalf("mapped = %#v", mapped)
	}
}

func TestErrorMappingNamesBareInternalFailures(t *testing.T) {
	mapped := mapError(status.Error(codes.Internal, "sqlite failure"))
	if mapped.Code != "RUNTIME_OPERATION_FAILED" || mapped.Message != "runtime operation failed" {
		t.Fatalf("mapped = %#v", mapped)
	}
	value, err := status.New(codes.Internal, "service import could not be recorded").WithDetails(&errdetails.ErrorInfo{Reason: "SERVICE_IMPORT_PERSIST_FAILED", Domain: "agent-vmm"})
	if err != nil {
		t.Fatal(err)
	}
	mapped = mapError(value.Err())
	if mapped.Code != "SERVICE_IMPORT_PERSIST_FAILED" || mapped.Message != "service import could not be recorded" {
		t.Fatalf("structured mapped = %#v", mapped)
	}
	mapped = mapError(errors.New("untyped host failure"))
	if mapped.Code != "RUNTIME_OPERATION_FAILED" || mapped.Message != "runtime operation failed" {
		t.Fatalf("unknown mapped = %#v", mapped)
	}
}

func TestExecReservationLeavesControlCapacity(t *testing.T) {
	adapter := newTestAdapter(t, runtimeClient(t))
	for index := 0; index < cap(adapter.execs); index++ {
		adapter.execs <- struct{}{}
	}
	defer func() {
		for index := 0; index < cap(adapter.execs); index++ {
			<-adapter.execs
		}
	}()
	result, envelope, err := adapter.call(context.Background(), time.Second, false, func(context.Context) (any, error) {
		return EmptyData{}, nil
	})
	if err != nil || result != nil || !envelope.OK {
		t.Fatalf("control call while execs are reserved = result=%#v envelope=%#v err=%v", result, envelope, err)
	}
}

func TestSerializedResultCapAndUnicodeSanitation(t *testing.T) {
	adapter := newTestAdapter(t, runtimeClient(t))
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
	client := mcpClient(t, newTestAdapter(t, runtimeClient(t)))
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
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := []struct {
		name      string
		arguments map[string]any
	}{
		{name: "image_pull", arguments: map[string]any{"reference": "example.invalid/image:test"}},
		{name: "container_run", arguments: map[string]any{"container_id": "job", "image": "example.invalid/image:test", "entrypoint": []string{"/bin/true"}, "command": []string{}, "startup_observation_ms": 1, "volumes": []map[string]any{{"owned_volume": "cache", "destination": "/data"}, {"host_share": "approved-share", "destination": "/external", "read_only": true}}}},
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
	partial, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "container_run", Arguments: map[string]any{"container_id": "partial", "image": "image", "entrypoint": []string{"true"}, "command": []string{}, "startup_observation_ms": 1}})
	if err != nil || !partial.IsError {
		t.Fatalf("partial run = %v %#v", err, partial)
	}
	runtime.mu.Lock()
	ids := append([]string(nil), runtime.requestIDs...)
	deletedGeneration := runtime.deletedGeneration
	runtime.mu.Unlock()
	if deletedGeneration != "generation-job" {
		t.Fatalf("delete generation = %q", deletedGeneration)
	}
	mounts := runtime.createdContainers[0].GetVolumes()
	if len(mounts) != 2 || mounts[0].GetOwnedVolume() != "cache" || mounts[1].GetHostShare() != "approved-share" || !mounts[1].GetReadOnly() {
		t.Fatalf("typed mount input was not preserved: %v", mounts)
	}
	seen := make(map[string]bool)
	for _, id := range ids {
		if !strings.HasPrefix(id, "mcp-") || seen[id] {
			t.Fatalf("invalid generated request IDs: %v", ids)
		}
		seen[id] = true
	}
}

func TestContainerRunReportsObservedFastExit(t *testing.T) {
	runtime := &fakeRuntime{containerState: hostv1.ContainerState_CONTAINER_STATE_STOPPED}
	client := mcpClient(t, newTestAdapter(t, runtimeClientFor(t, runtime)))
	result, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: "container_run", Arguments: map[string]any{
		"container_id": "quick", "image": "image", "entrypoint": []string{"true"}, "command": []string{}, "startup_observation_ms": 1,
	}})
	if err != nil || result.IsError {
		t.Fatalf("container_run = %v %#v", err, result)
	}
	payload, _ := json.Marshal(result.StructuredContent)
	if !strings.Contains(string(payload), `"launch_succeeded":true`) || !strings.Contains(string(payload), `"state":"stopped"`) {
		t.Fatalf("container_run = %s", payload)
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

func TestMapErrorPreservesSanitizedRuntimeDetails(t *testing.T) {
	value, err := status.New(codes.FailedPrecondition, "image content could not be unpacked").WithDetails(
		&errdetails.ErrorInfo{Reason: "IMAGE_UNPACK_FAILED", Domain: "agent-vmm"},
		&errdetails.ResourceInfo{ResourceType: "image", ResourceName: "docker.io/pgvector/pgvector:pg16"},
	)
	if err != nil {
		t.Fatal(err)
	}
	mapped := mapError(value.Err())
	if mapped.Code != "IMAGE_UNPACK_FAILED" || mapped.Message != "image content could not be unpacked" || mapped.Resource == nil || mapped.Resource.Kind != "image" || mapped.Resource.ID != "docker.io/pgvector/pgvector:pg16" {
		t.Fatalf("mapped error = %+v", mapped)
	}
}

func TestMapErrorPreservesImageDeletionConflicts(t *testing.T) {
	for _, reason := range []string{"IMAGE_IN_USE", "IMAGE_GENERATION_CONFLICT", "IMAGE_DIGEST_CONFLICT"} {
		value, err := status.New(codes.Aborted, "image record changed; refresh and try again").WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "agent-vmm"})
		if err != nil {
			t.Fatal(err)
		}
		mapped := mapError(value.Err())
		if mapped.Code != reason || mapped.Message != "image record changed; refresh and try again" {
			t.Fatalf("%s mapped to %+v", reason, mapped)
		}
	}
}

func TestMapErrorPreservesRetryableRegistryRateLimit(t *testing.T) {
	value, err := status.New(codes.ResourceExhausted, "image registry rate limit was reached").WithDetails(
		&errdetails.ErrorInfo{Reason: "IMAGE_REGISTRY_RATE_LIMITED", Domain: "agent-vmm"},
		&errdetails.ResourceInfo{ResourceType: "image", ResourceName: "docker.io/pgvector/pgvector:pg16"},
	)
	if err != nil {
		t.Fatal(err)
	}
	mapped := mapError(value.Err())
	if mapped.Code != "IMAGE_REGISTRY_RATE_LIMITED" || mapped.Message != "image registry rate limit was reached" || !mapped.Retryable {
		t.Fatalf("mapped error = %+v", mapped)
	}
}

func TestCleanupReplansConfirmsAndDrains(t *testing.T) {
	runtime := &fakeRuntime{}
	cleanup := &fakeCleanupOwner{}
	client := mcpClient(t, New(runtimeClientFor(t, runtime), cleanup, nil))
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
	cleanup.mu.Lock()
	executions := append([]CleanupExecution(nil), cleanup.executions...)
	cleanup.mu.Unlock()
	runtime.mu.Lock()
	runtimeRequests := append([]string(nil), runtime.requestIDs...)
	runtime.mu.Unlock()
	if len(executions) != 1 || executions[0].Kind != "discard" || executions[0].Revision != 8 || !slices.Equal(executions[0].ConfirmedVolumeIDs, []string{"a", "b"}) {
		t.Fatalf("cleanup execution = %#v", executions)
	}
	if len(runtimeRequests) != 0 {
		t.Fatalf("cleanup reached AgentRuntime: %v", runtimeRequests)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := client.ListTools(ctx, nil); err == nil {
		t.Fatal("server did not drain after cleanup")
	}
}

func TestCleanupResetUsesProductProjection(t *testing.T) {
	runtime := &fakeRuntime{}
	cleanup := &fakeCleanupOwner{}
	client := mcpClient(t, New(runtimeClientFor(t, runtime), cleanup, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "cleanup_execute", Arguments: map[string]any{"kind": "reset", "plan_revision": "7", "confirm_reset": true}})
	if err != nil || result.IsError {
		t.Fatalf("cleanup = %v %#v", err, result)
	}
	cleanup.mu.Lock()
	executions := append([]CleanupExecution(nil), cleanup.executions...)
	cleanup.mu.Unlock()
	if len(executions) != 1 || executions[0].Kind != "reset" || executions[0].Revision != 7 || len(executions[0].ConfirmedVolumeIDs) != 0 {
		t.Fatalf("cleanup execution = %#v", executions)
	}
	runtime.mu.Lock()
	runtimeRequests := append([]string(nil), runtime.requestIDs...)
	runtime.mu.Unlock()
	if len(runtimeRequests) != 0 {
		t.Fatalf("cleanup reached AgentRuntime: %v", runtimeRequests)
	}
}
