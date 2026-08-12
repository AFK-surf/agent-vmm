package remoteconnector

import (
	"context"
	"errors"
	"net"
	"testing"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type registrySyncTrustServer struct {
	trustv1.UnimplementedTrustServiceServer
	operations []*trustv1.PendingRegistryOperation
	failCommit bool
	committed  []string
	meshes     []*trustv1.PersonalMeshState
	endpoints  map[string]*trustv1.EndpointObservation
	applied    map[string][]string
}

func (server *registrySyncTrustServer) ListPersonalMeshes(context.Context, *trustv1.ListPersonalMeshesRequest) (*trustv1.ListPersonalMeshesResponse, error) {
	return &trustv1.ListPersonalMeshesResponse{Meshes: server.meshes}, nil
}

func (server *registrySyncTrustServer) GetEndpointObservation(_ context.Context, request *trustv1.GetEndpointObservationRequest) (*trustv1.EndpointObservation, error) {
	if endpoint := server.endpoints[request.GetDeviceId()]; endpoint != nil {
		return endpoint, nil
	}
	return nil, errors.New("not found")
}

func (server *registrySyncTrustServer) ApplyMeshEndpointObservation(_ context.Context, request *trustv1.ApplyMeshEndpointObservationRequest) (*trustv1.EndpointObservation, error) {
	if server.endpoints == nil {
		server.endpoints = make(map[string]*trustv1.EndpointObservation)
	}
	server.endpoints[request.GetObservation().GetDeviceId()] = request.GetObservation()
	if server.applied == nil {
		server.applied = make(map[string][]string)
	}
	server.applied[request.GetMeshId()] = append(server.applied[request.GetMeshId()], request.GetObservation().GetDeviceId())
	return request.GetObservation(), nil
}

func (server *registrySyncTrustServer) PullRegistryOperations(_ context.Context, request *trustv1.PullRegistryOperationsRequest) (*trustv1.PullRegistryOperationsResponse, error) {
	response := &trustv1.PullRegistryOperationsResponse{}
	for _, operation := range server.operations {
		if operation.GetSequence() > request.GetAfterSequence() {
			response.Operations = append(response.Operations, operation)
		}
	}
	return response, nil
}

func (server *registrySyncTrustServer) CommitRegistryOperation(_ context.Context, request *trustv1.CommitRegistryOperationRequest) (*trustv1.ApplyMeshSnapshotResponse, error) {
	if server.failCommit {
		server.failCommit = false
		return nil, errors.New("simulated host crash before receipt commit")
	}
	server.committed = append(server.committed, request.GetOperationId())
	return &trustv1.ApplyMeshSnapshotResponse{AppliedRevision: request.GetSnapshot().GetRevision()}, nil
}

type registrySyncServer struct {
	trustv1.UnimplementedPersonalMeshRegistryServiceServer
	calls     []string
	snapshot  *trustv1.MeshSnapshot
	endpoints []*trustv1.EndpointObservation
	published int
	snapshots map[string]*trustv1.MeshSnapshot
	byMesh    map[string][]*trustv1.EndpointObservation
}

func (server *registrySyncServer) GetSnapshot(_ context.Context, request *trustv1.GetSnapshotRequest) (*trustv1.MeshSnapshot, error) {
	if server.snapshots != nil {
		return server.snapshots[request.GetMeshId()], nil
	}
	return server.snapshot, nil
}

func (server *registrySyncServer) PublishEndpoint(_ context.Context, _ *trustv1.PublishEndpointRequest) (*trustv1.PublishEndpointResponse, error) {
	server.published++
	return &trustv1.PublishEndpointResponse{}, nil
}

func (server *registrySyncServer) ListEndpoints(_ context.Context, request *trustv1.ListMeshEndpointsRequest) (*trustv1.ListMeshEndpointsResponse, error) {
	if server.snapshots != nil {
		return &trustv1.ListMeshEndpointsResponse{Observations: server.byMesh[request.GetMeshId()], MeshRevision: server.snapshots[request.GetMeshId()].GetRevision()}, nil
	}
	return &trustv1.ListMeshEndpointsResponse{Observations: server.endpoints, MeshRevision: server.snapshot.GetRevision()}, nil
}

func (server *registrySyncServer) CreateMesh(_ context.Context, request *trustv1.CreateMeshRequest) (*trustv1.MeshCommitResponse, error) {
	server.calls = append(server.calls, request.GetGenesis().GetOperationId())
	return &trustv1.MeshCommitResponse{Snapshot: &trustv1.MeshSnapshot{MeshId: request.GetGenesis().GetMeshId(), Revision: 1}}, nil
}

func (server *registrySyncServer) CommitRevoke(_ context.Context, request *trustv1.CommitRevokeRequest) (*trustv1.MeshCommitResponse, error) {
	server.calls = append(server.calls, request.GetOperation().GetOperationId())
	return &trustv1.MeshCommitResponse{Snapshot: &trustv1.MeshSnapshot{MeshId: request.GetOperation().GetMeshId(), Revision: request.GetOperation().GetExpectedRevision() + 1}}, nil
}

func TestRegistrySyncRetriesDurableOperationButAdvancesOnlyAfterHostCommit(t *testing.T) {
	trustServer := &registrySyncTrustServer{failCommit: true, operations: []*trustv1.PendingRegistryOperation{
		{Sequence: 1, MeshDescriptor: &trustv1.PersonalMeshDescriptor{MeshId: "mesh"}, Operation: &trustv1.RegistryOperation{
			OperationId: "genesis", Kind: trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS, MeshId: "mesh"}},
		{Sequence: 2, Operation: &trustv1.RegistryOperation{OperationId: "revoke", Kind: trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE,
			MeshId: "mesh", ExpectedRevision: 1}},
	}}
	registryServer := &registrySyncServer{}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	trustv1.RegisterTrustServiceServer(server, trustServer)
	trustv1.RegisterPersonalMeshRegistryServiceServer(server, registryServer)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///registry-sync", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	local := trustv1.NewTrustServiceClient(connection)
	registry := trustv1.NewPersonalMeshRegistryServiceClient(connection)
	cursor, err := SyncPersonalMeshRegistry(context.Background(), local, registry, 0)
	if err == nil || cursor != 0 || len(registryServer.calls) != 1 || len(trustServer.committed) != 0 {
		t.Fatalf("ambiguous first sync cursor=%d calls=%v committed=%v err=%v", cursor, registryServer.calls, trustServer.committed, err)
	}
	cursor, err = SyncPersonalMeshRegistry(context.Background(), local, registry, cursor)
	if err != nil || cursor != 2 {
		t.Fatalf("retry sync cursor=%d err=%v", cursor, err)
	}
	if got := registryServer.calls; len(got) != 3 || got[0] != "genesis" || got[1] != "genesis" || got[2] != "revoke" {
		t.Fatalf("registry call order=%v", got)
	}
	if got := trustServer.committed; len(got) != 2 || got[0] != "genesis" || got[1] != "revoke" {
		t.Fatalf("host commit order=%v", got)
	}
}

func TestPersonalMeshSyncPublishesLocalAndAppliesRemoteEndpoint(t *testing.T) {
	meshID := "mesh"
	snapshot := &trustv1.MeshSnapshot{MeshId: meshID, Revision: 4}
	localEndpoint := &trustv1.EndpointObservation{DeviceId: "device-a", EndpointGeneration: 7}
	remoteEndpoint := &trustv1.EndpointObservation{DeviceId: "device-b", EndpointGeneration: 9}
	trustServer := &registrySyncTrustServer{meshes: []*trustv1.PersonalMeshState{{
		MeshDescriptor: &trustv1.PersonalMeshDescriptor{MeshId: meshID}, Snapshot: snapshot,
	}}, endpoints: make(map[string]*trustv1.EndpointObservation)}
	registryServer := &registrySyncServer{snapshot: snapshot, endpoints: []*trustv1.EndpointObservation{localEndpoint, remoteEndpoint}}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	trustv1.RegisterTrustServiceServer(server, trustServer)
	trustv1.RegisterPersonalMeshRegistryServiceServer(server, registryServer)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///mesh-sync", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	published := make(map[string]bool)
	config := Config{endpointObservation: localEndpoint}
	if err := syncPersonalMeshesWithClients(context.Background(), config, trustv1.NewTrustServiceClient(connection),
		trustv1.NewPersonalMeshRegistryServiceClient(connection), published); err != nil {
		t.Fatal(err)
	}
	if registryServer.published != 1 || !published[meshID] || trustServer.endpoints["device-b"].GetEndpointGeneration() != remoteEndpoint.GetEndpointGeneration() {
		t.Fatalf("published=%d state=%v endpoints=%v", registryServer.published, published, trustServer.endpoints)
	}
	if err := syncPersonalMeshesWithClients(context.Background(), config, trustv1.NewTrustServiceClient(connection),
		trustv1.NewPersonalMeshRegistryServiceClient(connection), published); err != nil {
		t.Fatal(err)
	}
	if registryServer.published != 1 {
		t.Fatalf("unchanged local endpoint republished %d times", registryServer.published)
	}
}

func TestPersonalMeshSyncDiscoversThreeDevicesWithoutCrossMeshInference(t *testing.T) {
	meshABC := &trustv1.MeshSnapshot{MeshId: "mesh-abc", Revision: 4}
	meshBX := &trustv1.MeshSnapshot{MeshId: "mesh-bx", Revision: 2}
	localEndpoint := &trustv1.EndpointObservation{DeviceId: "device-b", EndpointGeneration: 7}
	trustServer := &registrySyncTrustServer{meshes: []*trustv1.PersonalMeshState{
		{MeshDescriptor: &trustv1.PersonalMeshDescriptor{MeshId: meshABC.GetMeshId()}, Snapshot: meshABC},
		{MeshDescriptor: &trustv1.PersonalMeshDescriptor{MeshId: meshBX.GetMeshId()}, Snapshot: meshBX},
	}, endpoints: make(map[string]*trustv1.EndpointObservation)}
	registryServer := &registrySyncServer{snapshots: map[string]*trustv1.MeshSnapshot{meshABC.GetMeshId(): meshABC, meshBX.GetMeshId(): meshBX},
		byMesh: map[string][]*trustv1.EndpointObservation{
			meshABC.GetMeshId(): {{DeviceId: "device-a", EndpointGeneration: 1}, {DeviceId: "device-b", EndpointGeneration: 7}, {DeviceId: "device-c", EndpointGeneration: 1}},
			meshBX.GetMeshId():  {{DeviceId: "device-b", EndpointGeneration: 7}, {DeviceId: "device-x", EndpointGeneration: 1}},
		}}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	trustv1.RegisterTrustServiceServer(server, trustServer)
	trustv1.RegisterPersonalMeshRegistryServiceServer(server, registryServer)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///mesh-three-device", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := syncPersonalMeshesWithClients(context.Background(), Config{endpointObservation: localEndpoint}, trustv1.NewTrustServiceClient(connection),
		trustv1.NewPersonalMeshRegistryServiceClient(connection), make(map[string]bool)); err != nil {
		t.Fatal(err)
	}
	if got := trustServer.applied[meshABC.GetMeshId()]; len(got) != 2 || got[0] != "device-a" || got[1] != "device-c" {
		t.Fatalf("A→B→C endpoint discovery = %v", got)
	}
	if got := trustServer.applied[meshBX.GetMeshId()]; len(got) != 1 || got[0] != "device-x" {
		t.Fatalf("B secondary mesh endpoint discovery = %v", got)
	}
	for _, device := range trustServer.applied[meshABC.GetMeshId()] {
		if device == "device-x" {
			t.Fatal("secondary mesh member leaked into A/B/C mesh")
		}
	}
}
