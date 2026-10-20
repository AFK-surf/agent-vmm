//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recoveryServiceGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	mu                                 sync.Mutex
	started                            int
	containers                         map[string]*vmmdv1.Container
	containerErrors                    map[string]codes.Code
	namespaceRequired                  bool
	namespaceReady                     bool
	inspectedContainers                []string
	stopError                          codes.Code
	projectionDeleteError              codes.Code
	sourceInstance, targetInstance     string
	sourceGeneration, targetGeneration string
	targetError                        codes.Code
	inspected, applied, deleted        int
	deletedRequestIDs                  []string
}

func (guest *recoveryServiceGuest) InspectContainer(_ context.Context, request *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.containers != nil {
		if code := guest.containerErrors[request.GetContainerId()]; code != codes.OK {
			return nil, status.Error(code, "component unavailable")
		}
		container, ok := guest.containers[request.GetContainerId()]
		if !ok {
			return nil, status.Error(codes.NotFound, "component missing")
		}
		return &vmmdv1.InspectContainerResponse{Container: proto.Clone(container).(*vmmdv1.Container)}, nil
	}
	if guest.namespaceRequired && !guest.namespaceReady {
		return nil, status.Error(codes.NotFound, "namespace absent")
	}
	guest.inspected++
	guest.inspectedContainers = append(guest.inspectedContainers, request.GetContainerId())
	instance := guest.sourceInstance
	generation := guest.sourceGeneration
	if request.GetActorNamespaceId() == "gitea-ns" {
		if guest.targetError != codes.OK {
			return nil, status.Error(guest.targetError, "observation failed")
		}
		instance = guest.targetInstance
		generation = guest.targetGeneration
	}
	state := vmmdv1.ContainerState_CONTAINER_STATE_RUNNING
	if instance == "" {
		state = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	}
	return &vmmdv1.InspectContainerResponse{Container: &vmmdv1.Container{Id: request.GetContainerId(), InstanceId: instance, GenerationId: generation, State: state}}, nil
}
func (guest *recoveryServiceGuest) ListContainers(ctx context.Context, request *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	id := "runner"
	if request.GetActorNamespaceId() == "gitea-ns" {
		id = "gitea"
	}
	observed, err := guest.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: request.GetActorNamespaceId(), ContainerId: id})
	if err != nil {
		return nil, err
	}
	return &vmmdv1.ListContainersResponse{Containers: []*vmmdv1.Container{observed.GetContainer()}}, nil
}
func (guest *recoveryServiceGuest) StartContainer(_ context.Context, request *vmmdv1.StartContainerRequest) (*vmmdv1.StartContainerResponse, error) {
	if guest.containers != nil {
		container := guest.containers[request.GetContainerId()]
		if container == nil || request.GetExpectedGenerationId() != container.GetGenerationId() {
			return nil, status.Error(codes.FailedPrecondition, "unexpected generation")
		}
		guest.started++
		container.State = vmmdv1.ContainerState_CONTAINER_STATE_RUNNING
		container.InstanceId = fmt.Sprintf("instance-%s-%d", request.GetContainerId(), guest.started)
		return &vmmdv1.StartContainerResponse{Container: proto.Clone(container).(*vmmdv1.Container)}, nil
	}
	if request.GetActorNamespaceId() != "gitea-ns" || request.GetExpectedGenerationId() != guest.targetGeneration {
		return nil, status.Error(codes.FailedPrecondition, "unexpected service start")
	}
	guest.started++
	guest.targetInstance = "recovered"
	return &vmmdv1.StartContainerResponse{Container: &vmmdv1.Container{Id: request.GetContainerId(), GenerationId: guest.targetGeneration, InstanceId: guest.targetInstance, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}
func (guest *recoveryServiceGuest) EnsureNamespace(context.Context, *vmmdv1.EnsureNamespaceRequest) (*vmmdv1.EnsureNamespaceResponse, error) {
	guest.namespaceReady = true
	return &vmmdv1.EnsureNamespaceResponse{}, nil
}

func (guest *recoveryServiceGuest) GetSystemUsage(context.Context, *vmmdv1.GetSystemUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	return &vmmdv1.GetUsageResponse{WorkloadPool: &vmmdv1.Usage{}, WorkloadMemoryLimitBytes: 9 << 30}, nil
}

func (guest *recoveryServiceGuest) StopContainer(context.Context, *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	return nil, status.Error(guest.stopError, "stop response lost")
}

func serviceRecoveryClient(t *testing.T, guest vmmdv1.VMMDServiceServer) vmmdv1.VMMDServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///service-recovery", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return vmmdv1.NewVMMDServiceClient(connection)
}

func (guest *recoveryServiceGuest) ApplyServiceProjection(_ context.Context, request *vmmdv1.ApplyServiceProjectionRequest) (*vmmdv1.ApplyServiceProjectionResponse, error) {
	guest.applied++
	if request.GetProjection().GetSourceContainerInstanceId() != guest.sourceInstance || request.GetProjection().GetDestinationContainerInstanceId() != guest.targetInstance {
		return nil, status.Error(codes.FailedPrecondition, "service projection container instance is stale")
	}
	return &vmmdv1.ApplyServiceProjectionResponse{}, nil
}
func (guest *recoveryServiceGuest) DeleteServiceProjection(_ context.Context, request *vmmdv1.DeleteServiceProjectionRequest) (*vmmdv1.DeleteServiceProjectionResponse, error) {
	guest.deleted++
	guest.deletedRequestIDs = append(guest.deletedRequestIDs, request.GetRequestId())
	if guest.projectionDeleteError != codes.OK {
		return nil, status.Error(guest.projectionDeleteError, "Guest projection retirement unavailable")
	}
	return &vmmdv1.DeleteServiceProjectionResponse{}, nil
}

func TestServiceProjectionRestoreSeparatesApplianceOwnerAndSettlesStaleEndpoints(t *testing.T) {
	for _, test := range []struct {
		name             string
		appliance        bool
		source, target   string
		targetError      codes.Code
		revoked          bool
		applied, deleted int
	}{
		{"current", false, "source-instance", "gitea-instance", codes.OK, false, 2, 0},
		{"target_lan_unavailable", false, "source-instance", "gitea-instance", codes.OK, false, 2, 0},
		{"target_retire_failed", false, "source-instance", "new-target", codes.OK, false, 0, 1},
		{"expired_local_lease", false, "source-instance", "gitea-instance", codes.OK, false, 2, 0},
		{"expired_signed_capability", false, "source-instance", "gitea-instance", codes.OK, false, 0, 0},
		{"source_restarted", false, "new-source", "gitea-instance", codes.OK, false, 2, 2},
		{"target_restarted", false, "source-instance", "new-target", codes.OK, false, 2, 2},
		{"target_generation_replaced", false, "source-instance", "new-target", codes.OK, false, 2, 2},
		{"source_stopped", false, "", "gitea-instance", codes.OK, false, 0, 2},
		{"target_missing", false, "source-instance", "", codes.NotFound, false, 0, 2},
		{"target_unknown", false, "source-instance", "", codes.Unavailable, false, 0, 0},
		{"appliance_waits_for_lifecycle_owner", true, "new-controller", "new-target", codes.OK, false, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller, installation := updateDrainFixture(t)
			controller.egress = egress.New()
			ctx := context.Background()
			installation.State = 3
			installation.DesiredEnabled = true
			installation.CurrentBootID = "old-boot"
			installation.CurrentContainerInstance = "old-controller"
			var err error
			installation, err = controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
			if err != nil {
				t.Fatal(err)
			}
			for _, environment := range []store.Environment{{ID: "source", NamespaceID: "source-ns", DisplayName: "source", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}, {ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}} {
				if _, err := controller.store.CreateEnvironment(ctx, environment); err != nil {
					t.Fatal(err)
				}
			}
			exported, err := controller.store.CreateServiceExport(ctx, "export", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "gitea", ContainerId: "gitea", ContainerInstanceId: "gitea-instance", ContainerGenerationId: "gitea-generation", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
			if err != nil {
				t.Fatal(err)
			}
			expires := timestamppb.New(time.Now().Add(time.Hour))
			if strings.HasPrefix(test.name, "expired_") {
				expires = timestamppb.New(time.Now().Add(-time.Minute))
			}
			for _, id := range []string{"import", "neighbor-import"} {
				value := &servicev1.ServiceImport{ImportId: id, SourceEnvironmentId: "source", SourceContainerId: "source", SourceContainerInstanceId: "source-instance", SourceContainerGenerationId: "source-generation", DestinationDeviceId: "device", DestinationExportId: exported.ExportId, DestinationPort: 3000, VirtualServiceName: id}
				route := &servicev1.AccessRoute{RouteId: id, RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 1, ExpiresAt: expires, SourceEnvironmentId: "source", SourceContainerInstanceId: value.SourceContainerInstanceId, PolicyRevision: 1, DestinationExportId: exported.ExportId}
				lease := &servicev1.RouteLease{LeaseId: id, RouteId: id, RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: &trustv1.RouteCapability{CapabilityId: id, ExpiresAt: expires}}
				if test.name == "expired_local_lease" {
					lease.Capability.CapabilityId = "local-" + id
				}
				if test.name == "expired_signed_capability" {
					lease.Capability.CapabilityId = "local-" + id
					lease.Capability.Signature = []byte("foreign-signed")
				}
				if test.appliance {
					route.State = servicev1.RouteState_ROUTE_STATE_PREPARING
					value.SourceEnvironmentId = ""
					value.SourceInstallationId = installation.ID
					value.SourceContainerId = systemContainerID()
					value.SourceContainerInstanceId = ""
					route.SourceEnvironmentId = ""
					route.SourceInstallationId = installation.ID
					route.SourceBootId = installation.CurrentBootID
					route.SourceContainerInstanceId = installation.CurrentContainerInstance
					request := &appliancev1.CreateApplianceServiceImportRequest{RequestId: id, InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: installation.CurrentBootID, ExpectedContainerInstanceId: installation.CurrentContainerInstance, DestinationExportId: exported.ExportId, ExpectedExportRevision: 1, VirtualServiceName: id}
					_, err = controller.store.CreateApplianceServiceImport(ctx, request, systemNamespaceID(installation.ID), value, route, lease)
				} else {
					_, err = controller.store.CreateServiceImport(ctx, id, value, route, lease)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			guest := &recoveryServiceGuest{sourceInstance: test.source, targetInstance: test.target, targetError: test.targetError, sourceGeneration: "source-generation", targetGeneration: "gitea-generation"}
			if test.name == "target_generation_replaced" {
				guest.targetGeneration = "replacement"
			}
			if test.name == "target_retire_failed" {
				guest.projectionDeleteError = codes.Unavailable
			}
			frontendEndpoints := map[string]string{}
			if test.name == "target_lan_unavailable" {
				address := netip.MustParseAddr("10.255.255.254")
				for localInterfaceContains(address) {
					address = address.Prev()
				}
				listener := &hostv1.ServiceListener{Id: "missing-lan", Local: true, EnvironmentId: "gitea", ContainerId: "gitea", TargetPort: 3000, Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, ListenAddress: net.JoinHostPort(address.String(), "32019")}
				if _, err := controller.store.CreateServiceListener(ctx, "missing-lan", listener, &trustv1.RouteCapability{}, &trustv1.EndpointObservation{}); err != nil {
					t.Fatal(err)
				}
				controller.supportDirectory = t.TempDir()
				controller.applianceFrontends = make(map[string]*applianceFrontendRuntime)
				defer controller.stopAllApplianceFrontends()
				identity := &vmmdv1.SystemApplianceIdentity{InstallationId: installation.ID, BootId: installation.CurrentBootID, ContainerInstanceId: installation.CurrentContainerInstance, BootNonce: make([]byte, 32)}
				for _, name := range []string{"management", "webhook"} {
					frontend := store.ApplianceFrontend{Name: name, Transport: int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP), Exposure: int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK), MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2}
					runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(context.Context, *vmmdv1.SystemStreamHeader) (net.Conn, error) {
						return nil, errors.New("unused frontend proxy")
					})
					if err != nil {
						t.Fatal(err)
					}
					controller.applianceFrontends[frontendRuntimeKey(installation.ID, name)] = runtime
					frontendEndpoints[name] = runtime.endpoint
				}
			}
			client := serviceRecoveryClient(t, guest)
			err = controller.restoreServiceProjectionsLocked(ctx, client)
			if test.targetError == codes.Unavailable || test.name == "target_retire_failed" {
				if status.Code(err) != codes.Unavailable {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for name, endpoint := range frontendEndpoints {
				observed, available := controller.applianceFrontendProjection(installation.ID, name, installation.CurrentContainerInstance)
				if observed != endpoint || !available {
					t.Fatalf("local LAN loss withdrew %s frontend: endpoint=%q available=%t", name, observed, available)
				}
				if socket, err := net.Listen("tcp4", endpoint); err == nil {
					socket.Close()
					t.Fatalf("local LAN loss released %s frontend socket", name)
				}
			}
			intent, err := controller.store.GetServiceImport(ctx, "import")
			if err != nil || intent.Revoked != test.revoked {
				t.Fatalf("intent=%v %v", intent, err)
			}
			if guest.applied != test.applied || guest.deleted < test.deleted {
				t.Fatalf("applied=%d deleted=%d", guest.applied, guest.deleted)
			}
			if test.appliance {
				if guest.inspected != 0 {
					t.Fatal("ordinary VM restore inspected an unpublished appliance source")
				}
				prior, err := controller.store.ServiceRouteForImport(ctx, "import")
				if err != nil {
					t.Fatal(err)
				}
				valid, err := controller.revalidateServiceExportLocked(ctx, client, proto.Clone(exported).(*servicev1.ServiceExport), "gitea-ns")
				if err != nil || !valid {
					t.Fatalf("lifecycle target observation=%t %v", valid, err)
				}
				intent, err := controller.store.GetServiceImport(ctx, "import")
				if err != nil || intent.Revoked || guest.deleted != 2 {
					t.Fatalf("lifecycle stale target=%v deleted=%d %v", intent, guest.deleted, err)
				}
				current, err := controller.store.ServiceRouteForImport(ctx, intent.ImportId)
				if err != nil {
					t.Fatal(err)
				}
				if current.Route.Revision <= prior.Route.Revision || current.Export.ContainerInstanceId != test.target {
					t.Fatalf("destination observation did not update route: prior=%v current=%v", prior.Route, current.Route)
				}
				route := proto.Clone(current.Route).(*servicev1.AccessRoute)
				route.Revision++
				route.State = servicev1.RouteState_ROUTE_STATE_ACTIVE
				lease := proto.Clone(current.Lease).(*servicev1.RouteLease)
				lease.RouteRevision = route.Revision
				lease.Generation = route.Generation
				if err := controller.store.RenewApplianceServiceRoute(ctx, installation, 1, prior.Route.Revision, route, lease); !errors.Is(err, store.ErrTrustRevisionConflict) {
					t.Fatalf("pre-observation revision accepted: %v", err)
				}
				if err := controller.store.RenewApplianceServiceRoute(ctx, installation, 1, current.Route.Revision, route, lease); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestManagedServiceRunningIntentSurvivesUnknownStopAndExcludesJobs(t *testing.T) {
	controller, _ := updateDrainFixture(t)
	controller.egress = egress.New()
	controller.workloadRunsRecovered = false
	ctx := context.Background()
	for _, environment := range []store.Environment{{ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}, {ID: "runner", NamespaceID: "source-ns", DisplayName: "runner", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}} {
		if _, err := controller.store.CreateEnvironment(ctx, environment); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := controller.store.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "service-gitea", AdapterID: "agentd-service", DisplayName: "Gitea", Scope: 1, ConfigTarget: "image", EnvironmentID: "gitea", DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare-gitea"}); err != nil {
		t.Fatal(err)
	}
	if err := controller.store.SetManagedContainerRunning(ctx, "gitea", "gitea", "generation", true); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"http", "ssh"} {
		if _, err := controller.store.CreateServiceExport(ctx, id, &servicev1.ServiceExport{ExportId: id, EnvironmentId: "gitea", ContainerId: "gitea", ContainerGenerationId: "generation", ContainerInstanceId: "before", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP}); err != nil {
			t.Fatal(err)
		}
	}
	expires := timestamppb.New(time.Now().Add(time.Hour))
	if _, err := controller.store.CreateServiceImport(ctx, "runner-import", &servicev1.ServiceImport{ImportId: "runner-import", SourceEnvironmentId: "runner", SourceContainerId: "runner", SourceContainerGenerationId: "runner-generation", DestinationDeviceId: "local", DestinationExportId: "http", VirtualServiceName: "gitea"}, &servicev1.AccessRoute{RouteId: "runner-route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, DestinationExportId: "http", State: servicev1.RouteState_ROUTE_STATE_PREPARING, Revision: 1, Generation: 1, ExpiresAt: expires, SourceEnvironmentId: "runner"}, &servicev1.RouteLease{LeaseId: "runner-lease", RouteId: "runner-route", RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: &trustv1.RouteCapability{ExpiresAt: expires}}); err != nil {
		t.Fatal(err)
	}
	guest := &recoveryServiceGuest{targetGeneration: "generation", sourceGeneration: "runner-generation", stopError: codes.Unavailable}
	client := serviceRecoveryClient(t, guest)
	if err := controller.recoverRunningWorkloadsFromGuest(ctx, client); err != nil {
		t.Fatal(err)
	}
	if guest.started != 1 {
		t.Fatalf("started=%d", guest.started)
	}
	if guest.targetInstance != "recovered" || guest.sourceInstance != "" {
		t.Fatal("managed recovery did not preserve the import-only runner as stopped")
	}
	guest.sourceInstance = "job-instance"
	if err := controller.recoverWorkloadRunsFromGuest(ctx, client); err != nil && err.Error() != "workload pool pressure is unavailable" {
		t.Fatal(err)
	}
	if controller.workloadRuns[workloadRunKey("gitea", "gitea")].stopping {
		t.Fatal("managed service recovered as generic orphan to stop")
	}
	if !controller.workloadRuns[workloadRunKey("runner", "runner")].stopping {
		t.Fatal("unmanaged job inherited service recovery")
	}
	unlock := controller.lockEnvironment("gitea")
	_, err := controller.stopWorkloadContainerLocked(ctx, client, "gitea", &vmmdv1.StopContainerRequest{RequestId: "explicit-stop", ActorNamespaceId: "gitea-ns", ContainerId: "gitea"})
	unlock()
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	for _, id := range []string{"http", "ssh"} {
		exported, err := controller.store.GetServiceExport(ctx, id)
		if err != nil || exported.GetRevoked() {
			t.Fatalf("stop intent=%v err=%v", exported, err)
		}
	}
	intent, err := controller.store.GetManagedContainer(ctx, "gitea", "gitea")
	if err != nil || intent.DesiredRunning {
		t.Fatal("unknown Stop lost stopped intent", intent, err)
	}
	guest.targetInstance = ""
	controller.workloadRuns = map[string]*workloadRun{}
	if err := controller.restoreManagedServiceContainers(ctx, client); err != nil {
		t.Fatal(err)
	}
	if guest.started != 1 {
		t.Fatal("unknown explicit Stop was restarted")
	}
	if _, err := controller.startWorkloadContainer(ctx, client, "gitea", &vmmdv1.StartContainerRequest{RequestId: "explicit-start", ActorNamespaceId: "gitea-ns", ContainerId: "gitea", ExpectedGenerationId: "generation"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"http", "ssh"} {
		exported, err := controller.store.GetServiceExport(ctx, id)
		if err != nil {
			t.Fatal(exported, err)
		}
		if err := controller.store.RevokeServiceExport(ctx, "gitea", id, exported.GetRevision()); err != nil {
			t.Fatal(err)
		}
	}
	guest.targetInstance = ""
	controller.workloadRuns = map[string]*workloadRun{}
	if err := controller.restoreManagedServiceContainers(ctx, client); err != nil {
		t.Fatal(err)
	}
	if guest.started != 3 {
		t.Fatal("revoking exports changed the independent container running intent")
	}
}

func TestManagedRecoveryProjectsExportAndListenerWithoutImports(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(map[bool]string{false: "start", true: "already-running"}[running], func(t *testing.T) {
			controller, _ := updateDrainFixture(t)
			controller.egress = egress.New()
			controller.workloadRunsRecovered = false
			controller.serviceListeners = make(map[string]*serviceListenerRuntime)
			defer controller.closeAllServiceListeners()
			ctx := context.Background()
			if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
				t.Fatal(err)
			}
			if _, err := controller.store.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "service-gitea", AdapterID: "agentd-service", DisplayName: "Gitea", Scope: 1, ConfigTarget: "image", EnvironmentID: "gitea", DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare-gitea"}); err != nil {
				t.Fatal(err)
			}
			if err := controller.store.SetManagedContainerRunning(ctx, "gitea", "gitea", "generation", true); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"http", "ssh"} {
				if _, err := controller.store.CreateServiceExport(ctx, id, &servicev1.ServiceExport{ExportId: id, EnvironmentId: "gitea", ContainerId: "gitea", ContainerGenerationId: "generation", ContainerInstanceId: "old", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP}); err != nil {
					t.Fatal(err)
				}
			}
			bound, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := bound.Addr().String()
			bound.Close()
			if _, err := controller.store.CreateServiceListener(ctx, "listener", &hostv1.ServiceListener{Id: "listener", Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, EnvironmentId: "gitea", ContainerId: "gitea", TargetPort: 3000, ListenAddress: address, Local: true, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}, &trustv1.RouteCapability{CapabilityId: "listener"}, &trustv1.EndpointObservation{DeviceId: "local"}); err != nil {
				t.Fatal(err)
			}
			if _, err := controller.store.CreateServiceExport(ctx, "neighbor", &servicev1.ServiceExport{ExportId: "neighbor", EnvironmentId: "gitea", ContainerId: "ordinary-job", ContainerGenerationId: "neighbor-generation", ContainerInstanceId: "neighbor-instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP}); err != nil {
				t.Fatal(err)
			}
			if err := controller.store.SetManagedContainerRunning(ctx, "gitea", "ordinary-job", "neighbor-generation", false); err != nil {
				t.Fatal(err)
			}
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			if _, err := controller.store.CreateServiceListener(ctx, "neighbor-listener", &hostv1.ServiceListener{Id: "neighbor-listener", RouteId: "neighbor-route", CapabilityId: "neighbor", Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, EnvironmentId: "gitea", ContainerId: "ordinary-job", TargetPort: 3000, ListenAddress: occupied.Addr().String(), Local: true, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}, &trustv1.RouteCapability{CapabilityId: "neighbor"}, &trustv1.EndpointObservation{DeviceId: "local"}); err != nil {
				t.Fatal(err)
			}
			guest := &recoveryServiceGuest{targetGeneration: "generation", namespaceRequired: true}
			if running {
				guest.targetInstance = "recovered"
			}
			if err := controller.recoverRunningWorkloadsFromGuest(ctx, serviceRecoveryClient(t, guest)); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"http", "ssh"} {
				value, err := controller.store.GetServiceExport(ctx, id)
				if err != nil || value.GetContainerInstanceId() != "recovered" || value.GetContainerGenerationId() != "generation" || value.GetRevoked() {
					t.Fatalf("export %s=%v %v", id, value, err)
				}
			}
			listener := controller.serviceListeners["listener"]
			if listener == nil || listener.instance != "recovered" || listener.listener.Addr().String() != address {
				t.Fatalf("listener=%v", listener)
			}
			imports, err := controller.store.ListServiceImports(ctx, "gitea")
			if err != nil || len(imports) != 0 {
				t.Fatalf("imports=%v %v", imports, err)
			}
			wantStarts := 1
			if running {
				wantStarts = 0
			}
			if guest.started != wantStarts {
				t.Fatalf("starts=%d", guest.started)
			}
		})
	}
}

type missingAllocationNamespaceGuest struct {
	recoveryServiceGuest
	exists  bool
	ensured int
}

func (guest *missingAllocationNamespaceGuest) GetNamespace(context.Context, *vmmdv1.GetNamespaceRequest) (*vmmdv1.GetNamespaceResponse, error) {
	if !guest.exists {
		return nil, status.Error(codes.NotFound, "namespace absent")
	}
	return &vmmdv1.GetNamespaceResponse{}, nil
}
func (guest *missingAllocationNamespaceGuest) EnsureNamespace(context.Context, *vmmdv1.EnsureNamespaceRequest) (*vmmdv1.EnsureNamespaceResponse, error) {
	guest.ensured++
	return &vmmdv1.EnsureNamespaceResponse{}, nil
}
func (guest *missingAllocationNamespaceGuest) ListContainers(context.Context, *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	return nil, status.Error(codes.NotFound, "container inventory missing")
}
func TestWorkloadRecoveryDoesNotReadmitAbsentAllocation(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-admitted", true: "inventory-error"}[exists], func(t *testing.T) {
			controller, installation := updateDrainFixture(t)
			controller.egress = egress.New()
			controller.workloadRunsRecovered = false
			ctx := context.Background()
			environment, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "failed-runner", NamespaceID: "missing-namespace", DisplayName: "failed", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: installation.RegistrationID, AllocationID: "failed-allocation", AllocationGeneration: 1, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING)})
			if err != nil {
				t.Fatal(err)
			}
			guest := &missingAllocationNamespaceGuest{exists: exists}
			err = controller.recoverRunningWorkloadsFromGuest(ctx, workloadGuestClient(t, guest))
			if exists {
				if err == nil || !strings.Contains(err.Error(), "failed-runner") || !strings.Contains(err.Error(), "missing-namespace") || status.Code(err) != codes.NotFound {
					t.Fatalf("missing resource was swallowed: %v", err)
				}
			} else {
				if err != nil || guest.ensured != 0 || !controller.workloadRunsRecovered || len(controller.workloadRuns) != 0 {
					t.Fatalf("recovery=%v ensured=%d runs=%v", err, guest.ensured, controller.workloadRuns)
				}
				current, err := controller.store.GetEnvironment(ctx, environment.ID)
				if err != nil || current.AllocationState != environment.AllocationState {
					t.Fatalf("failed allocation was readmitted: %+v %v", current, err)
				}
			}
		})
	}
}

func TestManagedContainersRecoverNoPortComponentAndIsolateFailures(t *testing.T) {
	controller, _ := updateDrainFixture(t)
	controller.workloadRunsRecovered = true
	controller.egress = egress.New()
	ctx := context.Background()
	if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service-ns", DisplayName: "Service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "service", EnvironmentID: "service", AdapterID: "agentd-service", DisplayName: "Service", Scope: 1, ConfigTarget: "image", DesiredState: 1, ConfigState: 8, PrepareRequestID: "prepare-service"}); err != nil {
		t.Fatal(err)
	}
	guest := &recoveryServiceGuest{containers: map[string]*vmmdv1.Container{}, containerErrors: map[string]codes.Code{}}
	for _, id := range []string{"a-worker", "z-web"} {
		guest.containers[id] = &vmmdv1.Container{Id: id, GenerationId: "generation-" + id, State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
		if err := controller.store.SetManagedContainerRunning(ctx, "service", id, "generation-"+id, true); err != nil {
			t.Fatal(err)
		}
		observed := containerFromGuest(guest.containers[id])
		if err := controller.projectManagedContainerIntent(ctx, "service", observed); err != nil || observed.DesiredRunning == nil || !observed.GetDesiredRunning() || observed.GetState() != hostv1.ContainerState_CONTAINER_STATE_STOPPED {
			t.Fatal("desired and observed state were conflated", observed, err)
		}
		unmanaged := containerFromGuest(guest.containers[id])
		if err := controller.projectManagedContainerIntent(ctx, "controller", unmanaged); err != nil || unmanaged.DesiredRunning != nil {
			t.Fatal("controller acquired managed state", unmanaged, err)
		}
	}
	client := serviceRecoveryClient(t, guest)
	if err := controller.restoreManagedServiceContainers(ctx, client); err != nil {
		t.Fatal(err)
	}
	if guest.started != 2 {
		t.Fatalf("no-port recovery starts=%d", guest.started)
	}
	if err := controller.restoreManagedServiceContainers(ctx, client); err != nil {
		t.Fatal(err)
	}
	if guest.started != 2 {
		t.Fatal("running components duplicated")
	}
	guest.containerErrors["a-worker"] = codes.Unavailable
	guest.containers["z-web"].State = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	guest.containers["z-web"].InstanceId = ""
	controller.mu.Lock()
	controller.deleteWorkloadRunLocked(workloadRunKey("service", "z-web"))
	controller.mu.Unlock()
	if err := controller.restoreManagedServiceContainers(ctx, client); err == nil {
		t.Fatal("component failure disappeared")
	}
	if guest.started != 3 || guest.containers["z-web"].GetInstanceId() == "" {
		t.Fatal("failed worker blocked web recovery")
	}
	guest.containerErrors["a-worker"] = codes.OK
	guest.containerErrors["z-web"] = codes.Unavailable
	unlock := controller.lockEnvironment("service")
	_, err := controller.stopWorkloadContainerLocked(ctx, client, "service", &vmmdv1.StopContainerRequest{RequestId: "stop-web", ActorNamespaceId: "service-ns", ContainerId: "z-web"})
	unlock()
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	intent, err := controller.store.GetManagedContainer(ctx, "service", "z-web")
	if err != nil || intent.DesiredRunning {
		t.Fatal("unavailable observation lost Stop intent", intent, err)
	}
	observed := containerFromGuest(guest.containers["z-web"])
	if err := controller.projectManagedContainerIntent(ctx, "service", observed); err != nil || observed.DesiredRunning == nil || observed.GetDesiredRunning() || observed.GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatal("stopped intent lost running observation", observed, err)
	}
	encoded, err := proto.Marshal(observed)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(hostv1.Container)
	if err := proto.Unmarshal(encoded, decoded); err != nil || decoded.DesiredRunning == nil || decoded.GetDesiredRunning() {
		t.Fatal("explicit false intent was lost on the wire", decoded, err)
	}
	guest.containerErrors["z-web"] = codes.OK
	guest.containers["z-web"].State = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED
	guest.containers["z-web"].InstanceId = ""
	delete(guest.containers, "a-worker")
	if err := controller.restoreManagedServiceContainers(ctx, client); err != nil {
		t.Fatal(err)
	}
	if guest.started != 3 {
		t.Fatal("Stop or missing container was rebuilt")
	}
	if exports, err := controller.store.ListServiceExports(ctx, "service"); err != nil || len(exports) != 0 {
		t.Fatal(exports, err)
	}
	controller.manager = &vmm.Manager{}
	controller.mu.Lock()
	run := &workloadRun{environmentID: "service", namespaceID: "service-ns", containerID: "a-worker", instanceID: "active-instance"}
	controller.setWorkloadRunLocked(workloadRunKey("service", "a-worker"), run)
	controller.mu.Unlock()
	hostClient := executionClient(t, controller, auth.Identity{Role: auth.Agent, SessionID: "service-session", EnvironmentID: "service", NamespaceID: "service-ns"})
	if _, err := hostClient.StopContainer(ctx, &hostv1.StopContainerRequest{RequestId: "stale-stop", ContainerId: "a-worker", ExpectedInstanceId: "stale-instance"}); status.Code(err) != codes.Aborted {
		t.Fatal("stale instance changed Stop intent", err)
	}
	intent, err = controller.store.GetManagedContainer(ctx, "service", "a-worker")
	if err != nil || !intent.DesiredRunning {
		t.Fatal("unaccepted Stop changed intent", intent, err)
	}
	if _, err := hostClient.StopContainer(ctx, &hostv1.StopContainerRequest{RequestId: "offline-stop", ContainerId: "a-worker", ExpectedInstanceId: "active-instance"}); status.Code(err) != codes.Unavailable {
		t.Fatal("unavailable Guest transport", err)
	}
	intent, err = controller.store.GetManagedContainer(ctx, "service", "a-worker")
	if err != nil || intent.DesiredRunning || !run.stopping || run.inFlight {
		t.Fatal("Guest dial failure lost accepted Stop", intent, run, err)
	}
}

func TestContainerStartReceiptSurvivesUnavailableLANListener(t *testing.T) {
	for _, mode := range []string{"same-container", "other-environment", "guest-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			controller, integration := integrationAuthorityFixture(t)
			controller.workloadRunsRecovered = true
			defer controller.closeAllServiceListeners()
			ctx := t.Context()
			const id = "worker"
			listenerEnvironment := integration.EnvironmentID
			if mode == "other-environment" {
				listenerEnvironment = "media"
				if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: listenerEnvironment, NamespaceID: listenerEnvironment, DisplayName: "Media", OwnerKind: store.OwnerLocal}); err != nil {
					t.Fatal(err)
				}
				controller.setWorkloadRunLocked(workloadRunKey(listenerEnvironment, id), &workloadRun{environmentID: listenerEnvironment, containerID: id, instanceID: "media-instance"})
			}
			address := netip.MustParseAddr("10.255.255.254")
			for localInterfaceContains(address) {
				address = address.Prev()
			}
			value := &hostv1.ServiceListener{Id: "unavailable-lan", Local: true, EnvironmentId: listenerEnvironment, ContainerId: id, TargetPort: 8080, Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, ListenAddress: net.JoinHostPort(address.String(), "32019")}
			if _, err := controller.store.CreateServiceListener(ctx, "open-lan", value, &trustv1.RouteCapability{}, &trustv1.EndpointObservation{}); err != nil {
				t.Fatal(err)
			}
			guest := &recoveryServiceGuest{containers: map[string]*vmmdv1.Container{id: {Id: id, GenerationId: "generation", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}}, containerErrors: map[string]codes.Code{}}
			if mode == "guest-unavailable" {
				guest.containerErrors[id] = codes.Unavailable
			}
			client := serviceRecoveryClient(t, guest)
			server := &agentServer{controller: controller}
			identity := auth.Identity{EnvironmentID: integration.EnvironmentID, NamespaceID: "namespace"}
			request := &hostv1.ContainerMutationRequest{RequestId: "start", ContainerId: id, ExpectedGenerationId: "generation"}
			guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.RequestId, ActorNamespaceId: identity.NamespaceID, ContainerId: id, ExpectedGenerationId: request.ExpectedGenerationId}
			execute := func() (*hostv1.ContainerResponse, error) {
				response, err := controller.startWorkloadContainer(ctx, client, integration.EnvironmentID, guestRequest)
				if err != nil {
					return nil, err
				}
				if err := controller.invalidateStaleServiceExports(ctx, client, integration.EnvironmentID, id, response.GetContainer().GetInstanceId()); err != nil {
					return nil, err
				}
				return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
			}
			first, err := server.containerMutationReceipt(ctx, identity, "runtime-start-container", request.RequestId, request, guestRequest, id, execute, admitOperation(server))
			if mode == "guest-unavailable" {
				if status.Code(err) != codes.Unavailable || guest.started != 0 {
					t.Fatalf("real Guest failure was hidden: result=%v starts=%d err=%v", first, guest.started, err)
				}
				return
			}
			if err != nil || guest.started != 1 || first.GetContainer().GetInstanceId() == "" {
				t.Fatalf("completed Guest start became an RPC failure: result=%v starts=%d err=%v", first, guest.started, err)
			}
			replay, err := server.containerMutationReceipt(ctx, identity, "runtime-start-container", request.RequestId, request, guestRequest, id, func() (*hostv1.ContainerResponse, error) {
				t.Fatal("successful start receipt was executed again")
				return nil, nil
			}, admitOperation(server))
			if err != nil || !proto.Equal(first, replay) {
				t.Fatal("listener failure prevented successful receipt", replay, err)
			}
			observed := controller.observeLocalServiceListenerLocked(ctx, value)
			if observed.GetAvailable() || observed.GetUnavailableReason() == "" || observed.GetId() != value.GetId() || observed.GetListenAddress() != value.GetListenAddress() {
				t.Fatal("missing LAN interface lost its local observation", observed)
			}
		})
	}
}
