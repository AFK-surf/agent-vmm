//go:build darwin || linux

package appliance

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
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type serviceProjectionRecoveryGuest struct {
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
	stopped                            int
	deletedRequestIDs                  []string
}

func (guest *serviceProjectionRecoveryGuest) InspectContainer(_ context.Context, request *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
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
func (guest *serviceProjectionRecoveryGuest) ListContainers(ctx context.Context, request *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
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
func (guest *serviceProjectionRecoveryGuest) StartContainer(_ context.Context, request *vmmdv1.StartContainerRequest) (*vmmdv1.StartContainerResponse, error) {
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
func (guest *serviceProjectionRecoveryGuest) EnsureNamespace(context.Context, *vmmdv1.EnsureNamespaceRequest) (*vmmdv1.EnsureNamespaceResponse, error) {
	guest.namespaceReady = true
	return &vmmdv1.EnsureNamespaceResponse{}, nil
}

func (guest *serviceProjectionRecoveryGuest) GetSystemUsage(context.Context, *vmmdv1.GetSystemUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	return &vmmdv1.GetUsageResponse{WorkloadPool: &vmmdv1.Usage{}, WorkloadMemoryLimitBytes: 9 << 30}, nil
}

func (guest *serviceProjectionRecoveryGuest) StopContainer(_ context.Context, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.stopError != codes.OK {
		return nil, status.Error(guest.stopError, "stop response lost")
	}
	guest.stopped++
	guest.targetInstance = ""
	return &vmmdv1.StopContainerResponse{Container: &vmmdv1.Container{Id: request.GetContainerId(), State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}}, nil
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

func (guest *serviceProjectionRecoveryGuest) ApplyServiceProjection(_ context.Context, request *vmmdv1.ApplyServiceProjectionRequest) (*vmmdv1.ApplyServiceProjectionResponse, error) {
	guest.applied++
	if request.GetProjection().GetSourceContainerInstanceId() != guest.sourceInstance || request.GetProjection().GetDestinationContainerInstanceId() != guest.targetInstance {
		return nil, status.Error(codes.FailedPrecondition, "service projection container instance is stale")
	}
	return &vmmdv1.ApplyServiceProjectionResponse{}, nil
}
func (guest *serviceProjectionRecoveryGuest) DeleteServiceProjection(_ context.Context, request *vmmdv1.DeleteServiceProjectionRequest) (*vmmdv1.DeleteServiceProjectionResponse, error) {
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
			for _, environment := range []store.Environment{{ID: "source", NamespaceID: "source-ns", DisplayName: "source", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}, {ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}} {
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
			device, err := controller.trust.Identity(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"import", "neighbor-import"} {
				value := &servicev1.ServiceImport{ImportId: id, SourceEnvironmentId: "source", SourceContainerId: "source", SourceContainerInstanceId: "source-instance", SourceContainerGenerationId: "source-generation", DestinationDeviceId: device.Identity.GetDeviceId(), DestinationExportId: exported.ExportId, DestinationPort: 3000, VirtualServiceName: id}
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
			guest := &serviceProjectionRecoveryGuest{sourceInstance: test.source, targetInstance: test.target, targetError: test.targetError, sourceGeneration: "source-generation", targetGeneration: "gitea-generation"}
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
				defer controller.StopAllFrontends()
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
			err = controller.services.(*servicenet.Owner).RestoreServiceProjectionsLocked(ctx, client)
			if test.targetError == codes.Unavailable || test.name == "target_retire_failed" {
				if status.Code(err) != codes.Unavailable {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for name, endpoint := range frontendEndpoints {
				observed, available, _ := controller.applianceFrontendProjection(installation.ID, name, installation.CurrentContainerInstance)
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
				valid, err := controller.services.(*servicenet.Owner).RevalidateServiceExportLocked(ctx, client, proto.Clone(exported).(*servicev1.ServiceExport), "gitea-ns")
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
func localInterfaceContains(expected netip.Addr) bool {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, address := range addresses {
		prefix, err := netip.ParsePrefix(address.String())
		if err == nil && prefix.Addr().Unmap() == expected.Unmap() {
			return true
		}
	}
	return false
}
