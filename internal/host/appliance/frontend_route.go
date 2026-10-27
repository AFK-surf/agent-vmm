//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (controller *Owner) approvedWebhookFrontend(ctx context.Context, installationID string) error {
	frontends, err := controller.store.ListApplianceFrontends(ctx, installationID)
	if err != nil {
		return err
	}
	for _, frontend := range frontends {
		if frontend.Name == "webhook" && frontend.Transport == int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP) && frontend.Exposure != int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_OUTBOUND_ONLY) {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "approved HTTP webhook frontend is required")
}

func (controller *Owner) refreshApplianceFrontendServiceImportLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, grant store.ApplianceGrant, value *servicev1.ServiceImport) error {
	record, err := controller.store.ServiceRouteForImport(ctx, value.GetImportId())
	if err != nil {
		return err
	}
	source, err := controller.store.GetEnvironment(ctx, value.GetSourceEnvironmentId())
	if err != nil {
		return err
	}
	observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: source.NamespaceID, ContainerId: value.GetSourceContainerId()})
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	container := observed.GetContainer()
	instance := ""
	stopped, err := controller.store.ServiceContainerStoppedByOwner(ctx, source.ID, value.GetSourceContainerId(), container.GetGenerationId())
	if err != nil {
		return err
	}
	if !stopped && container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		instance = container.GetInstanceId()
	}
	if err := controller.services.ProjectServiceContainerLocked(ctx, client, source.ID, value.GetSourceContainerId(), container.GetGenerationId(), instance); err != nil {
		return err
	}
	if value.GetSourceContainerGenerationId() != container.GetGenerationId() {
		return nil
	}
	if instance == "" {
		return nil
	}
	value, err = controller.store.GetServiceImport(ctx, value.GetImportId())
	if err != nil {
		return err
	}
	record, err = controller.store.ServiceRouteForImport(ctx, value.GetImportId())
	if err != nil {
		return err
	}
	frontendErr := controller.approvedWebhookFrontend(ctx, installation.ID)
	if frontendErr != nil {
		if frontendErr != nil && status.Code(frontendErr) != codes.PermissionDenied {
			return frontendErr
		}
		if err := controller.store.RevokeServiceImport(ctx, value.GetSourceEnvironmentId(), value.GetImportId(), value.GetRevision()); err != nil {
			return err
		}
		return controller.services.DeleteServiceProjection(ctx, opreceipt.DerivedRequestID(fmt.Sprintf("frontend-service-revoked/%s/%d", value.GetImportId(), value.GetRevision())), value.GetImportId())
	}
	route := record.Route
	lease := record.Lease
	if route.GetState() != servicev1.RouteState_ROUTE_STATE_ACTIVE || route.GetDestinationBootId() != installation.CurrentBootID || route.GetDestinationContainerInstanceId() != installation.CurrentContainerInstance || route.GetDestinationGrantRevision() != grant.Revision || route.GetExpiresAt().AsTime().Before(time.Now().Add(time.Hour)) {
		if err := controller.services.DeleteServiceProjection(ctx, opreceipt.DerivedRequestID(fmt.Sprintf("frontend-service-renew/%s/%d", value.GetImportId(), route.GetGeneration())), value.GetImportId()); err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		route = proto.Clone(route).(*servicev1.AccessRoute)
		route.Revision++
		route.State = servicev1.RouteState_ROUTE_STATE_ACTIVE
		route.DestinationBootId = installation.CurrentBootID
		route.DestinationContainerInstanceId = installation.CurrentContainerInstance
		route.DestinationGrantRevision = grant.Revision
		capability := servicenet.LocalServiceRouteCapability(opreceipt.DerivedRequestID(fmt.Sprintf("frontend-service/%s/%s/%d/%s/%s/%d", installation.ID, value.GetImportId(), route.Revision, installation.CurrentBootID, installation.CurrentContainerInstance, grant.Revision)), route.GetSourceDeviceId(), "", "", false)
		capability.PolicyRevision = grant.Revision
		route.Generation = capability.GetRouteGeneration()
		route.ExpiresAt = capability.GetExpiresAt()
		lease = proto.Clone(lease).(*servicev1.RouteLease)
		lease.RouteRevision = route.Revision
		lease.Generation = route.Generation
		lease.ExpiresAt = route.ExpiresAt
		lease.Capability = proto.Clone(capability).(*trustv1.RouteCapability)
		if err := controller.store.RenewApplianceFrontendServiceRoute(ctx, installation, grant.Revision, record.Route.GetRevision(), route, lease); err != nil {
			return err
		}
	}
	virtualAddress := egresslink.ServiceAddress(value.GetImportId())
	response, err := client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{RequestId: opreceipt.DerivedRequestID(fmt.Sprintf("frontend-service-project/%s/%d", value.GetImportId(), route.GetGeneration())), Projection: &vmmdv1.ServiceProjection{Id: value.GetImportId(), SourceNamespaceId: source.NamespaceID, SourceContainerId: value.GetSourceContainerId(), SourceContainerInstanceId: value.GetSourceContainerInstanceId(), DestinationPort: 80, DnsAlias: value.GetVirtualServiceName(), ExpiresUnixMillis: route.GetExpiresAt().AsTime().UnixMilli(), Remote: true, VirtualAddress: virtualAddress.String()}})
	if err != nil {
		return err
	}
	sourceAddress, err := netip.ParseAddr(response.GetSourceAddress())
	if err != nil {
		return err
	}
	routeID, leaseID, generation := route.GetRouteId(), lease.GetLeaseId(), route.GetGeneration()
	return controller.egress.ApplyServiceRoute(egress.ServiceRoute{ID: value.GetImportId(), Generation: generation, EnvironmentID: source.ID, SourceAddress: sourceAddress, Destination: netip.AddrPortFrom(virtualAddress, 80), ExpiresAt: route.GetExpiresAt().AsTime(), Dial: func(dialContext context.Context) (net.Conn, error) {
		return controller.dialApplianceFrontendService(dialContext, routeID, leaseID, generation)
	}})
}

func (controller *Owner) dialApplianceFrontendService(ctx context.Context, routeID, leaseID string, generation uint64) (net.Conn, error) {
	record, err := controller.store.GetServiceRoute(ctx, routeID, leaseID, generation)
	if err != nil {
		return nil, err
	}
	destination := record.Import.GetDestinationFrontend()
	if record.Import.GetRevoked() || destination.GetName() != "webhook" || record.Route.GetState() != servicev1.RouteState_ROUTE_STATE_ACTIVE || !record.Route.GetExpiresAt().AsTime().After(time.Now()) {
		return nil, errors.New("frontend service route is unavailable")
	}
	installation, err := controller.store.GetApplianceInstallation(ctx, destination.GetInstallationId())
	if err != nil {
		return nil, err
	}
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	if !installation.DesiredEnabled || installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) || installation.CurrentBootID != record.Route.GetDestinationBootId() || installation.CurrentContainerInstance != record.Route.GetDestinationContainerInstanceId() || grant.Revision != record.Route.GetDestinationGrantRevision() {
		return nil, errors.New("frontend service destination changed")
	}
	if err := controller.approvedWebhookFrontend(ctx, installation.ID); err != nil {
		return nil, err
	}
	source, err := controller.store.GetEnvironment(ctx, record.Import.GetSourceEnvironmentId())
	if err != nil {
		return nil, err
	}
	if err := controller.services.ValidateContainerInstance(ctx, source.NamespaceID, record.Import.GetSourceContainerId(), record.Import.GetSourceContainerInstanceId()); err != nil {
		return nil, err
	}
	controller.applianceFrontendMu.Lock()
	runtime := controller.applianceFrontends[frontendRuntimeKey(installation.ID, destination.GetName())]
	controller.applianceFrontendMu.Unlock()
	if runtime == nil {
		return nil, errors.New("frontend service instance is unavailable")
	}
	runtime.mu.Lock()
	available := runtime.available
	runtime.mu.Unlock()
	if runtime.identity.GetBootId() != installation.CurrentBootID || runtime.identity.GetContainerInstanceId() != installation.CurrentContainerInstance || !available {
		return nil, errors.New("frontend service instance is unavailable")
	}
	return runtime.dialService(ctx)
}
