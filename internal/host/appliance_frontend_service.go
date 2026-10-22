//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
)

func (server *applianceAdminServer) CreateFrontendServiceImport(ctx context.Context, request *appliancev1.CreateApplianceFrontendServiceImportRequest) (*servicev1.ServiceImport, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetInstallationId() == "" || request.GetExpectedRevision() == 0 || request.GetExpectedGrantRevision() == 0 || request.GetExpectedBootId() == "" || request.GetExpectedContainerInstanceId() == "" || request.GetSourceEnvironmentId() == "" || request.GetSourceContainerId() == "" || request.GetSourceContainerInstanceId() == "" || request.GetFrontendName() != "webhook" || !virtualServiceName.MatchString(request.GetVirtualServiceName()) {
		return nil, invalidField("service_import", "complete exact source and webhook frontend identity are required")
	}
	release, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	unlock := server.controller.lockIntegration("appliance-lifecycle:" + request.GetInstallationId())
	defer unlock()
	if prior, err := server.controller.store.ReplayApplianceFrontendServiceImport(ctx, request); err == nil {
		return prior, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, resourceError(codes.Aborted, "APPLIANCE_SERVICE_IMPORT_CONFLICT", "service_import", request.GetRequestId(), "service request owner or payload changed")
	}
	installation, err := server.controller.store.GetApplianceInstallation(ctx, request.GetInstallationId())
	if err != nil {
		return nil, err
	}
	grant, err := server.controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	if installation.Revision != request.GetExpectedRevision() || grant.Revision != request.GetExpectedGrantRevision() || installation.CurrentBootID != request.GetExpectedBootId() || installation.CurrentContainerInstance != request.GetExpectedContainerInstanceId() || !installation.DesiredEnabled || installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) {
		return nil, resourceErrorWithMetadata(codes.Aborted, "APPLIANCE_SERVICE_DESTINATION_CHANGED", "appliance_installation", installation.ID, "read current destination installation and instance before approving a service import", map[string]string{"admission": "not_accepted"})
	}
	if err := server.controller.approvedWebhookFrontend(ctx, installation.ID); err != nil {
		return nil, err
	}
	source, err := server.controller.store.GetEnvironment(ctx, request.GetSourceEnvironmentId())
	if err != nil {
		return nil, err
	}
	sourceContainer, err := server.controller.serviceContainerIdentity(ctx, source.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId())
	if err != nil {
		return nil, err
	}
	if server.controller.trust == nil {
		return nil, resourceError(codes.FailedPrecondition, "TRUST_UNAVAILABLE", "device", "local", "device trust is unavailable")
	}
	device, err := server.controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	importID, err := randomID()
	if err != nil {
		return nil, err
	}
	routeID, err := randomID()
	if err != nil {
		return nil, err
	}
	leaseID, err := randomID()
	if err != nil {
		return nil, err
	}
	capability := localServiceRouteCapability(request.GetRequestId(), device.Identity.GetDeviceId(), "", "", false)
	capability.PolicyRevision = grant.Revision
	value := &servicev1.ServiceImport{ImportId: importID, SourceEnvironmentId: source.ID, SourceContainerId: request.GetSourceContainerId(), SourceContainerInstanceId: request.GetSourceContainerInstanceId(), SourceContainerGenerationId: sourceContainer.GetGenerationId(), DestinationDeviceId: device.Identity.GetDeviceId(), DestinationFrontend: &servicev1.ApplianceFrontendDestination{InstallationId: installation.ID, Name: request.GetFrontendName()}, VirtualServiceName: request.GetVirtualServiceName(), DestinationPort: 80}
	route := &servicev1.AccessRoute{RouteId: routeID, RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, SourceDeviceId: device.Identity.GetDeviceId(), DestinationDeviceId: device.Identity.GetDeviceId(), SourceEnvironmentId: source.ID, SourceContainerInstanceId: request.GetSourceContainerInstanceId(), DestinationInstallationId: installation.ID, DestinationBootId: installation.CurrentBootID, DestinationContainerInstanceId: installation.CurrentContainerInstance, DestinationGrantRevision: grant.Revision, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: capability.GetExpiresAt()}
	lease := &servicev1.RouteLease{LeaseId: leaseID, RouteId: routeID, RouteRevision: 1, Generation: route.Generation, ExpiresAt: route.ExpiresAt, Capability: capability}
	created, err := server.controller.store.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease)
	if err != nil {
		return nil, resourceError(codes.Aborted, "APPLIANCE_SERVICE_IMPORT_CONFLICT", "service_import", request.GetRequestId(), "service request, alias or owner fence changed")
	}
	if !created.GetRevoked() {
		if err := server.controller.refreshApplianceServiceImports(ctx, installation); err != nil {
			return nil, err
		}
	}
	return created, nil
}
