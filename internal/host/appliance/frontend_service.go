//go:build darwin || linux

package appliance

import (
	"context"
	"database/sql"
	"errors"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) CreateApplianceFrontendServiceImport(ctx context.Context, request *appliancev1.CreateApplianceFrontendServiceImportRequest) (*appliancev1.CreateApplianceFrontendServiceImportResponse, error) {
	if request.GetRequestId() == "" || request.GetInstallationId() == "" || request.GetExpectedRevision() == 0 || request.GetExpectedGrantRevision() == 0 || request.GetExpectedBootId() == "" || request.GetExpectedContainerInstanceId() == "" || request.GetSourceEnvironmentId() == "" || request.GetSourceContainerId() == "" || request.GetSourceContainerInstanceId() == "" || request.GetFrontendName() != "webhook" || !servicenet.ValidVirtualServiceName(request.GetVirtualServiceName()) {
		return nil, hostv1.InvalidField("service_import", "complete exact source and webhook frontend identity are required")
	}
	ctx, release, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	unlock, lockErr := server.applianceLocks.Lock(ctx, request.GetInstallationId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if prior, err := server.store.ReplayApplianceFrontendServiceImport(ctx, request); err == nil {
		return &appliancev1.CreateApplianceFrontendServiceImportResponse{Import: prior}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.Aborted, "service request owner or payload changed")
	}
	installation, err := server.store.GetApplianceInstallation(ctx, request.GetInstallationId())
	if err != nil {
		return nil, err
	}
	grant, err := server.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	if installation.Revision != request.GetExpectedRevision() || grant.Revision != request.GetExpectedGrantRevision() || installation.CurrentBootID != request.GetExpectedBootId() || installation.CurrentContainerInstance != request.GetExpectedContainerInstanceId() || !installation.DesiredEnabled || installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_APPLIANCE_SERVICE_DESTINATION_CHANGED, "read current destination installation and instance before approving a service import", hostv1.WithResource("appliance_installation", installation.ID), hostv1.WithMetadata(map[string]string{"admission": "not_accepted"}))
	}
	if err := server.approvedWebhookFrontend(ctx, installation.ID); err != nil {
		return nil, err
	}
	source, err := server.store.GetEnvironment(ctx, request.GetSourceEnvironmentId())
	if err != nil {
		return nil, err
	}
	sourceContainer, err := server.services.ServiceContainerIdentity(ctx, source.NamespaceID, request.GetSourceContainerId(), request.GetSourceContainerInstanceId())
	if err != nil {
		return nil, err
	}
	if server.trust == nil {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_TRUST_UNAVAILABLE, "device trust is unavailable", hostv1.WithResource("device", "local"))
	}
	device, err := server.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	importID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	routeID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	leaseID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	capability := servicenet.LocalServiceRouteCapability(request.GetRequestId(), device.Identity.GetDeviceId(), "", "", false)
	capability.PolicyRevision = grant.Revision
	value := &servicev1.ServiceImport{ImportId: importID, SourceEnvironmentId: source.ID, SourceContainerId: request.GetSourceContainerId(), SourceContainerInstanceId: request.GetSourceContainerInstanceId(), SourceContainerGenerationId: sourceContainer.GetGenerationId(), DestinationDeviceId: device.Identity.GetDeviceId(), DestinationFrontend: &servicev1.ApplianceFrontendDestination{InstallationId: installation.ID, Name: request.GetFrontendName()}, VirtualServiceName: request.GetVirtualServiceName(), DestinationPort: 80}
	route := &servicev1.AccessRoute{RouteId: routeID, RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, SourceDeviceId: device.Identity.GetDeviceId(), DestinationDeviceId: device.Identity.GetDeviceId(), SourceEnvironmentId: source.ID, SourceContainerInstanceId: request.GetSourceContainerInstanceId(), DestinationInstallationId: installation.ID, DestinationBootId: installation.CurrentBootID, DestinationContainerInstanceId: installation.CurrentContainerInstance, DestinationGrantRevision: grant.Revision, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: capability.GetExpiresAt()}
	lease := &servicev1.RouteLease{LeaseId: leaseID, RouteId: routeID, RouteRevision: 1, Generation: route.Generation, ExpiresAt: route.ExpiresAt, Capability: capability}
	created, err := server.store.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease)
	if err != nil {
		return nil, status.Error(codes.Aborted, "service request, alias or owner fence changed")
	}
	if !created.GetRevoked() {
		if err := server.refreshApplianceServiceImports(ctx, installation); err != nil {
			return nil, err
		}
	}
	return &appliancev1.CreateApplianceFrontendServiceImportResponse{Import: created}, nil
}
