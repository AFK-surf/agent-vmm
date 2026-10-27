//go:build darwin || linux

package appliance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (server *Owner) CreateApplianceServiceImport(ctx context.Context, request *appliancev1.CreateApplianceServiceImportRequest) (*appliancev1.CreateApplianceServiceImportResponse, error) {
	if request.GetRequestId() == "" || request.GetInstallationId() == "" || request.GetExpectedRevision() == 0 || request.GetExpectedGrantRevision() == 0 || request.GetExpectedBootId() == "" || request.GetExpectedContainerInstanceId() == "" || request.GetExpectedExportRevision() == 0 || !servicenet.ValidVirtualServiceName(request.GetVirtualServiceName()) {
		return nil, hostv1.InvalidField("service_import", "complete exact appliance/export identity and service alias are required")
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
	if prior, err := server.store.ReplayApplianceServiceImport(ctx, request); err == nil {
		return &appliancev1.CreateApplianceServiceImportResponse{Import: prior}, nil
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
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_APPLIANCE_SERVICE_SOURCE_CHANGED, "read current installation/grant/instance before approving a new service import", hostv1.WithResource("appliance_installation", installation.ID), hostv1.WithMetadata(map[string]string{"admission": "not_accepted"}))
	}
	destination, err := server.store.GetServiceExport(ctx, request.GetDestinationExportId())
	if err != nil || destination.GetRevoked() || destination.GetRevision() != request.GetExpectedExportRevision() {
		return nil, status.Error(codes.Aborted, "exact destination export is unavailable or changed")
	}
	environment, err := server.store.GetEnvironment(ctx, destination.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	if err := server.services.ValidateContainerInstance(ctx, environment.NamespaceID, destination.GetContainerId(), destination.GetContainerInstanceId()); err != nil {
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
	capability := servicenet.LocalServiceRouteCapability(request.GetRequestId(), device.Identity.GetDeviceId(), "", destination.GetExportId(), false)
	capability.PolicyRevision = grant.Revision
	value := &servicev1.ServiceImport{ImportId: importID, SourceInstallationId: installation.ID, SourceContainerId: systemContainerID(), DestinationDeviceId: device.Identity.GetDeviceId(), DestinationExportId: destination.GetExportId(), VirtualServiceName: request.GetVirtualServiceName(), DestinationPort: destination.GetPort()}
	route := &servicev1.AccessRoute{RouteId: routeID, RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, SourceDeviceId: device.Identity.GetDeviceId(), DestinationDeviceId: device.Identity.GetDeviceId(), DestinationExportId: destination.GetExportId(), State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: capability.GetExpiresAt(), SourceInstallationId: installation.ID, SourceBootId: installation.CurrentBootID, SourceContainerInstanceId: installation.CurrentContainerInstance, PolicyRevision: grant.Revision}
	lease := &servicev1.RouteLease{LeaseId: leaseID, RouteId: routeID, RouteRevision: 1, Generation: route.Generation, ExpiresAt: route.ExpiresAt, Capability: capability}
	created, err := server.store.CreateApplianceServiceImport(ctx, request, systemNamespaceID(installation.ID), value, route, lease)
	if err != nil {
		return nil, status.Error(codes.Aborted, "service request, alias, or owner fence changed")
	}
	if !created.GetRevoked() {
		if err := server.refreshApplianceServiceImports(ctx, installation); err != nil {
			return nil, err
		}
	}
	return &appliancev1.CreateApplianceServiceImportResponse{Import: created}, nil
}

func (server *Owner) ListApplianceServiceImports(ctx context.Context, request *appliancev1.ListApplianceServiceImportsRequest) (*appliancev1.ListApplianceServiceImportsResponse, error) {
	if _, err := server.store.GetApplianceInstallation(ctx, request.GetInstallationId()); err != nil {
		return nil, err
	}
	values, err := server.store.ListApplianceServiceImports(ctx, request.GetInstallationId())
	if err != nil {
		return nil, err
	}
	return &appliancev1.ListApplianceServiceImportsResponse{Imports: values}, nil
}

func (server *Owner) RevokeApplianceServiceImport(ctx context.Context, request *appliancev1.RevokeApplianceServiceImportRequest) (*appliancev1.RevokeApplianceServiceImportResponse, error) {
	if request.GetRequestId() == "" || request.GetInstallationId() == "" || request.GetImportId() == "" || request.GetExpectedInstallationRevision() == 0 || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("service_import", "exact installation/import revision and request ID are required")
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
	if err := server.services.ProjectionLock().Lock(ctx); err != nil {
		return nil, err
	}

	defer server.services.ProjectionLock().Unlock()
	if err := server.store.RevokeApplianceServiceImport(ctx, request); err != nil {
		return nil, status.Error(codes.Aborted, "service import owner, request, or revision changed")
	}
	if err := server.services.DeleteServiceProjection(ctx, request.GetRequestId(), request.GetImportId()); err != nil {
		return nil, err
	}
	return &appliancev1.RevokeApplianceServiceImportResponse{}, nil
}

func (controller *Owner) withdrawApplianceServiceImports(ctx context.Context, installationID string) error {
	if err := controller.services.ProjectionLock().Lock(ctx); err != nil {
		return err
	}

	defer controller.services.ProjectionLock().Unlock()
	ids, err := controller.store.WithdrawApplianceServiceImports(ctx, installationID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		controller.egress.DeleteServiceRoute(id)
	}
	var result error
	if controller.manager == nil || controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return nil
	}
	for _, id := range ids {
		record, err := controller.store.ServiceRouteForImport(ctx, id)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		requestID := opreceipt.DerivedRequestID(fmt.Sprintf("withdraw-appliance-service/%s/%s/%d", installationID, id, record.Route.GetGeneration()))
		result = errors.Join(result, controller.services.DeleteServiceProjection(ctx, requestID, id))
	}
	return result
}

func (controller *Owner) refreshApplianceServiceImports(ctx context.Context, installation store.ApplianceInstallation) error {
	if err := controller.services.ProjectionLock().Lock(ctx); err != nil {
		return err
	}

	defer controller.services.ProjectionLock().Unlock()
	imports, err := controller.store.ListApplianceServiceImports(ctx, installation.ID)
	if err != nil {
		return err
	}
	if len(imports) == 0 {
		return nil
	}
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) {
		return nil
	}
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return err
	}
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	for _, value := range imports {
		if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, value); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			controller.logger.Warn("appliance service import refresh failed", "installation_id", installation.ID, "import_id", value.GetImportId(), "error", err)
		}
	}
	return nil
}

func (controller *Owner) refreshApplianceServiceImportLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, current store.ApplianceInstallation, grant store.ApplianceGrant, value *servicev1.ServiceImport) (importErr error) {
	defer func() {
		if importErr == nil {
			return
		}
		if ctx.Err() != nil {
			importErr = ctx.Err()
			return
		}
		controller.logger.Warn("appliance service import unavailable", "installation_id", current.ID, "import_id", value.GetImportId(), "error", importErr)
		if err := controller.store.SuspendServiceImportRoute(ctx, value.GetImportId()); err != nil {
			importErr = err
			return
		}
		record, err := controller.store.ServiceRouteForImport(ctx, value.GetImportId())
		if err != nil {
			importErr = err
			return
		}
		controller.egress.DeleteServiceRoute(value.GetImportId())
		_, importErr = client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: opreceipt.DerivedRequestID(fmt.Sprintf("appliance-service-unavailable/%s/%d/%d", value.GetImportId(), record.Route.GetRevision(), record.Route.GetGeneration())), ProjectionId: value.GetImportId()})
		if status.Code(importErr) == codes.NotFound {
			importErr = nil
		}
	}()
	if value.GetRevoked() {
		if err := controller.services.DeleteServiceProjection(ctx, opreceipt.DerivedRequestID(fmt.Sprintf("appliance-service-revoked/%s/%d", value.GetImportId(), value.GetRevision())), value.GetImportId()); err != nil {
			return err
		}
		return nil
	}
	if value.GetDestinationFrontend() != nil {
		if err := controller.refreshApplianceFrontendServiceImportLocked(ctx, client, current, grant, value); err != nil {
			return err
		}
		return nil
	}
	record, err := controller.store.ServiceRouteForImport(ctx, value.GetImportId())
	if err != nil {
		return err
	}
	if record.Export.GetRevoked() {
		if err := controller.services.DeleteServiceProjection(ctx, opreceipt.DerivedRequestID("appliance-service-export-revoked/"+value.GetImportId()), value.GetImportId()); err != nil {
			return err
		}
		return nil
	}
	destination, err := controller.store.GetEnvironment(ctx, record.Export.GetEnvironmentId())
	if err != nil {
		return err
	}
	valid, observationErr := controller.services.RevalidateServiceExportLocked(ctx, client, record.Export, destination.NamespaceID)
	if observationErr != nil {
		return observationErr
	}
	if !valid {
		return nil
	}
	record, err = controller.store.ServiceRouteForImport(ctx, value.GetImportId())
	if err != nil {
		return err
	}
	route := record.Route
	lease := record.Lease
	if route.GetState() != servicev1.RouteState_ROUTE_STATE_ACTIVE || route.GetSourceBootId() != current.CurrentBootID || route.GetSourceContainerInstanceId() != current.CurrentContainerInstance || route.GetPolicyRevision() != grant.Revision || route.GetExpiresAt().AsTime().Before(time.Now().Add(time.Hour)) {
		route = proto.Clone(route).(*servicev1.AccessRoute)
		route.Revision++
		route.State = servicev1.RouteState_ROUTE_STATE_ACTIVE
		route.SourceBootId = current.CurrentBootID
		route.SourceContainerInstanceId = current.CurrentContainerInstance
		route.PolicyRevision = grant.Revision
		capability := servicenet.LocalServiceRouteCapability(opreceipt.DerivedRequestID(fmt.Sprintf("appliance-service/%s/%s/%d/%s/%s/%d", current.ID, value.GetImportId(), route.Revision, current.CurrentBootID, current.CurrentContainerInstance, grant.Revision)), route.GetSourceDeviceId(), "", value.GetDestinationExportId(), false)
		capability.PolicyRevision = grant.Revision
		route.Generation = capability.GetRouteGeneration()
		route.ExpiresAt = capability.GetExpiresAt()
		lease = proto.Clone(lease).(*servicev1.RouteLease)
		lease.RouteRevision = route.Revision
		lease.Generation = route.Generation
		lease.ExpiresAt = route.ExpiresAt
		lease.Capability = proto.Clone(capability).(*trustv1.RouteCapability)
		if err := controller.store.RenewApplianceServiceRoute(ctx, current, grant.Revision, record.Route.GetRevision(), route, lease); err != nil {
			return err
		}
	}
	projection := &vmmdv1.ServiceProjection{Id: value.GetImportId(), SourceNamespaceId: systemNamespaceID(current.ID), SourceContainerId: systemContainerID(), SourceContainerInstanceId: current.CurrentContainerInstance, DestinationNamespaceId: destination.NamespaceID, DestinationContainerId: record.Export.GetContainerId(), DestinationContainerInstanceId: record.Export.GetContainerInstanceId(), DestinationPort: record.Export.GetPort(), DnsAlias: value.GetVirtualServiceName(), ExpiresUnixMillis: route.GetExpiresAt().AsTime().UnixMilli()}
	if _, err := client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{RequestId: opreceipt.DerivedRequestID(fmt.Sprintf("appliance-service-project/%s/%d", value.GetImportId(), route.Generation)), Projection: projection}); err != nil {
		return err
	}
	return nil
}
