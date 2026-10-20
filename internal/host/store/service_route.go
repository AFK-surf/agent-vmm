package store

import (
	"context"
	"database/sql"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (store *Store) GetServiceRoute(ctx context.Context, routeID, leaseID string, generation uint64) (ServiceRouteRecord, error) {
	var importID string
	if err := store.db.QueryRowContext(ctx, `SELECT import_id FROM access_routes WHERE route_id=?`, routeID).Scan(&importID); err != nil {
		return ServiceRouteRecord{}, err
	}
	value, err := store.GetServiceImport(ctx, importID)
	if err != nil {
		return ServiceRouteRecord{}, err
	}
	if value.GetDestinationFrontend() != nil {
		return store.applianceFrontendServiceRoute(ctx, value, routeID, leaseID, generation)
	}
	var record ServiceRouteRecord
	var routeClass, state int32
	var routeExpiry, leaseExpiry int64
	var capabilityPayload []byte
	row := store.db.QueryRowContext(ctx, `SELECT r.route_id, r.route_class, r.source_device_id, COALESCE(r.source_allocation_id, ''),
		r.destination_device_id, r.destination_export_id, r.state, r.revision, r.generation, r.expires_at,
		r.source_environment_id, r.source_container_instance_id, r.policy_revision,r.source_installation_id,r.source_boot_id,
		l.lease_id, l.route_revision, l.generation, l.expires_at, l.capability,
		i.import_id, COALESCE(i.source_environment_id,''), i.source_container_id, i.source_container_instance_id,
		i.destination_device_id, i.destination_export_id, i.virtual_service_name, i.revision, i.revoked,COALESCE(i.source_installation_id,''),
		COALESCE(e.export_id,''),COALESCE(e.environment_id,''),COALESCE(e.container_id,''),COALESCE(e.container_instance_id,''),COALESCE(e.port,0),COALESCE(e.protocol,0),COALESCE(e.revision,0),COALESCE(e.revoked,0),COALESCE(e.created_at,0), i.source_container_generation_id, COALESCE(e.container_generation_id,''),i.destination_port
		FROM access_routes r
		JOIN route_leases l ON l.route_id=r.route_id
		JOIN service_imports i ON i.import_id=r.import_id
		LEFT JOIN service_exports e ON e.export_id=r.destination_export_id
		WHERE r.route_id=? AND l.lease_id=? AND r.generation=?`, routeID, leaseID, generation)
	route := new(servicev1.AccessRoute)
	lease := new(servicev1.RouteLease)
	serviceImport := new(servicev1.ServiceImport)
	serviceExport := new(servicev1.ServiceExport)
	var sourceAllocationValue string
	var importRevoked, exportRevoked bool
	var exportProtocol int32
	var exportCreated int64
	if err := row.Scan(&route.RouteId, &routeClass, &route.SourceDeviceId, &sourceAllocationValue, &route.DestinationDeviceId,
		&route.DestinationExportId, &state, &route.Revision, &route.Generation, &routeExpiry, &route.SourceEnvironmentId,
		&route.SourceContainerInstanceId, &route.PolicyRevision, &route.SourceInstallationId, &route.SourceBootId, &lease.LeaseId, &lease.RouteRevision, &lease.Generation,
		&leaseExpiry, &capabilityPayload, &serviceImport.ImportId, &serviceImport.SourceEnvironmentId, &serviceImport.SourceContainerId,
		&serviceImport.SourceContainerInstanceId, &serviceImport.DestinationDeviceId, &serviceImport.DestinationExportId,
		&serviceImport.VirtualServiceName, &serviceImport.Revision, &importRevoked, &serviceImport.SourceInstallationId, &serviceExport.ExportId, &serviceExport.EnvironmentId,
		&serviceExport.ContainerId, &serviceExport.ContainerInstanceId, &serviceExport.Port, &exportProtocol, &serviceExport.Revision,
		&exportRevoked, &exportCreated, &serviceImport.SourceContainerGenerationId, &serviceExport.ContainerGenerationId, &serviceImport.DestinationPort); err != nil {
		return ServiceRouteRecord{}, err
	}
	route.RouteClass, route.State = servicev1.RouteClass(routeClass), servicev1.RouteState(state)
	if sourceAllocationValue != "" {
		route.SourceAllocationId = &sourceAllocationValue
	}
	route.ExpiresAt = timestamppb.New(time.Unix(0, routeExpiry))
	lease.RouteId, lease.ExpiresAt = route.RouteId, timestamppb.New(time.Unix(0, leaseExpiry))
	capability := new(trustv1.RouteCapability)
	if err := proto.Unmarshal(capabilityPayload, capability); err != nil {
		return ServiceRouteRecord{}, err
	}
	lease.Capability = capability
	serviceImport.Revoked = importRevoked
	serviceExport.Protocol, serviceExport.Revoked = servicev1.ServiceProtocol(exportProtocol), exportRevoked
	serviceExport.CreatedAt = timestamppb.New(time.Unix(0, exportCreated))
	record.Route, record.Lease, record.Capability, record.Import, record.Export = route, lease, capability, serviceImport, serviceExport
	return record, nil
}

func insertServiceRoute(ctx context.Context, tx *sql.Tx, importID string, route *servicev1.AccessRoute, lease *servicev1.RouteLease, capabilityPayload []byte, now int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_routes(route_id, import_id, route_class, source_device_id, source_allocation_id,
		destination_device_id, destination_export_id, state, revision, generation, expires_at, source_environment_id,
		source_container_instance_id, policy_revision, capability, updated_at,source_installation_id,source_boot_id) VALUES(?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)`,
		route.GetRouteId(), importID, route.GetRouteClass(), route.GetSourceDeviceId(), route.GetSourceAllocationId(),
		route.GetDestinationDeviceId(), route.GetDestinationExportId(), route.GetState(), route.GetRevision(), route.GetGeneration(),
		route.GetExpiresAt().AsTime().UnixNano(), route.GetSourceEnvironmentId(), route.GetSourceContainerInstanceId(), route.GetPolicyRevision(), capabilityPayload, now, route.GetSourceInstallationId(), route.GetSourceBootId()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO route_leases(lease_id, route_id, route_revision, generation, expires_at, capability, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, lease.GetLeaseId(), lease.GetRouteId(), lease.GetRouteRevision(), lease.GetGeneration(),
		lease.GetExpiresAt().AsTime().UnixNano(), capabilityPayload, now); err != nil {
		return err
	}
	return nil
}
