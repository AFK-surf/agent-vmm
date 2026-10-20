package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ServiceRouteRecord struct {
	Route      *servicev1.AccessRoute
	Lease      *servicev1.RouteLease
	Capability *trustv1.RouteCapability
	Import     *servicev1.ServiceImport
	Export     *servicev1.ServiceExport
}

type ServiceProjectionRecord struct {
	Import               *servicev1.ServiceImport
	Export               *servicev1.ServiceExport
	SourceNamespace      string
	DestinationNamespace string
	ExpiresAt            time.Time
	Generation           uint64
}

type RemoteServiceProjectionRecord struct {
	Import          *servicev1.ServiceImport
	Route           *servicev1.AccessRoute
	Lease           *servicev1.RouteLease
	Capability      *trustv1.RouteCapability
	SourceNamespace string
}

func (store *Store) CreateServiceExport(ctx context.Context, requestID string, value *servicev1.ServiceExport) (*servicev1.ServiceExport, error) {
	if existing, err := store.serviceExportByRequest(ctx, requestID); err == nil {
		if existing.GetEnvironmentId() != value.GetEnvironmentId() || existing.GetContainerId() != value.GetContainerId() || existing.GetPort() != value.GetPort() || existing.GetProtocol() != value.GetProtocol() {
			return nil, ErrTrustRevisionConflict
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	now := time.Now()
	_, err := store.db.ExecContext(ctx, `INSERT INTO service_exports(export_id, request_id, environment_id, container_id,
		container_instance_id, port, protocol, revision, revoked, created_at, updated_at, container_generation_id) VALUES(?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?)`,
		value.GetExportId(), requestID, value.GetEnvironmentId(), value.GetContainerId(), value.GetContainerInstanceId(),
		value.GetPort(), value.GetProtocol(), now.UnixNano(), now.UnixNano(), value.GetContainerGenerationId())
	if err != nil {
		return nil, err
	}
	return store.GetServiceExport(ctx, value.GetExportId())
}

func scanServiceExport(row rowScanner) (*servicev1.ServiceExport, error) {
	value := new(servicev1.ServiceExport)
	var protocol int32
	var revoked bool
	var created int64
	err := row.Scan(&value.ExportId, &value.EnvironmentId, &value.ContainerId, &value.ContainerInstanceId, &value.Port,
		&protocol, &value.Revision, &revoked, &created, &value.ContainerGenerationId)
	value.Protocol = servicev1.ServiceProtocol(protocol)
	value.Revoked = revoked
	value.CreatedAt = timestamppb.New(time.Unix(0, created))
	return value, err
}

func (store *Store) GetServiceExport(ctx context.Context, exportID string) (*servicev1.ServiceExport, error) {
	return scanServiceExport(store.db.QueryRowContext(ctx, `SELECT export_id, environment_id, container_id, container_instance_id,
		port, protocol, revision, revoked, created_at, container_generation_id FROM service_exports WHERE export_id=?`, exportID))
}

func (store *Store) serviceExportByRequest(ctx context.Context, requestID string) (*servicev1.ServiceExport, error) {
	return scanServiceExport(store.db.QueryRowContext(ctx, `SELECT export_id, environment_id, container_id, container_instance_id,
		port, protocol, revision, revoked, created_at, container_generation_id FROM service_exports WHERE request_id=?`, requestID))
}

func (store *Store) ListServiceExports(ctx context.Context, environmentID string) ([]*servicev1.ServiceExport, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT export_id, environment_id, container_id, container_instance_id,
		port, protocol, revision, revoked, created_at, container_generation_id FROM service_exports WHERE environment_id=? ORDER BY export_id`, environmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*servicev1.ServiceExport
	for rows.Next() {
		value, err := scanServiceExport(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (store *Store) RevokeServiceExport(ctx context.Context, environmentID, exportID string, expectedRevision uint64) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE service_exports SET revoked=1, revision=revision+1, updated_at=?
		WHERE export_id=? AND environment_id=? AND revision=? AND revoked=0`, time.Now().UnixNano(), exportID, environmentID, expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1,revision=revision+1,updated_at=? WHERE destination_export_id=? AND revoked=0`, time.Now().UnixNano(), exportID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE access_routes SET state=?, revision=revision+1, generation=generation+1, updated_at=?
		WHERE destination_export_id=? AND state NOT IN (?, ?)`, servicev1.RouteState_ROUTE_STATE_REVOKED, time.Now().UnixNano(), exportID,
		servicev1.RouteState_ROUTE_STATE_REVOKED, servicev1.RouteState_ROUTE_STATE_EXPIRED)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) CreateServiceImport(ctx context.Context, requestID string, value *servicev1.ServiceImport, route *servicev1.AccessRoute, lease *servicev1.RouteLease) (*servicev1.ServiceImport, error) {
	if value.GetDestinationFrontend() != nil {
		return nil, ErrTrustRevisionConflict
	}
	return store.createServiceImport(ctx, requestID, value, route, lease, nil, "")
}

func (store *Store) CreateApplianceServiceImport(ctx context.Context, request *appliancev1.CreateApplianceServiceImportRequest, namespace string, value *servicev1.ServiceImport, route *servicev1.AccessRoute, lease *servicev1.RouteLease) (*servicev1.ServiceImport, error) {
	if value.GetDestinationFrontend() != nil {
		return nil, ErrTrustRevisionConflict
	}
	return store.createServiceImport(ctx, request.GetRequestId(), value, route, lease, request, namespace)
}

func (store *Store) createServiceImport(ctx context.Context, requestID string, value *servicev1.ServiceImport, route *servicev1.AccessRoute, lease *servicev1.RouteLease, source *appliancev1.CreateApplianceServiceImportRequest, namespace string) (*servicev1.ServiceImport, error) {
	if existing, err := store.serviceImportByRequest(ctx, requestID); err == nil {
		if existing.GetSourceInstallationId() != value.GetSourceInstallationId() || existing.GetSourceEnvironmentId() != value.GetSourceEnvironmentId() || existing.GetSourceContainerId() != value.GetSourceContainerId() || existing.GetDestinationDeviceId() != value.GetDestinationDeviceId() || existing.GetDestinationExportId() != value.GetDestinationExportId() || existing.GetVirtualServiceName() != value.GetVirtualServiceName() || existing.GetDestinationPort() != value.GetDestinationPort() {
			return nil, ErrTrustRevisionConflict
		}
		if source != nil {
			return store.ReplayApplianceServiceImport(ctx, source)
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	capabilityPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(lease.GetCapability())
	if err != nil {
		return nil, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if source != nil {
		var matched int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM appliance_installations a JOIN appliance_grants g ON g.installation_id=a.id JOIN service_exports e ON e.export_id=?
 WHERE a.id=? AND a.revision=? AND g.revision=? AND a.current_boot_id=? AND a.current_container_instance=? AND a.desired_enabled=1 AND a.state=3 AND e.revoked=0 AND e.revision=?`, source.GetDestinationExportId(), source.GetInstallationId(), source.GetExpectedRevision(), source.GetExpectedGrantRevision(), source.GetExpectedBootId(), source.GetExpectedContainerInstanceId(), source.GetExpectedExportRevision()).Scan(&matched); err != nil {
			return nil, err
		}
		if matched != 1 {
			return nil, ErrInstallationRevisionConflict
		}
	}
	var requestHash []byte
	if source != nil {
		body, _ := proto.MarshalOptions{Deterministic: true}.Marshal(source)
		hash := sha256.Sum256(body)
		requestHash = hash[:]
	}
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_imports(import_id, request_id, source_environment_id, source_container_id,
		source_container_instance_id, destination_device_id, destination_export_id, virtual_service_name, destination_port, revision, revoked, created_at, updated_at,source_installation_id,source_namespace_id,request_sha256,source_container_generation_id)
		VALUES(?, ?, NULLIF(?,''), ?, ?, ?, ?, ?, ?, 1, 0, ?, ?,NULLIF(?,''),?,?,?)`, value.GetImportId(), requestID, value.GetSourceEnvironmentId(), value.GetSourceContainerId(),
		value.GetSourceContainerInstanceId(), value.GetDestinationDeviceId(), value.GetDestinationExportId(), value.GetVirtualServiceName(), value.GetDestinationPort(), now, now, value.GetSourceInstallationId(), namespace, requestHash, value.GetSourceContainerGenerationId()); err != nil {
		return nil, err
	}
	if err := insertServiceRoute(ctx, tx, value.GetImportId(), route, lease, capabilityPayload, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return store.GetServiceImport(ctx, value.GetImportId())
}

func scanServiceImport(row rowScanner) (*servicev1.ServiceImport, error) {
	value := new(servicev1.ServiceImport)
	var revoked bool
	var installation, frontend string
	err := row.Scan(&value.ImportId, &value.SourceEnvironmentId, &value.SourceContainerId, &value.SourceContainerInstanceId,
		&value.DestinationDeviceId, &value.DestinationExportId, &value.VirtualServiceName, &value.DestinationPort, &value.Revision, &revoked, &value.SourceInstallationId, &installation, &frontend, &value.SourceContainerGenerationId)
	if installation != "" {
		value.DestinationFrontend = &servicev1.ApplianceFrontendDestination{InstallationId: installation, Name: frontend}
	}
	value.Revoked = revoked
	return value, err
}

func (store *Store) GetServiceImport(ctx context.Context, importID string) (*servicev1.ServiceImport, error) {
	return scanServiceImport(store.db.QueryRowContext(ctx, `SELECT import_id, COALESCE(source_environment_id,''), source_container_id,
		source_container_instance_id, destination_device_id, destination_export_id, virtual_service_name, destination_port, revision, revoked,COALESCE(source_installation_id,''),COALESCE(destination_installation_id,''),destination_frontend_name,source_container_generation_id
		FROM service_imports WHERE import_id=?`, importID))
}

func (store *Store) serviceImportByRequest(ctx context.Context, requestID string) (*servicev1.ServiceImport, error) {
	return scanServiceImport(store.db.QueryRowContext(ctx, `SELECT import_id, COALESCE(source_environment_id,''), source_container_id,
		source_container_instance_id, destination_device_id, destination_export_id, virtual_service_name, destination_port, revision, revoked,COALESCE(source_installation_id,''),COALESCE(destination_installation_id,''),destination_frontend_name,source_container_generation_id
		FROM service_imports WHERE request_id=?`, requestID))
}

func (store *Store) ListServiceImports(ctx context.Context, environmentID string) ([]*servicev1.ServiceImport, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT import_id, COALESCE(source_environment_id,''), source_container_id,
		source_container_instance_id, destination_device_id, destination_export_id, virtual_service_name, destination_port, revision, revoked,COALESCE(source_installation_id,''),COALESCE(destination_installation_id,''),destination_frontend_name,source_container_generation_id
		FROM service_imports WHERE source_environment_id=? ORDER BY import_id`, environmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*servicev1.ServiceImport
	for rows.Next() {
		value, err := scanServiceImport(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (store *Store) ListActiveServiceImportsForExport(ctx context.Context, exportID string) ([]string, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT import_id FROM service_imports WHERE destination_export_id=? AND revoked=0 ORDER BY import_id`, exportID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func (store *Store) ListActiveServiceProjections(ctx context.Context, _ time.Time) ([]ServiceProjectionRecord, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT i.import_id, COALESCE(i.source_environment_id,''), i.source_container_id,
		CASE WHEN i.source_installation_id IS NULL THEN i.source_container_instance_id ELSE r.source_container_instance_id END, i.destination_device_id, i.destination_export_id, i.virtual_service_name, i.destination_port, i.revision, i.revoked,COALESCE(i.source_installation_id,''),
		e.export_id, e.environment_id, e.container_id, e.container_instance_id, e.port, e.protocol, e.revision, e.revoked, e.created_at,
		COALESCE(se.namespace_id,i.source_namespace_id), de.namespace_id, l.expires_at, r.generation, i.source_container_generation_id, e.container_generation_id
		FROM service_imports i
		JOIN service_exports e ON e.export_id=i.destination_export_id
		JOIN access_routes r ON r.import_id=i.import_id
		JOIN route_leases l ON l.route_id=r.route_id
		LEFT JOIN environments se ON se.id=i.source_environment_id
 LEFT JOIN appliance_installations a ON a.id=i.source_installation_id
 LEFT JOIN appliance_grants g ON g.installation_id=a.id
		JOIN environments de ON de.id=e.environment_id
		WHERE i.revoked=0 AND e.revoked=0 AND r.state IN (?,1)
		AND (i.source_installation_id IS NULL OR (a.desired_enabled=1 AND a.state=3 AND g.revision=r.policy_revision AND a.current_boot_id=r.source_boot_id AND a.current_container_instance=r.source_container_instance_id))
 AND r.generation=l.generation ORDER BY i.import_id`,
		servicev1.RouteState_ROUTE_STATE_ACTIVE)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ServiceProjectionRecord
	for rows.Next() {
		serviceImport, serviceExport := new(servicev1.ServiceImport), new(servicev1.ServiceExport)
		var importRevoked, exportRevoked bool
		var protocol int32
		var created int64
		var sourceNamespace, destinationNamespace string
		var expiresAt int64
		var generation uint64
		if err := rows.Scan(&serviceImport.ImportId, &serviceImport.SourceEnvironmentId, &serviceImport.SourceContainerId,
			&serviceImport.SourceContainerInstanceId, &serviceImport.DestinationDeviceId, &serviceImport.DestinationExportId,
			&serviceImport.VirtualServiceName, &serviceImport.DestinationPort, &serviceImport.Revision, &importRevoked, &serviceImport.SourceInstallationId, &serviceExport.ExportId, &serviceExport.EnvironmentId,
			&serviceExport.ContainerId, &serviceExport.ContainerInstanceId, &serviceExport.Port, &protocol, &serviceExport.Revision,
			&exportRevoked, &created, &sourceNamespace, &destinationNamespace, &expiresAt, &generation, &serviceImport.SourceContainerGenerationId, &serviceExport.ContainerGenerationId); err != nil {
			return nil, err
		}
		serviceImport.Revoked = importRevoked
		serviceExport.Protocol, serviceExport.Revoked, serviceExport.CreatedAt = servicev1.ServiceProtocol(protocol), exportRevoked, timestamppb.New(time.Unix(0, created))
		result = append(result, ServiceProjectionRecord{Import: serviceImport, Export: serviceExport, SourceNamespace: sourceNamespace, DestinationNamespace: destinationNamespace, ExpiresAt: time.Unix(0, expiresAt), Generation: generation})
	}
	return result, rows.Err()
}

func (store *Store) ListActiveRemoteServiceProjections(ctx context.Context, localDeviceID string, observedAt time.Time) ([]RemoteServiceProjectionRecord, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT i.import_id, i.source_environment_id, i.source_container_id,
		i.source_container_instance_id, i.destination_device_id, i.destination_export_id, i.virtual_service_name, i.destination_port, i.revision, i.revoked,
		r.route_id, r.route_class, r.source_device_id, COALESCE(r.source_allocation_id, ''), r.destination_device_id,
		r.destination_export_id, r.state, r.revision, r.generation, r.expires_at, r.source_environment_id,
		r.source_container_instance_id, r.policy_revision, r.capability,
		l.lease_id, l.route_revision, l.generation, l.expires_at, se.namespace_id, i.source_container_generation_id
		FROM service_imports i JOIN access_routes r ON r.import_id=i.import_id
		JOIN route_leases l ON l.route_id=r.route_id JOIN environments se ON se.id=i.source_environment_id
		WHERE i.revoked=0 AND i.destination_installation_id IS NULL AND i.destination_device_id<>? AND i.destination_port>0 AND r.state IN (?,1)
		AND r.expires_at>? AND l.expires_at>? AND r.generation=l.generation
		ORDER BY i.import_id`, localDeviceID, servicev1.RouteState_ROUTE_STATE_ACTIVE, observedAt.UnixNano(), observedAt.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RemoteServiceProjectionRecord
	for rows.Next() {
		serviceImport, route, lease, capability := new(servicev1.ServiceImport), new(servicev1.AccessRoute), new(servicev1.RouteLease), new(trustv1.RouteCapability)
		var importRevoked bool
		var routeClass, routeState int32
		var routeExpiry, leaseExpiry int64
		var capabilityPayload []byte
		var sourceNamespace string
		var sourceAllocation string
		if err := rows.Scan(&serviceImport.ImportId, &serviceImport.SourceEnvironmentId, &serviceImport.SourceContainerId,
			&serviceImport.SourceContainerInstanceId, &serviceImport.DestinationDeviceId, &serviceImport.DestinationExportId,
			&serviceImport.VirtualServiceName, &serviceImport.DestinationPort, &serviceImport.Revision, &importRevoked,
			&route.RouteId, &routeClass, &route.SourceDeviceId, &sourceAllocation, &route.DestinationDeviceId,
			&route.DestinationExportId, &routeState, &route.Revision, &route.Generation, &routeExpiry, &route.SourceEnvironmentId,
			&route.SourceContainerInstanceId, &route.PolicyRevision, &capabilityPayload,
			&lease.LeaseId, &lease.RouteRevision, &lease.Generation, &leaseExpiry, &sourceNamespace, &serviceImport.SourceContainerGenerationId); err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(capabilityPayload, capability); err != nil {
			return nil, err
		}
		serviceImport.Revoked = importRevoked
		if sourceAllocation != "" {
			route.SourceAllocationId = &sourceAllocation
		}
		route.RouteClass, route.State, route.ExpiresAt = servicev1.RouteClass(routeClass), servicev1.RouteState(routeState), timestamppb.New(time.Unix(0, routeExpiry))
		lease.RouteId, lease.ExpiresAt, lease.Capability = route.RouteId, timestamppb.New(time.Unix(0, leaseExpiry)), capability
		result = append(result, RemoteServiceProjectionRecord{Import: serviceImport, Route: route, Lease: lease, Capability: capability, SourceNamespace: sourceNamespace})
	}
	return result, rows.Err()
}

func (store *Store) RevokeServiceImport(ctx context.Context, environmentID, importID string, expectedRevision uint64) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1, revision=revision+1, updated_at=?
		WHERE import_id=? AND source_environment_id=? AND revision=? AND revoked=0`, time.Now().UnixNano(), importID, environmentID, expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?, revision=revision+1, generation=generation+1, updated_at=?
		WHERE import_id=?`, servicev1.RouteState_ROUTE_STATE_REVOKED, time.Now().UnixNano(), importID); err != nil {
		return err
	}
	return tx.Commit()
}
