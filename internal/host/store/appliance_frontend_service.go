package store

import (
	"bytes"
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

func (store *Store) ReplayApplianceFrontendServiceImport(ctx context.Context, request *appliancev1.CreateApplianceFrontendServiceImportRequest) (*servicev1.ServiceImport, error) {
	value, err := store.serviceImportByRequest(ctx, request.GetRequestId())
	if err != nil {
		return nil, err
	}
	var stored []byte
	if err := store.db.QueryRowContext(ctx, `SELECT request_sha256 FROM service_imports WHERE request_id=?`, request.GetRequestId()).Scan(&stored); err != nil {
		return nil, err
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(body)
	if value.GetDestinationFrontend().GetInstallationId() != request.GetInstallationId() || !bytes.Equal(stored, hash[:]) {
		return nil, ErrTrustRevisionConflict
	}
	return value, nil
}

func (store *Store) CreateApplianceFrontendServiceImport(ctx context.Context, request *appliancev1.CreateApplianceFrontendServiceImportRequest, value *servicev1.ServiceImport, route *servicev1.AccessRoute, lease *servicev1.RouteLease) (*servicev1.ServiceImport, error) {
	if existing, err := store.ReplayApplianceFrontendServiceImport(ctx, request); err == nil {
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	destination := value.GetDestinationFrontend()
	if request.GetFrontendName() != "webhook" || destination.GetName() != "webhook" || destination.GetInstallationId() != request.GetInstallationId() || value.GetDestinationExportId() != "" || value.GetSourceInstallationId() != "" || value.GetSourceEnvironmentId() != request.GetSourceEnvironmentId() || value.GetSourceContainerId() != request.GetSourceContainerId() || value.GetSourceContainerInstanceId() != request.GetSourceContainerInstanceId() || value.GetVirtualServiceName() != request.GetVirtualServiceName() || value.GetDestinationPort() != 80 || request.GetSourceContainerInstanceId() == "" || route.GetDestinationInstallationId() != request.GetInstallationId() || route.GetDestinationBootId() != request.GetExpectedBootId() || route.GetDestinationContainerInstanceId() != request.GetExpectedContainerInstanceId() || route.GetDestinationGrantRevision() != request.GetExpectedGrantRevision() || route.GetSourceEnvironmentId() != value.GetSourceEnvironmentId() || route.GetSourceContainerInstanceId() != value.GetSourceContainerInstanceId() {
		return nil, ErrTrustRevisionConflict
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := matchApplianceFrontend(ctx, tx, request.GetInstallationId(), request.GetExpectedRevision(), request.GetExpectedGrantRevision(), request.GetExpectedBootId(), request.GetExpectedContainerInstanceId()); err != nil {
		return nil, err
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(body)
	now := time.Now().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_imports(import_id,request_id,source_environment_id,source_container_id,source_container_instance_id,destination_device_id,destination_export_id,virtual_service_name,destination_port,revision,revoked,created_at,updated_at,request_sha256,destination_installation_id,destination_frontend_name,source_container_generation_id) VALUES(?,?,?,?,?,?,'',?,80,1,0,?,?,?,?, 'webhook',?)`, value.GetImportId(), request.GetRequestId(), value.GetSourceEnvironmentId(), value.GetSourceContainerId(), value.GetSourceContainerInstanceId(), value.GetDestinationDeviceId(), value.GetVirtualServiceName(), now, now, hash[:], request.GetInstallationId(), value.GetSourceContainerGenerationId()); err != nil {
		return nil, err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(lease.GetCapability())
	if err != nil {
		return nil, err
	}
	if err := insertServiceRoute(ctx, tx, value.GetImportId(), route, lease, payload, now); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET destination_installation_id=?,destination_boot_id=?,destination_container_instance_id=?,destination_grant_revision=? WHERE route_id=?`, request.GetInstallationId(), request.GetExpectedBootId(), request.GetExpectedContainerInstanceId(), request.GetExpectedGrantRevision(), route.GetRouteId()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return store.GetServiceImport(ctx, value.GetImportId())
}

func matchApplianceFrontend(ctx context.Context, tx *sql.Tx, installationID string, revision, grantRevision uint64, bootID, instanceID string) error {
	var matched int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM appliance_installations a JOIN appliance_grants g ON g.installation_id=a.id JOIN appliance_frontends f ON f.installation_id=a.id WHERE a.id=? AND a.revision=? AND g.revision=? AND a.desired_enabled=1 AND a.state=3 AND a.current_boot_id=? AND a.current_container_instance=? AND f.name='webhook' AND f.transport=2 AND f.exposure!=2`, installationID, revision, grantRevision, bootID, instanceID).Scan(&matched); err != nil {
		return err
	}
	if matched != 1 {
		return ErrInstallationRevisionConflict
	}
	return nil
}

func (store *Store) RenewApplianceFrontendServiceRoute(ctx context.Context, installation ApplianceInstallation, grantRevision, expectedRouteRevision uint64, route *servicev1.AccessRoute, lease *servicev1.RouteLease) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if route.GetDestinationInstallationId() != installation.ID || route.GetDestinationBootId() != installation.CurrentBootID || route.GetDestinationContainerInstanceId() != installation.CurrentContainerInstance || route.GetDestinationGrantRevision() != grantRevision {
		return ErrTrustRevisionConflict
	}
	if err := matchApplianceFrontend(ctx, tx, installation.ID, installation.Revision, grantRevision, installation.CurrentBootID, installation.CurrentContainerInstance); err != nil {
		return err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(lease.GetCapability())
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=?,generation=?,expires_at=?,destination_boot_id=?,destination_container_instance_id=?,destination_grant_revision=?,capability=?,updated_at=? WHERE route_id=? AND revision=? AND destination_installation_id=? AND source_environment_id=? AND source_container_instance_id=? AND EXISTS(SELECT 1 FROM service_imports i WHERE i.import_id=access_routes.import_id AND i.revoked=0 AND i.destination_installation_id=? AND i.destination_frontend_name='webhook' AND i.source_environment_id=access_routes.source_environment_id AND i.source_container_instance_id=access_routes.source_container_instance_id)`, servicev1.RouteState_ROUTE_STATE_ACTIVE, route.GetRevision(), route.GetGeneration(), route.GetExpiresAt().AsTime().UnixNano(), installation.CurrentBootID, installation.CurrentContainerInstance, grantRevision, payload, time.Now().UnixNano(), route.GetRouteId(), expectedRouteRevision, installation.ID, route.GetSourceEnvironmentId(), route.GetSourceContainerInstanceId(), installation.ID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE route_leases SET route_revision=?,generation=?,expires_at=?,capability=? WHERE route_id=?`, route.GetRevision(), route.GetGeneration(), route.GetExpiresAt().AsTime().UnixNano(), payload, route.GetRouteId()); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) applianceFrontendServiceRoute(ctx context.Context, value *servicev1.ServiceImport, routeID, leaseID string, generation uint64) (ServiceRouteRecord, error) {
	route := new(servicev1.AccessRoute)
	lease := new(servicev1.RouteLease)
	capability := new(trustv1.RouteCapability)
	var routeClass, state int32
	var expiry, leaseExpiry int64
	var payload []byte
	err := store.db.QueryRowContext(ctx, `SELECT r.route_class,r.source_device_id,r.destination_device_id,r.state,r.revision,r.generation,r.expires_at,r.source_environment_id,r.source_container_instance_id,r.destination_installation_id,r.destination_boot_id,r.destination_container_instance_id,r.destination_grant_revision,l.route_revision,l.generation,l.expires_at,l.capability FROM access_routes r JOIN route_leases l ON l.route_id=r.route_id WHERE r.route_id=? AND l.lease_id=? AND r.generation=?`, routeID, leaseID, generation).Scan(&routeClass, &route.SourceDeviceId, &route.DestinationDeviceId, &state, &route.Revision, &route.Generation, &expiry, &route.SourceEnvironmentId, &route.SourceContainerInstanceId, &route.DestinationInstallationId, &route.DestinationBootId, &route.DestinationContainerInstanceId, &route.DestinationGrantRevision, &lease.RouteRevision, &lease.Generation, &leaseExpiry, &payload)
	if err != nil {
		return ServiceRouteRecord{}, err
	}
	if err := proto.Unmarshal(payload, capability); err != nil {
		return ServiceRouteRecord{}, err
	}
	route.RouteId = routeID
	route.RouteClass = servicev1.RouteClass(routeClass)
	route.State = servicev1.RouteState(state)
	route.ExpiresAt = timestamppb.New(time.Unix(0, expiry))
	lease.LeaseId = leaseID
	lease.RouteId = routeID
	lease.ExpiresAt = timestamppb.New(time.Unix(0, leaseExpiry))
	lease.Capability = capability
	return ServiceRouteRecord{Import: value, Route: route, Lease: lease, Capability: capability}, nil
}

func revokeMissingApplianceFrontends(ctx context.Context, tx *sql.Tx, installationID string, now int64) error {
	if _, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1,revision=revision+1,updated_at=? WHERE destination_installation_id=? AND revoked=0 AND NOT EXISTS(SELECT 1 FROM appliance_frontends f WHERE f.installation_id=service_imports.destination_installation_id AND f.name=service_imports.destination_frontend_name AND f.name='webhook' AND f.transport=2 AND f.exposure!=2)`, now, installationID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=revision+1,generation=generation+1,updated_at=? WHERE destination_installation_id=? AND state<>? AND import_id IN(SELECT import_id FROM service_imports WHERE revoked=1)`, servicev1.RouteState_ROUTE_STATE_REVOKED, now, installationID, servicev1.RouteState_ROUTE_STATE_REVOKED)
	return err
}
