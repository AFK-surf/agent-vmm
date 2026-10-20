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
	"google.golang.org/protobuf/proto"
)

func (store *Store) ListApplianceServiceImports(ctx context.Context, installationID string) ([]*servicev1.ServiceImport, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT import_id,COALESCE(source_environment_id,''),source_container_id,source_container_instance_id,destination_device_id,destination_export_id,virtual_service_name,destination_port,revision,revoked,COALESCE(source_installation_id,''),COALESCE(destination_installation_id,''),destination_frontend_name,source_container_generation_id FROM service_imports WHERE source_installation_id=? OR destination_installation_id=? ORDER BY import_id`, installationID, installationID)
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

func (store *Store) ServiceRouteForImport(ctx context.Context, importID string) (ServiceRouteRecord, error) {
	var routeID, leaseID string
	var generation uint64
	err := store.db.QueryRowContext(ctx, `SELECT r.route_id,l.lease_id,r.generation FROM access_routes r JOIN route_leases l ON l.route_id=r.route_id WHERE r.import_id=?`, importID).Scan(&routeID, &leaseID, &generation)
	if err != nil {
		return ServiceRouteRecord{}, err
	}
	return store.GetServiceRoute(ctx, routeID, leaseID, generation)
}

func (store *Store) RevokeApplianceServiceImport(ctx context.Context, request *appliancev1.RevokeApplianceServiceImportRequest) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner string
	var revoked bool
	var priorID sql.NullString
	var priorHash []byte
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(source_installation_id,destination_installation_id),revoked,revoke_request_id,revoke_request_sha256 FROM service_imports WHERE import_id=?`, request.GetImportId()).Scan(&owner, &revoked, &priorID, &priorHash); err != nil {
		return err
	}
	if owner != request.GetInstallationId() {
		return sql.ErrNoRows
	}
	body, _ := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	hash := sha256.Sum256(body)
	if priorID.Valid && priorID.String == request.GetRequestId() {
		if !bytes.Equal(priorHash, hash[:]) {
			return ErrTrustRevisionConflict
		}
		return nil
	}
	if revoked {
		return ErrTrustRevisionConflict
	}
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE service_imports SET revoked=1,revision=revision+1,updated_at=?,revoke_request_id=?,revoke_request_sha256=? WHERE import_id=? AND (source_installation_id=? OR destination_installation_id=?) AND revoked=0 AND revision=? AND EXISTS(SELECT 1 FROM appliance_installations WHERE id=? AND revision=?)`, now, request.GetRequestId(), hash[:], request.GetImportId(), owner, owner, request.GetExpectedRevision(), owner, request.GetExpectedInstallationRevision())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=revision+1,generation=generation+1,updated_at=? WHERE import_id=?`, servicev1.RouteState_ROUTE_STATE_REVOKED, now, request.GetImportId()); err != nil {
		return err
	}
	return tx.Commit()
}

// Withdrawal revokes current routes and preserves approved import intents.
func (store *Store) WithdrawApplianceServiceImports(ctx context.Context, installationID string) ([]string, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT import_id FROM service_imports WHERE source_installation_id=? OR destination_installation_id=? ORDER BY import_id`, installationID, installationID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()

	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=revision+1,generation=generation+1,updated_at=? WHERE (source_installation_id=? OR destination_installation_id=?) AND state<>?`, servicev1.RouteState_ROUTE_STATE_REVOKED, now, installationID, installationID, servicev1.RouteState_ROUTE_STATE_REVOKED); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (store *Store) RenewApplianceServiceRoute(ctx context.Context, installation ApplianceInstallation, grantRevision uint64, expectedRouteRevision uint64, route *servicev1.AccessRoute, lease *servicev1.RouteLease) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(lease.GetCapability())
	if err != nil {
		return err
	}
	now := time.Now().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=?,generation=?,expires_at=?,source_container_instance_id=?,source_boot_id=?,policy_revision=?,capability=?,updated_at=? WHERE route_id=? AND revision=? AND source_installation_id=? AND EXISTS(
 SELECT 1 FROM service_imports i JOIN service_exports e ON e.export_id=i.destination_export_id JOIN appliance_installations a ON a.id=i.source_installation_id JOIN appliance_grants g ON g.installation_id=a.id
 WHERE i.import_id=access_routes.import_id AND i.revoked=0 AND e.revoked=0 AND a.revision=? AND a.desired_enabled=1 AND a.state=3 AND a.current_boot_id=? AND a.current_container_instance=? AND g.revision=?)`, servicev1.RouteState_ROUTE_STATE_ACTIVE, route.GetRevision(), route.GetGeneration(), route.GetExpiresAt().AsTime().UnixNano(), installation.CurrentContainerInstance, installation.CurrentBootID, grantRevision, payload, now, route.GetRouteId(), expectedRouteRevision, installation.ID, installation.Revision, installation.CurrentBootID, installation.CurrentContainerInstance, grantRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.Join(ErrTrustRevisionConflict, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE route_leases SET route_revision=?,generation=?,expires_at=?,capability=? WHERE route_id=?`, route.GetRevision(), route.GetGeneration(), route.GetExpiresAt().AsTime().UnixNano(), payload, route.GetRouteId()); err != nil {
		return err
	}
	return tx.Commit()
}

// Replay returns the original approval receipt without changing its current projection.
func (store *Store) ReplayApplianceServiceImport(ctx context.Context, request *appliancev1.CreateApplianceServiceImportRequest) (*servicev1.ServiceImport, error) {
	value, err := store.serviceImportByRequest(ctx, request.GetRequestId())
	if err != nil {
		return nil, err
	}
	if value.GetSourceInstallationId() != request.GetInstallationId() {
		return nil, ErrTrustRevisionConflict
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
	if !bytes.Equal(stored, hash[:]) {
		return nil, ErrTrustRevisionConflict
	}
	return value, nil
}
