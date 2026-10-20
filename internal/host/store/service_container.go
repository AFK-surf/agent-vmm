package store

import (
	"context"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
)

func (store *Store) ProjectServiceContainer(ctx context.Context, environment, container, generation, instance string) ([]string, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT i.import_id FROM service_imports i LEFT JOIN service_exports e ON e.export_id=i.destination_export_id WHERE i.revoked=0 AND ((i.source_environment_id=? AND i.source_container_id=? AND (i.source_container_generation_id<>? OR i.source_container_instance_id<>? OR EXISTS(SELECT 1 FROM access_routes r WHERE r.import_id=i.import_id AND r.state=1))) OR (e.environment_id=? AND e.container_id=? AND e.revoked=0 AND (e.container_generation_id<>? OR e.container_instance_id<>? OR EXISTS(SELECT 1 FROM access_routes r WHERE r.import_id=i.import_id AND r.state=1))))`, environment, container, generation, instance, environment, container, generation, instance)
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
	now := time.Now().UnixNano()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=revision+1,generation=generation+CASE WHEN state<>1 AND destination_installation_id!='' THEN 1 ELSE 0 END,updated_at=? WHERE import_id=? AND state NOT IN (?,?)`, servicev1.RouteState_ROUTE_STATE_PREPARING, now, id, servicev1.RouteState_ROUTE_STATE_REVOKED, servicev1.RouteState_ROUTE_STATE_EXPIRED); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_exports SET container_generation_id=?,container_instance_id=?,updated_at=? WHERE environment_id=? AND container_id=? AND revoked=0`, generation, instance, now, environment, container); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_imports SET source_container_generation_id=?,source_container_instance_id=?,updated_at=? WHERE source_environment_id=? AND source_container_id=? AND revoked=0`, generation, instance, now, environment, container); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET source_container_instance_id=(SELECT source_container_instance_id FROM service_imports WHERE import_id=access_routes.import_id) WHERE import_id IN(SELECT import_id FROM service_imports WHERE source_environment_id=? AND source_container_id=? AND revoked=0)`, environment, container); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=?,generation=generation+CASE WHEN destination_installation_id='' THEN 1 ELSE 0 END WHERE import_id IN(SELECT import_id FROM service_imports WHERE revoked=1) AND state=?`, servicev1.RouteState_ROUTE_STATE_REVOKED, servicev1.RouteState_ROUTE_STATE_PREPARING); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (store *Store) SuspendServiceImportRoute(ctx context.Context, id string) error {
	_, err := store.db.ExecContext(ctx, `UPDATE access_routes SET state=?,revision=revision+1,generation=generation+1,updated_at=? WHERE import_id=? AND state=?`, servicev1.RouteState_ROUTE_STATE_PREPARING, time.Now().UnixNano(), id, servicev1.RouteState_ROUTE_STATE_ACTIVE)
	return err
}

func (store *Store) ActivateServiceProjection(ctx context.Context, id string, generation uint64) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE access_routes SET state=? WHERE import_id=? AND generation=? AND state=? AND EXISTS(SELECT 1 FROM service_imports i LEFT JOIN service_exports e ON e.export_id=i.destination_export_id WHERE i.import_id=access_routes.import_id AND i.revoked=0 AND i.source_container_instance_id!='' AND (e.export_id IS NULL OR (e.revoked=0 AND e.container_instance_id!='')))`, servicev1.RouteState_ROUTE_STATE_ACTIVE, id, generation, servicev1.RouteState_ROUTE_STATE_PREPARING); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE route_leases SET route_revision=(SELECT revision FROM access_routes WHERE route_id=route_leases.route_id),generation=? WHERE route_id IN(SELECT route_id FROM access_routes WHERE import_id=? AND generation=? AND state=?)`, generation, id, generation, servicev1.RouteState_ROUTE_STATE_ACTIVE); err != nil {
		return err
	}

	return tx.Commit()
}

func (store *Store) ListApprovedServiceExports(ctx context.Context, registration string) ([]*servicev1.ServiceExport, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT DISTINCT e.export_id,e.environment_id,e.container_id,e.container_instance_id,e.port,e.protocol,e.revision,e.revoked,e.created_at,e.container_generation_id FROM remote_registrations r JOIN appliance_installations a ON a.id=r.installation_id JOIN service_imports i ON i.source_installation_id=a.id JOIN service_exports e ON e.export_id=i.destination_export_id WHERE r.id=? AND r.owner_kind=? AND a.desired_enabled=1 AND i.revoked=0 AND e.revoked=0 ORDER BY e.export_id`, registration, RemoteRegistrationPrivate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var exports []*servicev1.ServiceExport
	for rows.Next() {
		value, err := scanServiceExport(rows)
		if err != nil {
			return nil, err
		}
		exports = append(exports, value)
	}
	return exports, rows.Err()
}

func (store *Store) ServiceExportApproved(ctx context.Context, registration, exported string) (bool, error) {
	var approved bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM remote_registrations r JOIN appliance_installations a ON a.id=r.installation_id JOIN service_imports i ON i.source_installation_id=a.id JOIN service_exports e ON e.export_id=i.destination_export_id WHERE r.id=? AND r.owner_kind=? AND a.desired_enabled=1 AND i.revoked=0 AND e.revoked=0 AND e.export_id=?)`, registration, RemoteRegistrationPrivate, exported).Scan(&approved)
	return approved, err
}

func (store *Store) RenewLocalServiceProjectionLease(ctx context.Context, id string, capability *trustv1.RouteCapability) error {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(capability)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE access_routes SET expires_at=?,capability=? WHERE import_id=? AND state IN(1,2) AND EXISTS(SELECT 1 FROM service_imports i JOIN service_exports e ON e.export_id=i.destination_export_id WHERE i.import_id=access_routes.import_id AND i.revoked=0 AND e.revoked=0)`, capability.GetExpiresAt().AsTime().UnixNano(), payload, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE route_leases SET expires_at=?,capability=? WHERE route_id IN(SELECT route_id FROM access_routes WHERE import_id=? AND state IN(1,2))`, capability.GetExpiresAt().AsTime().UnixNano(), payload, id); err != nil {
		return err
	}
	return tx.Commit()
}
