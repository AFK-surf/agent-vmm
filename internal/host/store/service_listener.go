package store

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ServiceListenerRecord struct {
	Listener   *hostv1.ServiceListener
	Capability *trustv1.RouteCapability
	Endpoint   *trustv1.EndpointObservation
}

func (store *Store) CreateServiceListener(ctx context.Context, requestID string, listener *hostv1.ServiceListener, capability *trustv1.RouteCapability, endpoint *trustv1.EndpointObservation) (*hostv1.ServiceListener, error) {
	capabilityPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(capability)
	if err != nil {
		return nil, err
	}
	endpointPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(endpoint)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()
	_, err = store.db.ExecContext(ctx, `INSERT INTO service_listeners(listener_id, request_id, mesh_id,
		destination_device_id, destination_export_id, exposure, listen_address, route_id, capability_id,
		capability, endpoint, expires_at, revision, revoked, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?)`, listener.GetId(), requestID,
		listener.GetMeshId(), listener.GetDestinationDeviceId(), listener.GetDestinationExportId(), listener.GetExposure(),
		listener.GetListenAddress(), listener.GetRouteId(), listener.GetCapabilityId(), capabilityPayload, endpointPayload,
		listener.GetExpiresAt().AsTime().UnixNano(), now, now)
	if err != nil {
		existing, getErr := store.GetServiceListenerByRequest(ctx, requestID)
		if getErr == nil {
			return existing.Listener, nil
		}
		return nil, err
	}
	result := proto.Clone(listener).(*hostv1.ServiceListener)
	result.Revision = 1
	return result, nil
}

func (store *Store) GetServiceListenerByRequest(ctx context.Context, requestID string) (ServiceListenerRecord, error) {
	return store.scanServiceListener(store.db.QueryRowContext(ctx, `SELECT listener_id, mesh_id, destination_device_id,
		destination_export_id, exposure, listen_address, route_id, capability_id, capability, endpoint, expires_at, revision, revoked
		FROM service_listeners WHERE request_id=?`, requestID))
}

func (store *Store) GetServiceListener(ctx context.Context, listenerID string) (ServiceListenerRecord, error) {
	return store.scanServiceListener(store.db.QueryRowContext(ctx, `SELECT listener_id, mesh_id, destination_device_id,
		destination_export_id, exposure, listen_address, route_id, capability_id, capability, endpoint, expires_at, revision, revoked
		FROM service_listeners WHERE listener_id=?`, listenerID))
}

func (store *Store) ListServiceListeners(ctx context.Context) ([]*hostv1.ServiceListener, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT listener_id, mesh_id, destination_device_id, destination_export_id,
		exposure, listen_address, route_id, capability_id, expires_at, revision, revoked
		FROM service_listeners ORDER BY created_at DESC, listener_id LIMIT 64`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*hostv1.ServiceListener
	for rows.Next() {
		value := new(hostv1.ServiceListener)
		var expiresAt int64
		if err := rows.Scan(&value.Id, &value.MeshId, &value.DestinationDeviceId, &value.DestinationExportId,
			&value.Exposure, &value.ListenAddress, &value.RouteId, &value.CapabilityId, &expiresAt, &value.Revision, &value.Revoked); err != nil {
			return nil, err
		}
		value.ExpiresAt = timestamppb.New(time.Unix(0, expiresAt))
		result = append(result, value)
	}
	return result, rows.Err()
}

func (store *Store) RevokeServiceListener(ctx context.Context, listenerID string, expectedRevision uint64) error {
	result, err := store.db.ExecContext(ctx, `UPDATE service_listeners SET revoked=1, revision=revision+1, updated_at=?
		WHERE listener_id=? AND revision=? AND revoked=0`, time.Now().UnixNano(), listenerID, expectedRevision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrTrustRevisionConflict
	}
	return nil
}

func (store *Store) scanServiceListener(row rowScanner) (ServiceListenerRecord, error) {
	value := new(hostv1.ServiceListener)
	var capabilityPayload, endpointPayload []byte
	var expiresAt int64
	if err := row.Scan(&value.Id, &value.MeshId, &value.DestinationDeviceId, &value.DestinationExportId,
		&value.Exposure, &value.ListenAddress, &value.RouteId, &value.CapabilityId, &capabilityPayload, &endpointPayload,
		&expiresAt, &value.Revision, &value.Revoked); err != nil {
		return ServiceListenerRecord{}, err
	}
	value.ExpiresAt = timestamppb.New(time.Unix(0, expiresAt))
	record := ServiceListenerRecord{Listener: value, Capability: new(trustv1.RouteCapability), Endpoint: new(trustv1.EndpointObservation)}
	if err := proto.Unmarshal(capabilityPayload, record.Capability); err != nil {
		return ServiceListenerRecord{}, err
	}
	if err := proto.Unmarshal(endpointPayload, record.Endpoint); err != nil {
		return ServiceListenerRecord{}, err
	}
	return record, nil
}
