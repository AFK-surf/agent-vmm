package store

import (
	"context"
	"errors"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"google.golang.org/protobuf/proto"
)

func (store *Store) GetPersonalRouteIssuance(ctx context.Context, request *servicev1.RequestPersonalRouteRequest) (*servicev1.RequestPersonalRouteResponse, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT response FROM personal_route_issuances WHERE request_id=? AND mesh_id=? AND source_device_id=? AND destination_export_id=? AND expires_at>?`,
		request.GetRequestId(), request.GetMeshId(), request.GetSourceDeviceId(), request.GetDestinationExportId(), time.Now().UnixNano()).Scan(&payload); err != nil {
		return nil, err
	}
	response := new(servicev1.RequestPersonalRouteResponse)
	if err := proto.Unmarshal(payload, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (store *Store) PutPersonalRouteIssuance(ctx context.Context, request *servicev1.RequestPersonalRouteRequest, response *servicev1.RequestPersonalRouteResponse) error {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, `INSERT INTO personal_route_issuances(request_id, mesh_id, source_device_id,
		destination_export_id, expires_at, response, created_at) VALUES(?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_id) DO NOTHING`,
		request.GetRequestId(), request.GetMeshId(), request.GetSourceDeviceId(), request.GetDestinationExportId(),
		request.GetExpiresAt().AsTime().UnixNano(), payload, time.Now().UnixNano())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		existing, getErr := store.GetPersonalRouteIssuance(ctx, request)
		if getErr != nil {
			return errors.Join(ErrTrustRevisionConflict, getErr)
		}
		if !proto.Equal(existing, response) {
			return ErrTrustRevisionConflict
		}
	}
	return nil
}
