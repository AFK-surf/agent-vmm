package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"time"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
)

const maxRegistryOperationBatch = 32

const personalMeshSyncInterval = 30 * time.Second

// SyncPersonalMeshRegistry advances the durable host-owned operation queue.
// Retrying the same device-signed operation ID after an ambiguous transport
// result is intentional: registry mutations are CAS/idempotency operations,
// not live runtime side effects. The cursor advances only after the host
// accepts the signed freshness receipt and resulting snapshot.
//
// The queue/retry/CAS/remove-wins transitions are modeled by
// the PersonalMeshTrust model.
func SyncPersonalMeshRegistry(ctx context.Context, local connectorv1.TrustServiceClient, registry trustv1.PersonalMeshRegistryServiceClient, afterSequence uint64) (uint64, error) {
	if local == nil || registry == nil {
		return afterSequence, errors.New("local trust and registry clients are required")
	}
	pending, err := local.PullRegistryOperations(ctx, &trustv1.PullRegistryOperationsRequest{AfterSequence: afterSequence, Limit: maxRegistryOperationBatch})
	if err != nil {
		return afterSequence, fmt.Errorf("pull host registry operations: %w", err)
	}
	cursor := afterSequence
	for _, item := range pending.GetOperations() {
		if item.GetSequence() <= cursor || item.GetOperation() == nil {
			return cursor, errors.New("host registry operation sequence is invalid")
		}
		operation := item.GetOperation()
		var committed *trustv1.MeshSnapshot
		switch operation.GetKind() {
		case trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_GENESIS:
			if item.GetMeshDescriptor() == nil {
				return cursor, errors.New("genesis operation is missing mesh descriptor")
			}
			response, commitErr := registry.CreateMesh(ctx, &trustv1.CreateMeshRequest{MeshDescriptor: item.GetMeshDescriptor(), Genesis: operation})
			committed, err = response.GetSnapshot(), commitErr
		case trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_JOIN:
			response, commitErr := registry.CommitJoin(ctx, &trustv1.CommitJoinRequest{Operation: operation})
			committed, err = response.GetSnapshot(), commitErr
		case trustv1.RegistryOperationKind_REGISTRY_OPERATION_KIND_REVOKE:
			response, commitErr := registry.CommitRevoke(ctx, &trustv1.CommitRevokeRequest{Operation: operation})
			committed, err = response.GetSnapshot(), commitErr
		default:
			return cursor, errors.New("unsupported registry operation kind")
		}
		if err != nil {
			return cursor, fmt.Errorf("commit registry operation %s: %w", operation.GetOperationId(), err)
		}
		if committed == nil {
			return cursor, errors.New("registry returned no committed snapshot")
		}
		if _, err := local.CommitRegistryOperation(ctx, &trustv1.CommitRegistryOperationRequest{
			OperationId: operation.GetOperationId(), Snapshot: committed,
		}); err != nil {
			return cursor, fmt.Errorf("commit registry receipt to host: %w", err)
		}
		cursor = item.GetSequence()
	}
	return cursor, nil
}

func syncPersonalMeshesOnce(ctx context.Context, config Config, observations *endpointObservationState, registry trustv1.PersonalMeshRegistryServiceClient, published map[string]string) error {
	local, requestContext, closeClient, err := trustClientWithTimeout(ctx, config.trustSocket(), 20*time.Second)
	if err != nil {
		return err
	}
	defer closeClient()
	return syncPersonalMeshesWithClients(requestContext, observations, local, registry, published)
}

func syncPersonalMeshesWithClients(ctx context.Context, observations *endpointObservationState, local connectorv1.TrustServiceClient, registry trustv1.PersonalMeshRegistryServiceClient, published map[string]string) error {
	if _, err := SyncPersonalMeshRegistry(ctx, local, registry, 0); err != nil {
		return err
	}
	listed, err := local.ListPersonalMeshes(ctx, &trustv1.ListPersonalMeshesRequest{})
	if err != nil {
		return err
	}
	localObservation := observations.Load()
	localDeviceID := ""
	observationVersion := ""
	if localObservation != nil {
		localDeviceID = localObservation.GetDeviceId()
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(localObservation)
		if err != nil {
			return fmt.Errorf("encode local endpoint observation: %w", err)
		}
		observationVersion = string(encoded)
	}
	for _, mesh := range listed.GetMeshes() {
		descriptor, snapshot := mesh.GetMeshDescriptor(), mesh.GetSnapshot()
		if descriptor == nil || snapshot == nil || descriptor.GetMeshId() == "" || snapshot.GetRevision() == 0 {
			return errors.New("host returned an incomplete personal mesh")
		}
		if localObservation != nil && published[descriptor.GetMeshId()] != observationVersion {
			if _, err := registry.PublishEndpoint(ctx, &trustv1.PublishEndpointRequest{
				MeshId: descriptor.GetMeshId(), ExpectedRevision: snapshot.GetRevision(), Observation: localObservation,
			}); err != nil {
				return fmt.Errorf("publish mesh endpoint: %w", err)
			}
			published[descriptor.GetMeshId()] = observationVersion
		}
		current := snapshot
		response, snapshotErr := registry.GetSnapshot(ctx, &trustv1.GetSnapshotRequest{
			MeshId: descriptor.GetMeshId(), MinimumRevision: snapshot.GetRevision(),
		})
		if snapshotErr != nil {
			return snapshotErr
		}
		remoteSnapshot := response.GetSnapshot()
		if remoteSnapshot.GetRevision() > snapshot.GetRevision() {
			if _, err := local.ApplyMeshSnapshot(ctx, &trustv1.ApplyMeshSnapshotRequest{
				Snapshot: remoteSnapshot, ExpectedRevision: snapshot.GetRevision(),
			}); err != nil {
				return err
			}
			current = remoteSnapshot
		} else if remoteSnapshot.GetRevision() != snapshot.GetRevision() {
			return errors.New("registry returned a stale personal mesh snapshot")
		}
		endpoints, err := registry.ListEndpoints(ctx, &trustv1.ListEndpointsRequest{
			MeshId: descriptor.GetMeshId(), ExpectedRevision: current.GetRevision(),
		})
		if err != nil || endpoints.GetMeshRevision() != current.GetRevision() || len(endpoints.GetObservations()) > 32 {
			return fmt.Errorf("list mesh endpoints: invalid bounded response: %w", err)
		}
		for _, observation := range endpoints.GetObservations() {
			if observation.GetDeviceId() == localDeviceID {
				continue
			}
			existing, getErr := local.GetEndpointObservation(ctx, &trustv1.GetEndpointObservationRequest{DeviceId: observation.GetDeviceId()})
			if getErr == nil && proto.Equal(existing, observation) {
				continue
			}
			if _, err := local.ApplyMeshEndpointObservation(ctx, &trustv1.ApplyMeshEndpointObservationRequest{
				MeshId: descriptor.GetMeshId(), Observation: observation,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func personalMeshSyncLoop(ctx context.Context, config Config, observations *endpointObservationState, registry trustv1.PersonalMeshRegistryServiceClient) {
	published := make(map[string]string)
	for ctx.Err() == nil {
		iterationContext, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := syncPersonalMeshesOnce(iterationContext, config, observations, registry, published)
		cancel()
		if err != nil && config.Logger != nil {
			config.Logger.Warn("personal mesh registry sync failed", "error", err)
		}
		timer := time.NewTimer(personalMeshSyncInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
