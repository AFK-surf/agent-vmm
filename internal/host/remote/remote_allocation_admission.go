//go:build darwin || linux

package remote

import (
	"context"
	"database/sql"
	"errors"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) EnsureAllocation(ctx context.Context, request *connectorv1.EnsureAllocationRequest) (*connectorv1.AllocationResponse, error) {
	if err := server.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	ctx, release, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	environment, err := server.admitRemoteAllocation(ctx, request)
	if err != nil {
		return nil, err
	}
	unlock, err := server.workloads.LockEnvironment(ctx, environment.ID)
	if err != nil {
		return nil, opreceipt.CommittedFailure(err)
	}
	defer unlock()
	_, current, err := server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if err != nil {
		return nil, opreceipt.CommittedFailure(err)
	}
	if !sameAllocationRequest(current, request) {
		return nil, opreceipt.CommittedFailure(status.Error(codes.Aborted, "allocation changed before projection"))
	}
	if err := server.services.ReconcileEnvironment(ctx, current); err != nil {
		return nil, allocationProjectionError(err, server.Allocation(current))
	}
	current, err = server.store.GetEnvironment(ctx, current.ID)
	if err != nil {
		return nil, opreceipt.CommittedFailure(err)
	}
	return &connectorv1.AllocationResponse{Allocation: server.Allocation(current)}, nil
}

func (server *Owner) admitRemoteAllocation(ctx context.Context, request *connectorv1.EnsureAllocationRequest) (store.Environment, error) {
	server.remoteAdmissionMu.Lock()
	defer server.remoteAdmissionMu.Unlock()
	if err := server.requireRemoteAdmission(); err != nil {
		return store.Environment{}, err
	}
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err == nil && registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) && registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return store.Environment{}, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_DRAINING, "remote registration is draining", hostv1.WithResource("remote_registration", registration.ID))
	}
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return store.Environment{}, status.Error(codes.FailedPrecondition, "remote registration is not enabled")
	}
	if request.GetConnectionInstance() == "" || request.GetConnectionInstance() != registration.ConnectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return store.Environment{}, status.Error(codes.Aborted, "connection instance is not current")
	}
	resources := contractFromHost(request.GetLimits())
	if request.GetAllocationId() == "" || request.GetGeneration() == 0 || request.GetLimits() == nil || request.GetLimits().Pids == nil || request.GetLimits().DiskBytes == nil || resources.Validate() != nil {
		return store.Environment{}, hostv1.InvalidField("allocation_id/generation/limits", "complete allocation identity and PID and disk limits are required")
	}
	existing, existingErr := server.store.GetRemoteEnvironment(ctx, registration.ID, request.GetAllocationId())
	if existingErr != nil && !errors.Is(existingErr, sql.ErrNoRows) {
		return store.Environment{}, existingErr
	}
	if existingErr == nil && sameAllocationRequest(existing, request) {
		return existing, nil
	}
	if request.GetPolicyRevision() != registration.PoolPolicyRevision {
		return store.Environment{}, status.Error(codes.Aborted, "pool policy revision is stale")
	}
	if existingErr == nil {
		return store.Environment{}, hostv1.NewError(codes.AlreadyExists, hostv1.ErrorReason_ERROR_REASON_ALLOCATION_CONFLICT, "allocation exists with different limits", hostv1.WithResource("allocation", request.GetAllocationId()))
	}
	if err := admitAllocationV2(registration, resources, server.capacity, request.GetEgressMode()); err != nil {
		return store.Environment{}, err
	}
	environmentID, err := opreceipt.NewID()
	if err != nil {
		return store.Environment{}, err
	}
	namespaceID, err := opreceipt.NewID()
	if err != nil {
		return store.Environment{}, err
	}
	environmentFact := store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: request.GetDisplayLabel(),
		PIDs: uint32(resources.PIDMax), DiskBytes: resources.WritableQuotaBytes,
		EgressMode: int32(request.GetEgressMode()), OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: request.GetAllocationId(), AllocationGeneration: request.GetGeneration(), AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING),
	}
	environment, err := server.store.BeginWorkloadAdmission(ctx, environmentFact, server.capacity)
	if err != nil {
		if errors.Is(err, store.ErrResourceCapacityExceeded) {
			return store.Environment{}, capacityError("vm_capacity", registration.ID, err)
		}
		return store.Environment{}, err
	}
	return environment, nil
}

func (server *Owner) ReadAllocation(ctx context.Context, registrationID, instance, allocationID string) (store.RemoteRegistration, store.Environment, error) {
	server.remoteAdmissionMu.Lock()
	defer server.remoteAdmissionMu.Unlock()
	registration, err := server.requireCurrentRemoteConnectionLocked(ctx, registrationID, instance)
	if err != nil {
		return registration, store.Environment{}, err
	}
	environment, err := server.store.GetRemoteEnvironment(ctx, registrationID, allocationID)
	return registration, environment, err
}

func allocationProjectionError(err error, allocation *hostv1.RemoteAllocation) error {
	err = opreceipt.CommittedFailure(err)
	value, detailErr := status.Convert(err).WithDetails(allocation)
	if detailErr != nil {
		return detailErr
	}
	return value.Err()
}

func contractFromHost(value *hostv1.ResourceLimits) resourcev2.Contract {
	return resourcev2.Contract{PIDMax: uint64(value.GetPids()), WritableQuotaBytes: value.GetDiskBytes()}
}

func admitAllocationV2(registration store.RemoteRegistration, resources resourcev2.Contract, hostCapacity resourcev2.Contract, egress hostv1.EgressMode) error {
	if int32(egress) > registration.MaxEgressMode {
		return hostv1.NewError(codes.PermissionDenied, hostv1.ErrorReason_ERROR_REASON_POOL_EGRESS_LIMIT, "allocation egress exceeds pool policy", hostv1.WithResource("remote_registration", registration.ID))
	}
	perEnvironment := resourcev2.Contract{
		PIDMax:             uint64(registration.PerEnvPIDs),
		WritableQuotaBytes: registration.PerEnvDiskBytes,
	}
	if err := resources.Fits(perEnvironment); err != nil {
		return capacityError("pool_limit", registration.ID, err)
	}
	if err := resources.Fits(hostCapacity); err != nil {
		return capacityError("vm_capacity", registration.ID, err)
	}
	return nil
}

func capacityError(stage, registrationID string, err error) error {
	metadata := map[string]string{"stage": stage}
	if dimension := resourcev2.ExhaustedDimension(err); dimension != "" {
		metadata["resource_dimension"] = string(dimension)
	}
	return hostv1.NewError(codes.ResourceExhausted, hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED, err.Error(), hostv1.WithResource("remote_registration", registrationID), hostv1.WithMetadata(metadata))
}

func sameAllocationRequest(environment store.Environment, request *connectorv1.EnsureAllocationRequest) bool {
	return environment.AllocationGeneration == request.GetGeneration() && environment.Resources() == contractFromHost(request.GetLimits()) && environment.EgressMode == int32(request.GetEgressMode())
}

func (server *Owner) ReleaseAllocation(ctx context.Context, request *connectorv1.ReleaseAllocationRequest) (*connectorv1.AllocationResponse, error) {
	if request.GetAllocationId() == "" || request.GetExpectedPolicyRevision() == 0 || request.GetExpectedGeneration() == 0 {
		return nil, hostv1.InvalidField("allocation_id/expected_policy_revision/expected_generation", "complete allocation release fence is required")
	}
	ctx, release, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	_, environment, err := server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	if environment.AllocationGeneration != request.GetExpectedGeneration() {
		return nil, status.Error(codes.Aborted, "release generation is not current")
	}
	unlockEnvironment, lockErr := server.workloads.LockEnvironment(ctx, environment.ID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlockEnvironment()
	_, environment, err = server.ReadAllocation(ctx, request.GetRegistrationId(), request.GetConnectionInstance(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	if environment.AllocationGeneration != request.GetExpectedGeneration() {
		return nil, status.Error(codes.Aborted, "allocation generation changed")
	}
	if environment.PolicyRevision != request.GetExpectedPolicyRevision() {
		return nil, status.Error(codes.Aborted, "allocation policy revision is stale")
	}
	current, err := server.RetainEnvironmentLocked(ctx, environment, false)
	if err != nil {
		return nil, opreceipt.CommittedFailure(err)
	}

	server.wakeAllocationRetirement()
	return &connectorv1.AllocationResponse{Allocation: server.Allocation(current)}, nil
}
