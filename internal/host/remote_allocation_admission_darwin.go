//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"sort"
	"time"
)

func (server *remoteComputeServer) EnsureAllocation(ctx context.Context, request *hostv1.EnsureAllocationRequest) (response *hostv1.AllocationResponse, resultErr error) {
	stage := "authorize"
	defer func() {
		if resultErr != nil {
			log.Printf("allocation ensure failed: registration=%s allocation=%s generation=%d stage=%s error=%v details=%v", request.GetRegistrationId(), request.GetAllocationId(), request.GetGeneration(), stage, resultErr, status.Convert(resultErr).Details())
		}
	}()
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	stage = "inventory_admission"
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	stage = "remote_admission"
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err == nil && registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) && registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		return nil, resourceError(codes.Unavailable, "REGISTRATION_DRAINING", "remote_registration", registration.ID, "remote registration is draining")
	}
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_NOT_ENABLED", "remote_registration", "active", "remote registration is not enabled")
	}
	if request.GetConnectionInstance() == "" || request.GetConnectionInstance() != registration.ConnectionInstance || registration.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil, resourceError(codes.Aborted, "STALE_CONNECTION", "remote_connection", request.GetConnectionInstance(), "connection instance is not current")
	}
	resources := contractFromHost(request.GetResourcesV2())
	if request.GetAllocationId() == "" || request.GetGeneration() == 0 || request.GetResourcesV2() == nil || resources.Validate(server.controller.remoteCapacityV2.CPUMaxMillis) != nil {
		return nil, invalidField("allocation_id/generation/resources_v2", "complete resource-v2 allocation identity and resources are required")
	}
	existing, existingErr := server.controller.store.GetRemoteEnvironment(ctx, registration.ID, request.GetAllocationId())
	if existingErr != nil && !errors.Is(existingErr, sql.ErrNoRows) {
		return nil, existingErr
	}
	if existingErr == nil && sameAllocationRequest(existing, request) {
		unlockEnvironment := server.controller.lockEnvironment(existing.ID)
		defer unlockEnvironment()
		if err := server.controller.reconcileEnvironment(ctx, existing); err != nil {
			log.Printf("allocation projection failed: allocation=%s environment=%s namespace=%s error=%v details=%v", request.GetAllocationId(), existing.ID, existing.NamespaceID, err, status.Convert(err).Details())
			return nil, allocationProjectionError(err, "Guest namespace projection is incomplete", server.controller.remoteAllocation(existing))
		}
		existing, err = server.controller.store.GetEnvironment(ctx, existing.ID)
		if err != nil {
			return nil, err
		}
		return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(existing)}, nil
	}
	if request.GetPolicyRevision() != registration.PoolPolicyRevision {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "remote_registration", registration.ID, "pool policy revision is stale")
	}
	if existingErr == nil {
		return nil, resourceError(codes.AlreadyExists, "ALLOCATION_CONFLICT", "allocation", request.GetAllocationId(), "allocation exists with different limits")
	}
	if err := admitAllocationV2(registration, resources, server.controller.remoteCapacityV2, request.GetEgressMode()); err != nil {
		return nil, err
	}
	environmentID, err := randomID()
	if err != nil {
		return nil, err
	}
	namespaceID, err := randomID()
	if err != nil {
		return nil, err
	}
	environmentFact := store.Environment{
		ID: environmentID, NamespaceID: namespaceID, DisplayName: request.GetDisplayLabel(),
		CPUMillis: uint32(resources.CPUMaxMillis), MemoryBytes: resources.MemoryMaxBytes, PIDs: uint32(resources.PIDMax), DiskBytes: resources.WritableQuotaBytes,
		EgressMode: int32(request.GetEgressMode()), OwnerKind: store.OwnerRemoteRegistration, RegistrationID: registration.ID,
		AllocationID: request.GetAllocationId(), AllocationGeneration: request.GetGeneration(), AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_DRAINING),
	}
	stage = "persist_admission"
	environment, err := server.controller.store.BeginWorkloadAdmission(ctx, environmentFact, server.controller.remoteCapacityV2)
	if err != nil {
		if errors.Is(err, store.ErrResourceCapacityExceeded) {
			return nil, capacityError("RESOURCE_LEDGER_EXHAUSTED", registration.ID, err)
		}
		return nil, err
	}
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	stage = "project_namespace_and_policy"
	if err := server.controller.reconcileEnvironment(ctx, environment); err != nil {
		log.Printf("allocation projection failed: allocation=%s environment=%s namespace=%s error=%v details=%v", request.GetAllocationId(), environment.ID, environment.NamespaceID, err, status.Convert(err).Details())
		return nil, allocationProjectionError(err, "allocation was saved but guest projection failed", server.controller.remoteAllocation(environment))
	}
	return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(environment)}, nil
}

func allocationProjectionError(err error, message string, allocation *hostv1.RemoteAllocation) error {
	if status.Code(err) != codes.ResourceExhausted {
		err = retryable("ALLOCATION_PROJECTION_FAILED", message, time.Second)
	}
	value, detailErr := status.Convert(err).WithDetails(allocation)
	if detailErr != nil {
		return detailErr
	}
	return value.Err()
}

func contractFromHost(value *hostv1.WorkloadResourcesV2) resourcev2.Contract {
	return resourcev2.Contract{CPUMaxMillis: uint64(value.GetCpuMaxMillis()),
		MemoryMaxBytes: value.GetMemoryMaxBytes(), PIDMax: uint64(value.GetPidMax()), WritableQuotaBytes: value.GetWritableQuotaBytes()}
}

func admitAllocationV2(registration store.RemoteRegistration, resources resourcev2.Contract, hostCapacity resourcev2.Contract, egress hostv1.EgressMode) error {
	if int32(egress) > registration.MaxEgressMode {
		return resourceError(codes.PermissionDenied, "POOL_EGRESS_LIMIT", "remote_registration", registration.ID, "allocation egress exceeds pool policy")
	}
	perEnvironment := resourcev2.Contract{
		PIDMax:             uint64(registration.PerEnvPIDs),
		WritableQuotaBytes: registration.PerEnvDiskBytes,
	}
	if err := resources.Fits(perEnvironment); err != nil {
		return capacityError("POOL_PER_ENVIRONMENT_LIMIT", registration.ID, err)
	}
	if err := resources.Fits(hostCapacity); err != nil {
		return capacityError("POOL_PER_ENVIRONMENT_LIMIT", registration.ID, err)
	}
	return nil
}

func capacityError(reason, registrationID string, err error) error {
	metadata := map[string]string{}
	if dimension := resourcev2.ExhaustedDimension(err); dimension != "" {
		metadata["resource_dimension"] = string(dimension)
	}
	return resourceErrorWithMetadata(codes.ResourceExhausted, reason, "remote_registration", registrationID, err.Error(), metadata)
}

func sameAllocationRequest(environment store.Environment, request *hostv1.EnsureAllocationRequest) bool {
	return environment.AllocationGeneration == request.GetGeneration() && environment.Resources() == contractFromHost(request.GetResourcesV2()) && environment.EgressMode == int32(request.GetEgressMode())
}

func (server *remoteComputeServer) ReleaseAllocation(ctx context.Context, request *hostv1.ReleaseAllocationRequest) (*hostv1.AllocationResponse, error) {
	if request.GetAllocationId() == "" || request.GetExpectedPolicyRevision() == 0 || request.GetExpectedGeneration() == 0 {
		return nil, invalidField("allocation_id/expected_policy_revision/expected_generation", "complete allocation release fence is required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	environment, err := server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.AllocationGeneration != request.GetExpectedGeneration() {
		return nil, resourceError(codes.Aborted, "STALE_GENERATION", "allocation", request.GetAllocationId(), "release generation is not current")
	}
	unlockEnvironment := server.controller.lockEnvironment(environment.ID)
	defer unlockEnvironment()
	environment, err = server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.PolicyRevision != request.GetExpectedPolicyRevision() {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "environment", environment.ID, "allocation policy revision is stale")
	}
	current, err := server.controller.retainRemoteEnvironmentLocked(ctx, environment, false)
	if err != nil {
		return nil, err
	}
	if environment.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		current, err = server.controller.store.RequeueAllocationRetirement(ctx, environment.ID, current.PolicyRevision)
		if err != nil {
			return nil, err
		}
	}
	server.controller.wakeAllocationRetirement()
	return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(current)}, nil
}

func (server *remoteComputeServer) PlanCleanupAllocation(ctx context.Context, request *hostv1.PlanCleanupAllocationRequest) (*hostv1.AllocationCleanupPlanResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancel()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseAdmission()
	if err := validateCleanupKind(request.GetKind()); err != nil {
		return nil, err
	}
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	if request.GetAllocationId() == "" {
		return nil, invalidField("allocation_id", "allocation identity is required")
	}
	environment, err := server.controller.store.GetRemoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) && request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return &hostv1.AllocationCleanupPlanResponse{Plan: &hostv1.AllocationCleanupPlan{AllocationId: request.GetAllocationId(), Kind: request.GetKind()}}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, resourceError(codes.NotFound, "ALLOCATION_NOT_FOUND", "allocation", request.GetAllocationId(), "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	unlockEnvironment, err := server.controller.lockEnvironmentContext(ctx, environment.ID)
	if err != nil {
		return nil, err
	}
	defer unlockEnvironment()
	environment, err = server.remoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanupWithRemoteOwner(ctx, environment.ID, request.GetKind(), true)
	if err != nil {
		return nil, err
	}
	return &hostv1.AllocationCleanupPlanResponse{Plan: allocationCleanupPlan(request.GetAllocationId(), environment.AllocationGeneration, plan)}, nil
}

func allocationCleanupPlan(allocationID string, allocationGeneration uint64, plan *hostv1.CleanupPlan) *hostv1.AllocationCleanupPlan {
	result := &hostv1.AllocationCleanupPlan{AllocationId: allocationID, AllocationGeneration: allocationGeneration, Revision: plan.GetRevision(), Kind: plan.GetKind()}
	for _, impact := range plan.GetImpacts() {
		copy := &hostv1.CleanupImpact{Effect: impact.GetEffect(), Irreversible: impact.GetIrreversible(), Blocker: impact.GetBlocker()}
		if resource := impact.GetResource(); resource != nil {
			copy.Resource = &hostv1.ResourceReference{Kind: resource.GetKind(), ResourceId: resource.GetResourceId()}
			if resource.GetKind() == "volume" {
				result.VolumeIds = append(result.VolumeIds, resource.GetResourceId())
			}
		}
		result.Impacts = append(result.Impacts, copy)
	}
	sort.Strings(result.VolumeIds)
	return result
}

func (server *remoteComputeServer) ExecuteCleanupAllocation(ctx context.Context, request *hostv1.ExecuteCleanupAllocationRequest) (*hostv1.AllocationResponse, error) {
	ctx, cancelAdmission := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancelAdmission()
	releaseInventory, err := server.controller.lockInventoryTransition(ctx)
	if err != nil {
		return nil, cleanupAdmissionError(err, request.GetAllocationId())
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, cleanupAdmissionError(err, request.GetAllocationId())
	}
	defer releaseAdmission()
	if err := validateCleanupKind(request.GetKind()); err != nil {
		return nil, err
	}
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	if request.GetAllocationId() == "" {
		return nil, invalidField("allocation_id", "allocation identity is required")
	}
	environment, err := server.controller.store.GetRemoteEnvironment(ctx, request.GetRegistrationId(), request.GetAllocationId())
	if errors.Is(err, sql.ErrNoRows) && request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && request.GetPlanRevision() == 0 && len(request.GetConfirmedVolumeIds()) == 0 {
		return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, resourceError(codes.NotFound, "ALLOCATION_NOT_FOUND", "allocation", request.GetAllocationId(), "allocation does not exist")
	}
	if err != nil {
		return nil, err
	}
	if request.GetPlanRevision() == 0 {
		return nil, resourceError(codes.Aborted, "PLAN_STALE", "allocation", request.GetAllocationId(), "allocation was created after the absence plan")
	}
	confirmed := append([]string(nil), request.GetConfirmedVolumeIds()...)
	sort.Strings(confirmed)
	requestID, err := randomID()
	if err != nil {
		return nil, err
	}
	requested := &hostv1.CleanupPlan{
		EnvironmentId: environment.ID,
		Kind:          request.GetKind(),
		Revision:      request.GetPlanRevision(),
	}
	operation, err := (&operatorServer{controller: server.controller}).executeCleanupAdmissionLockedWithFence(ctx, requestID, requested, confirmed, "", false, true, func(executionCtx context.Context, plan *hostv1.CleanupPlan) error {
		current, err := server.controller.store.GetEnvironment(executionCtx, plan.GetEnvironmentId())
		if err != nil {
			return err
		}
		_, err = server.controller.retainRemoteEnvironmentLocked(executionCtx, current, cleanupPlanHasMissingNamespace(plan))
		return err
	})
	if err != nil {
		return nil, err
	}
	if operation.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
		return nil, resourceError(codes.FailedPrecondition, "CLEANUP_FAILED", "allocation", request.GetAllocationId(), operation.ErrorMessage)
	}
	if request.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		responseCtx, cancelResponse := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
		defer cancelResponse()
		current, err := server.remoteEnvironment(responseCtx, request.GetRegistrationId(), request.GetAllocationId())
		if err != nil {
			return nil, err
		}
		return &hostv1.AllocationResponse{Allocation: server.controller.remoteAllocation(current)}, nil
	}
	return &hostv1.AllocationResponse{Allocation: &hostv1.RemoteAllocation{AllocationId: request.GetAllocationId(), State: hostv1.AllocationState_ALLOCATION_STATE_DISCARDED}}, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (server *remoteComputeServer) remoteEnvironment(ctx context.Context, registrationID, allocationID string) (store.Environment, error) {
	if err := authorizeRemoteRegistration(ctx, registrationID); err != nil {
		return store.Environment{}, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, registrationID)
	if err != nil {
		return store.Environment{}, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	environment, err := server.controller.store.GetRemoteEnvironment(ctx, registration.ID, allocationID)
	if err != nil {
		return store.Environment{}, resourceError(codes.NotFound, "ALLOCATION_NOT_FOUND", "allocation", allocationID, "allocation does not exist")
	}
	return environment, nil
}
