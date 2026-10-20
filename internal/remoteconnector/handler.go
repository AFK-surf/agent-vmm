package remoteconnector

import (
	"context"
	"errors"
	"log"
	"sort"
	"strconv"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type commandHandler struct {
	client             hostv1.RemoteComputeServiceClient
	registrationID     string
	connectionInstance string
	connectionEpoch    uint64
	tunnels            *tunnelManager
}

func (handler commandHandler) handle(ctx context.Context, command *remotev1.ControllerCommand) *remotev1.CommandResult {
	result := &remotev1.CommandResult{
		CommandId:       command.GetCommandId(),
		Sequence:        command.GetSequence(),
		ConnectionEpoch: command.GetConnectionEpoch(),
	}
	if command.GetCommandId() == "" || len(command.GetCommandId()) > 128 {
		return fail(result, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "invalid command ID")
	}
	var err error
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_EnsureAllocation:
		var response *hostv1.AllocationResponse
		request := value.EnsureAllocation
		response, err = handler.client.EnsureAllocation(ctx, &hostv1.EnsureAllocationRequest{RegistrationId: handler.registrationID, AllocationId: request.GetAllocationId(), DisplayLabel: boundedLabel(request.GetDisplayLabel()), EgressMode: request.GetEgressMode(), PolicyRevision: request.GetPolicyRevision(), ConnectionInstance: handler.connectionInstance, Generation: request.GetGeneration(), ResourcesV2: request.GetResourcesV2()})
		if err == nil {
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		} else {
			for _, detail := range status.Convert(err).Details() {
				if allocation, ok := detail.(*hostv1.RemoteAllocation); ok {
					result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(allocation)}
				}
			}
		}
	case *remotev1.ControllerCommand_ReleaseAllocation:
		var response *hostv1.AllocationResponse
		if value.ReleaseAllocation.GetExpectedRevision() == 0 || value.ReleaseAllocation.GetExpectedRevision() != command.GetTargetRevision() || value.ReleaseAllocation.GetExpectedGeneration() == 0 {
			return fail(result, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "allocation release revision is stale")
		}
		response, err = handler.client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: value.ReleaseAllocation.GetAllocationId(), ExpectedPolicyRevision: value.ReleaseAllocation.GetExpectedRevision(), ExpectedGeneration: value.ReleaseAllocation.GetExpectedGeneration()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(value.ReleaseAllocation.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_DisposeAllocation:
		request := value.DisposeAllocation
		if request.GetExpectedRevision() == 0 || request.GetExpectedRevision() != command.GetTargetRevision() || request.GetExpectedGeneration() == 0 {
			return fail(result, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "allocation deletion revision is stale")
		}
		var response *hostv1.AllocationResponse
		response, err = handler.client.DisposeAllocation(ctx, &hostv1.DisposeAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), ExpectedPolicyRevision: request.GetExpectedRevision(), ExpectedGeneration: request.GetExpectedGeneration(), RequestId: request.GetRequestId()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(request.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		} else if reasonFor(err) == remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED {
			for _, detail := range status.Convert(err).Details() {
				info, ok := detail.(*errdetails.ErrorInfo)
				if !ok || info.GetReason() != "DISPOSAL_REVISION_NOT_ACCEPTED" {
					continue
				}
				generation, genErr := strconv.ParseUint(info.Metadata["generation"], 10, 64)
				revision, revErr := strconv.ParseUint(info.Metadata["revision"], 10, 64)
				if genErr == nil && revErr == nil && revision > 0 && generation == request.GetExpectedGeneration() && info.Metadata["allocation_id"] == request.GetAllocationId() {
					result.Result = &remotev1.CommandResult_Allocation{Allocation: &remotev1.Allocation{AllocationId: request.GetAllocationId(), Generation: generation, Revision: revision}}
				}
			}
		}
	case *remotev1.ControllerCommand_PlanCleanupAllocation:
		var response *hostv1.AllocationCleanupPlanResponse
		request := value.PlanCleanupAllocation
		response, err = handler.client.PlanCleanupAllocation(ctx, &hostv1.PlanCleanupAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), Kind: request.GetKind()})
		if err == nil {
			plan := response.GetPlan()
			result.Result = &remotev1.CommandResult_CleanupPlan{CleanupPlan: &remotev1.CleanupPlan{AllocationId: plan.GetAllocationId(), AllocationGeneration: plan.GetAllocationGeneration(), Revision: plan.GetRevision(), VolumeIds: append([]string(nil), plan.GetVolumeIds()...), Kind: plan.GetKind(), Impacts: cloneCleanupImpacts(plan.GetImpacts())}}
		}
	case *remotev1.ControllerCommand_ExecuteCleanupAllocation:
		var response *hostv1.AllocationResponse
		request := value.ExecuteCleanupAllocation
		volumes := append([]string(nil), request.GetConfirmedVolumeIds()...)
		sort.Strings(volumes)
		response, err = handler.client.ExecuteCleanupAllocation(ctx, &hostv1.ExecuteCleanupAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), Kind: request.GetKind(), PlanRevision: request.GetPlanRevision(), ConfirmedVolumeIds: volumes})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(request.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_DrainRegistration:
		_, err = handler.client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING})
	case *remotev1.ControllerCommand_OpenSession:
		if handler.tunnels == nil {
			return fail(result, remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE, "session transport is unavailable")
		}
		var ready *remotev1.SessionReady
		ready, err = handler.tunnels.Open(ctx, value.OpenSession)
		if err == nil {
			result.Result = &remotev1.CommandResult_SessionReady{SessionReady: ready}
		}
	case *remotev1.ControllerCommand_AcquireHostLoopbackFrontend:
		request := value.AcquireHostLoopbackFrontend
		var response *hostv1.HostLoopbackFrontendLease
		response, err = handler.client.AcquireHostLoopbackFrontend(ctx, &hostv1.AcquireHostLoopbackFrontendRequest{RegistrationId: handler.registrationID,
			ConnectionInstance: handler.connectionInstance, ConnectionEpoch: handler.connectionEpoch, AllocationId: request.GetAllocationId(),
			AllocationGeneration: request.GetAllocationGeneration(), TargetRevision: command.GetTargetRevision(), AssignmentId: request.GetAssignmentId(),
			Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second)})
		if err == nil {
			result.Result = &remotev1.CommandResult_HostLoopbackFrontendLease{HostLoopbackFrontendLease: response}
		}
	case *remotev1.ControllerCommand_RenewHostLoopbackFrontend:
		request := value.RenewHostLoopbackFrontend
		var response *hostv1.HostLoopbackFrontendLease
		response, err = handler.client.RenewHostLoopbackFrontend(ctx, &hostv1.RenewHostLoopbackFrontendRequest{RegistrationId: handler.registrationID,
			ConnectionInstance: handler.connectionInstance, ConnectionEpoch: handler.connectionEpoch, FrontendLeaseId: request.GetFrontendLeaseId(), Generation: request.GetGeneration(),
			Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second)})
		if err == nil {
			result.Result = &remotev1.CommandResult_HostLoopbackFrontendLease{HostLoopbackFrontendLease: response}
		}
	case *remotev1.ControllerCommand_ReleaseHostLoopbackFrontend:
		request := value.ReleaseHostLoopbackFrontend
		_, err = handler.client.ReleaseHostLoopbackFrontend(ctx, &hostv1.ReleaseHostLoopbackFrontendRequest{RegistrationId: handler.registrationID,
			ConnectionInstance: handler.connectionInstance, ConnectionEpoch: handler.connectionEpoch, FrontendLeaseId: request.GetFrontendLeaseId(), Generation: request.GetGeneration()})
	default:
		return fail(result, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "unknown command")
	}
	if err != nil {
		log.Printf("remote compute command failed: command=%s kind=%T deadline_ms=%d error=%v details=%v", command.GetCommandId(), command.GetCommand(), command.GetDeadlineUnixMillis(), err, status.Convert(err).Details())
		result.CapacityDimension = capacityDimensionFor(err)
		var local *localRejection
		if errors.As(err, &local) {
			return fail(result, local.reason, err.Error())
		}
		reason, rejected := hostRejection(err)
		result.Outcome = remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN
		if rejected {
			result.Outcome = remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED
		}
		result.Reason = reason
		result.Message = err.Error()
		return result
	}
	result.Outcome = remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED
	return result
}

func capacityDimensionFor(err error) remotev1.CapacityDimension {
	current, ok := status.FromError(err)
	if !ok {
		return remotev1.CapacityDimension_CAPACITY_DIMENSION_UNSPECIFIED
	}
	for _, detail := range current.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			switch info.GetMetadata()["resource_dimension"] {
			case "cpu_max":
				return remotev1.CapacityDimension_CAPACITY_DIMENSION_CPU_MAX
			case "memory":
				return remotev1.CapacityDimension_CAPACITY_DIMENSION_MEMORY
			case "pids":
				return remotev1.CapacityDimension_CAPACITY_DIMENSION_PIDS
			case "writable":
				return remotev1.CapacityDimension_CAPACITY_DIMENSION_WRITABLE
			case "storage_headroom":
				return remotev1.CapacityDimension_CAPACITY_DIMENSION_STORAGE_HEADROOM
			case "import_slot":
				return remotev1.CapacityDimension_CAPACITY_DIMENSION_IMPORT_SLOT
			}
		}
	}
	return remotev1.CapacityDimension_CAPACITY_DIMENSION_UNSPECIFIED
}

func cloneCleanupImpacts(values []*hostv1.CleanupImpact) []*hostv1.CleanupImpact {
	result := make([]*hostv1.CleanupImpact, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		copy := &hostv1.CleanupImpact{Effect: value.GetEffect(), Irreversible: value.GetIrreversible(), Blocker: value.GetBlocker()}
		if resource := value.GetResource(); resource != nil {
			copy.Resource = &hostv1.ResourceReference{Kind: resource.GetKind(), ResourceId: resource.GetResourceId()}
		}
		result = append(result, copy)
	}
	return result
}

func fail(result *remotev1.CommandResult, reason remotev1.ErrorReason, message string) *remotev1.CommandResult {
	result.Outcome = remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED
	result.Reason = reason
	result.Message = message
	return result
}

type localRejection struct {
	reason  remotev1.ErrorReason
	message string
}

func (rejection *localRejection) Error() string { return rejection.message }

func reasonFor(err error) remotev1.ErrorReason {
	reason, _ := hostRejection(err)
	return reason
}

func hostRejection(err error) (remotev1.ErrorReason, bool) {
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		switch info.GetReason() {
		case "VM_ADMISSION_CLOSED", "REGISTRATION_DRAINING":
			return remotev1.ErrorReason_ERROR_REASON_ADMISSION_PAUSED, true
		case "INVALID_ARGUMENT":
			return remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, true
		case "REGISTRATION_MISMATCH":
			return remotev1.ErrorReason_ERROR_REASON_UNAUTHENTICATED, true
		case "DISPOSAL_REVISION_NOT_ACCEPTED":
			return remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED, true
		case "STALE_CONNECTION", "POLICY_REVISION_CONFLICT", "STALE_GENERATION", "STALE_RUNTIME_SESSION", "PLAN_STALE", "ALLOCATION_DISPOSAL_CONFLICT", "INVENTORY_REVISION_CONFLICT", "ADMISSION_STATE_CONFLICT", "POOL_REVISION_CONFLICT":
			return remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, true
		case "STALE_CONNECTION_EPOCH":
			return remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH, true
		case "REGISTRATION_NOT_ENABLED", "ALLOCATION_CONFLICT", "POOL_EGRESS_LIMIT":
			return remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT, true
		case "RESOURCE_LEDGER_EXHAUSTED", "POOL_PER_ENVIRONMENT_LIMIT":
			return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED, true
		case "ALLOCATION_NOT_FOUND":
			return remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND, true
		case "ALLOCATION_PROJECTION_FAILED", "RESOURCE_CAPACITY_EXHAUSTED":
			for _, detail := range status.Convert(err).Details() {
				if allocation, ok := detail.(*hostv1.RemoteAllocation); ok && allocation.GetAllocationId() != "" && allocation.GetGeneration() != 0 {
					if info.GetReason() == "RESOURCE_CAPACITY_EXHAUSTED" {
						return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED, true
					}
					return remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE, true
				}
			}
		}
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE, false
	default:
		return remotev1.ErrorReason_ERROR_REASON_INTERNAL, false
	}
}
