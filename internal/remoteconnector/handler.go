package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type commandHandler struct {
	logger             *slog.Logger
	client             connectorv1.RemoteComputeServiceClient
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
	var err error
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_EnsureAllocation:
		var response *connectorv1.AllocationResponse
		request := value.EnsureAllocation
		response, err = handler.client.EnsureAllocation(ctx, &connectorv1.EnsureAllocationRequest{RegistrationId: handler.registrationID, AllocationId: request.GetAllocationId(), DisplayLabel: boundedLabel(request.GetDisplayLabel()), EgressMode: hostv1.EgressMode(request.GetEgressMode()), PolicyRevision: request.GetPolicyRevision(), ConnectionInstance: handler.connectionInstance, Generation: request.GetGeneration(), Limits: limitsToHost(request.GetLimits())})
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
		var response *connectorv1.AllocationResponse
		response, err = handler.client.ReleaseAllocation(ctx, &connectorv1.ReleaseAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: value.ReleaseAllocation.GetAllocationId(), ExpectedPolicyRevision: value.ReleaseAllocation.GetExpectedRevision(), ExpectedGeneration: value.ReleaseAllocation.GetExpectedGeneration()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(value.ReleaseAllocation.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_DisposeAllocation:
		request := value.DisposeAllocation
		var response *connectorv1.AllocationResponse
		response, err = handler.client.DisposeAllocation(ctx, &connectorv1.DisposeAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), ExpectedPolicyRevision: request.GetExpectedRevision(), ExpectedGeneration: request.GetExpectedGeneration(), RequestId: request.GetRequestId()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(request.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		} else if reason, metadata, ok := hostv1.ErrorInfoOf(err); ok && reason == hostv1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED {
			generation, genErr := strconv.ParseUint(metadata["generation"], 10, 64)
			revision, revErr := strconv.ParseUint(metadata["revision"], 10, 64)
			if genErr == nil && revErr == nil && revision > 0 && generation == request.GetExpectedGeneration() && metadata["allocation_id"] == request.GetAllocationId() {
				result.Result = &remotev1.CommandResult_Allocation{Allocation: &remotev1.Allocation{AllocationId: request.GetAllocationId(), Generation: generation, Revision: revision}}
			}
		}
	case *remotev1.ControllerCommand_PlanCleanupAllocation:
		var response *connectorv1.AllocationCleanupPlanResponse
		request := value.PlanCleanupAllocation
		response, err = handler.client.PlanCleanupAllocation(ctx, &connectorv1.PlanCleanupAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), Kind: hostv1.CleanupKind(request.GetKind())})
		if err == nil {
			plan := response.GetPlan()
			result.Result = &remotev1.CommandResult_CleanupPlan{CleanupPlan: &remotev1.CleanupPlan{AllocationId: plan.GetAllocationId(), AllocationGeneration: plan.GetAllocationGeneration(), Revision: plan.GetRevision(), VolumeIds: append([]string(nil), plan.GetVolumeIds()...), Kind: remotev1.CleanupKind(plan.GetKind()), Impacts: cloneCleanupImpacts(plan.GetImpacts())}}
		}
	case *remotev1.ControllerCommand_ExecuteCleanupAllocation:
		var response *connectorv1.AllocationResponse
		request := value.ExecuteCleanupAllocation
		volumes := append([]string(nil), request.GetConfirmedVolumeIds()...)
		sort.Strings(volumes)
		response, err = handler.client.ExecuteCleanupAllocation(ctx, &connectorv1.ExecuteCleanupAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), Kind: hostv1.CleanupKind(request.GetKind()), PlanRevision: request.GetPlanRevision(), ConfirmedVolumeIds: volumes})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(request.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_DrainRegistration:
		_, err = handler.client.SetRemoteAdmissionState(ctx, &connectorv1.SetRemoteAdmissionStateRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING})
	case *remotev1.ControllerCommand_OpenSession:
		if handler.tunnels == nil {
			return fail(result, remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE, "session transport is unavailable")
		}
		var ready *remotev1.SessionReady
		ready, err = handler.tunnels.Open(ctx, value.OpenSession)
		if err == nil {
			result.Result = &remotev1.CommandResult_SessionReady{SessionReady: ready}
		}
	default:
		return fail(result, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "unknown command")
	}
	if err != nil {
		handler.logger.Warn("remote compute command failed", "command_id", command.GetCommandId(), "kind", fmt.Sprintf("%T", command.GetCommand()), "timeout_millis", command.GetTimeoutMillis(), "error", err)
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
	reason, metadata, ok := hostv1.ErrorInfoOf(err)
	if !ok || reason != hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED {
		return remotev1.CapacityDimension_CAPACITY_DIMENSION_UNSPECIFIED
	}
	switch metadata["resource_dimension"] {
	case "pids":
		return remotev1.CapacityDimension_CAPACITY_DIMENSION_PIDS
	case "writable":
		return remotev1.CapacityDimension_CAPACITY_DIMENSION_WRITABLE
	case "storage_headroom":
		return remotev1.CapacityDimension_CAPACITY_DIMENSION_STORAGE_HEADROOM
	case "import_slot":
		return remotev1.CapacityDimension_CAPACITY_DIMENSION_IMPORT_SLOT
	}
	return remotev1.CapacityDimension_CAPACITY_DIMENSION_UNSPECIFIED
}

func cloneCleanupImpacts(values []*hostv1.CleanupImpact) []*remotev1.CleanupImpact {
	result := make([]*remotev1.CleanupImpact, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		copy := &remotev1.CleanupImpact{Effect: value.GetEffect(), Irreversible: value.GetIrreversible(), Blocker: value.GetBlocker()}
		if resource := value.GetResource(); resource != nil {
			copy.Resource = &remotev1.ResourceReference{Kind: resource.GetKind(), ResourceId: resource.GetResourceId()}
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

func hostRejection(err error) (remotev1.ErrorReason, bool) {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE, false
	case codes.Unknown, codes.Internal, codes.DataLoss:
		return remotev1.ErrorReason_ERROR_REASON_INTERNAL, false
	}
	reason, _ := hostv1.ErrorReasonOf(err)
	switch reason {
	case hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_DRAINING:
		return remotev1.ErrorReason_ERROR_REASON_ADMISSION_PAUSED, true
	case hostv1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED:
		return remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED, true
	case hostv1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH:
		return remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH, true
	case hostv1.ErrorReason_ERROR_REASON_ALLOCATION_CONFLICT, hostv1.ErrorReason_ERROR_REASON_POOL_EGRESS_LIMIT:
		return remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT, true
	case hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED:
		return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED, true
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.OutOfRange:
		return remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, true
	case codes.NotFound:
		return remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND, true
	case codes.FailedPrecondition:
		return remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT, true
	case codes.Aborted, codes.AlreadyExists:
		return remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, true
	case codes.PermissionDenied, codes.Unauthenticated:
		return remotev1.ErrorReason_ERROR_REASON_UNAUTHENTICATED, true
	case codes.ResourceExhausted:
		return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED, true
	default:
		return remotev1.ErrorReason_ERROR_REASON_INTERNAL, false
	}
}
