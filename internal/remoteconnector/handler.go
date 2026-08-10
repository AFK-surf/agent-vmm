package remoteconnector

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	connectionInstance string
	tunnels            *tunnelManager
}

func (handler commandHandler) handle(ctx context.Context, command *remotev1.ControllerCommand) *remotev1.CommandResult {
	result := &remotev1.CommandResult{CorrelationId: command.GetCorrelationId()}
	if command.GetCorrelationId() == "" || len(command.GetCorrelationId()) > 128 {
		return fail(result, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "invalid correlation ID")
	}
	var err error
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_EnsureAllocation:
		var response *hostv1.AllocationResponse
		request := value.EnsureAllocation
		response, err = handler.client.EnsureAllocation(ctx, &hostv1.EnsureAllocationRequest{AllocationId: request.GetAllocationId(), DisplayLabel: boundedLabel(request.GetDisplayLabel()), Limits: request.GetLimits(), EgressMode: request.GetEgressMode(), PolicyRevision: request.GetPolicyRevision(), ConnectionInstance: handler.connectionInstance})
		if err == nil {
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_ReleaseAllocation:
		var response *hostv1.AllocationResponse
		response, err = handler.client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{AllocationId: value.ReleaseAllocation.GetAllocationId()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(value.ReleaseAllocation.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_PlanDiscardAllocation:
		var response *hostv1.AllocationCleanupPlanResponse
		response, err = handler.client.PlanDiscardAllocation(ctx, &hostv1.PlanDiscardAllocationRequest{AllocationId: value.PlanDiscardAllocation.GetAllocationId()})
		if err == nil {
			plan := response.GetPlan()
			result.Result = &remotev1.CommandResult_CleanupPlan{CleanupPlan: &remotev1.CleanupPlan{AllocationId: plan.GetAllocationId(), Revision: plan.GetRevision(), VolumeIds: append([]string(nil), plan.GetVolumeIds()...)}}
		}
	case *remotev1.ControllerCommand_ExecuteDiscardAllocation:
		var response *hostv1.AllocationResponse
		request := value.ExecuteDiscardAllocation
		volumes := append([]string(nil), request.GetConfirmedVolumeIds()...)
		sort.Strings(volumes)
		response, err = handler.client.ExecuteDiscardAllocation(ctx, &hostv1.ExecuteDiscardAllocationRequest{AllocationId: request.GetAllocationId(), PlanRevision: request.GetPlanRevision(), ConfirmedVolumeIds: volumes})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(request.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_AcquireLease:
		request := value.AcquireLease
		var response *hostv1.LeaseResponse
		response, err = handler.client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{AllocationId: request.GetAllocationId(), Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second), WorkloadRef: boundedLabel(request.GetWorkloadRef()), ConnectionInstance: handler.connectionInstance})
		if err == nil {
			result.Result = &remotev1.CommandResult_Lease{Lease: leaseToRemote(response.GetLease())}
		}
	case *remotev1.ControllerCommand_RenewLease:
		request := value.RenewLease
		var response *hostv1.LeaseResponse
		response, err = handler.client.RenewLease(ctx, &hostv1.RenewLeaseRequest{LeaseId: request.GetLeaseId(), Generation: request.GetGeneration(), Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second)})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseLease(request.GetLeaseId())
			}
			result.Result = &remotev1.CommandResult_Lease{Lease: leaseToRemote(response.GetLease())}
		}
	case *remotev1.ControllerCommand_ReleaseLease:
		request := value.ReleaseLease
		var response *hostv1.LeaseResponse
		response, err = handler.client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{LeaseId: request.GetLeaseId(), Generation: request.GetGeneration()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseLease(request.GetLeaseId())
			}
			result.Result = &remotev1.CommandResult_Lease{Lease: leaseToRemote(response.GetLease())}
		}
	case *remotev1.ControllerCommand_DrainRegistration:
		_, err = handler.client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{ConnectionInstance: handler.connectionInstance, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING})
	case *remotev1.ControllerCommand_OpenSession:
		if handler.tunnels == nil {
			err = status.Error(codes.Unavailable, "session transport is unavailable")
			break
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
		return fail(result, reasonFor(err), safeErrorMessage(err))
	}
	result.Success = true
	return result
}

func safeErrorMessage(err error) string {
	switch reasonFor(err) {
	case remotev1.ErrorReason_ERROR_REASON_LEASE_REVOKED:
		message := status.Convert(err).Message()
		if strings.Contains(message, "while opening") {
			return "lease changed while opening the session"
		}
		if strings.Contains(message, "cannot open") {
			return "lease cannot open a runtime session"
		}
		return "lease is no longer active"
	case remotev1.ErrorReason_ERROR_REASON_LEASE_CONFLICT:
		return "allocation already has an active lease"
	case remotev1.ErrorReason_ERROR_REASON_STALE_REVISION:
		return "revision is stale"
	case remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED:
		return "capacity is exhausted"
	case remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND:
		return "allocation does not exist"
	default:
		return "command rejected"
	}
}

func fail(result *remotev1.CommandResult, reason remotev1.ErrorReason, message string) *remotev1.CommandResult {
	result.Success = false
	result.Reason = reason
	result.Message = message
	return result
}

func reasonFor(err error) remotev1.ErrorReason {
	if current, ok := status.FromError(err); ok {
		for _, detail := range current.Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				switch info.GetReason() {
				case "LEASE_REVOKED":
					return remotev1.ErrorReason_ERROR_REASON_LEASE_REVOKED
				case "ACTIVE_LEASE_EXISTS":
					return remotev1.ErrorReason_ERROR_REASON_LEASE_CONFLICT
				case "POLICY_REVISION_CONFLICT":
					return remotev1.ErrorReason_ERROR_REASON_STALE_REVISION
				case "POOL_ENVIRONMENT_LIMIT", "POOL_PER_ENVIRONMENT_LIMIT", "POOL_AGGREGATE_LIMIT":
					return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED
				case "ALLOCATION_NOT_FOUND":
					return remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND
				}
			}
		}
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT
	case codes.Unauthenticated, codes.PermissionDenied:
		return remotev1.ErrorReason_ERROR_REASON_UNAUTHENTICATED
	case codes.NotFound:
		return remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND
	case codes.AlreadyExists:
		return remotev1.ErrorReason_ERROR_REASON_LEASE_CONFLICT
	case codes.ResourceExhausted:
		return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED
	case codes.Aborted:
		return remotev1.ErrorReason_ERROR_REASON_STALE_REVISION
	case codes.FailedPrecondition:
		return remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT
	case codes.Unavailable, codes.DeadlineExceeded:
		return remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE
	default:
		_ = fmt.Sprintf("%v", err)
		return remotev1.ErrorReason_ERROR_REASON_INTERNAL
	}
}
