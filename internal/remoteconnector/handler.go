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
		}
	case *remotev1.ControllerCommand_ReleaseAllocation:
		var response *hostv1.AllocationResponse
		if value.ReleaseAllocation.GetExpectedRevision() == 0 || value.ReleaseAllocation.GetExpectedRevision() != command.GetTargetRevision() || value.ReleaseAllocation.GetExpectedGeneration() == 0 {
			err = status.Error(codes.Aborted, "allocation release revision is stale")
			break
		}
		response, err = handler.client.ReleaseAllocation(ctx, &hostv1.ReleaseAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: value.ReleaseAllocation.GetAllocationId(), ExpectedPolicyRevision: value.ReleaseAllocation.GetExpectedRevision(), ExpectedGeneration: value.ReleaseAllocation.GetExpectedGeneration()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(value.ReleaseAllocation.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_PlanCleanupAllocation:
		var response *hostv1.AllocationCleanupPlanResponse
		request := value.PlanCleanupAllocation
		response, err = handler.client.PlanCleanupAllocation(ctx, &hostv1.PlanCleanupAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), Kind: request.GetKind()})
		if err == nil {
			plan := response.GetPlan()
			result.Result = &remotev1.CommandResult_CleanupPlan{CleanupPlan: &remotev1.CleanupPlan{AllocationId: plan.GetAllocationId(), Revision: plan.GetRevision(), VolumeIds: append([]string(nil), plan.GetVolumeIds()...), Kind: plan.GetKind(), Impacts: cloneCleanupImpacts(plan.GetImpacts())}}
		}
	case *remotev1.ControllerCommand_ExecuteCleanupAllocation:
		var response *hostv1.AllocationResponse
		request := value.ExecuteCleanupAllocation
		volumes := append([]string(nil), request.GetConfirmedVolumeIds()...)
		sort.Strings(volumes)
		response, err = handler.client.ExecuteCleanupAllocation(ctx, &hostv1.ExecuteCleanupAllocationRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, AllocationId: request.GetAllocationId(), Kind: request.GetKind(), PlanRevision: request.GetPlanRevision(), ConfirmedVolumeIds: volumes, ExpectedLeaseId: request.GetExpectedLeaseId(), ExpectedLeaseGeneration: request.GetExpectedLeaseGeneration()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseAllocation(request.GetAllocationId())
			}
			result.Result = &remotev1.CommandResult_Allocation{Allocation: allocationToRemote(response.GetAllocation())}
		}
	case *remotev1.ControllerCommand_AcquireLease:
		request := value.AcquireLease
		var response *hostv1.LeaseResponse
		response, err = handler.client.AcquireLease(ctx, &hostv1.AcquireLeaseRequest{RegistrationId: handler.registrationID, AllocationId: request.GetAllocationId(), Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second), WorkloadRef: boundedLabel(request.GetWorkloadRef()), ConnectionInstance: handler.connectionInstance, ExpectedPolicyRevision: command.GetTargetRevision()})
		if err == nil {
			result.Result = &remotev1.CommandResult_Lease{Lease: leaseToRemote(response.GetLease(), response.GetAllocationRevision())}
		}
	case *remotev1.ControllerCommand_RenewLease:
		request := value.RenewLease
		var response *hostv1.LeaseResponse
		response, err = handler.client.RenewLease(ctx, &hostv1.RenewLeaseRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, LeaseId: request.GetLeaseId(), Generation: request.GetGeneration(), Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second)})
		if err == nil {
			result.Result = &remotev1.CommandResult_Lease{Lease: leaseToRemote(response.GetLease(), response.GetAllocationRevision())}
		}
	case *remotev1.ControllerCommand_ReleaseLease:
		request := value.ReleaseLease
		var response *hostv1.LeaseResponse
		response, err = handler.client.ReleaseLease(ctx, &hostv1.ReleaseLeaseRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, LeaseId: request.GetLeaseId(), Generation: request.GetGeneration()})
		if err == nil {
			if handler.tunnels != nil {
				handler.tunnels.CloseLease(request.GetLeaseId())
			}
			result.Result = &remotev1.CommandResult_Lease{Lease: leaseToRemote(response.GetLease(), response.GetAllocationRevision())}
		}
	case *remotev1.ControllerCommand_DrainRegistration:
		_, err = handler.client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{RegistrationId: handler.registrationID, ConnectionInstance: handler.connectionInstance, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING})
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
	case *remotev1.ControllerCommand_AcquireHostLoopbackFrontend:
		request := value.AcquireHostLoopbackFrontend
		var response *hostv1.HostLoopbackFrontendLease
		response, err = handler.client.AcquireHostLoopbackFrontend(ctx, &hostv1.AcquireHostLoopbackFrontendRequest{RegistrationId: handler.registrationID,
			ConnectionInstance: handler.connectionInstance, ConnectionEpoch: handler.connectionEpoch, AllocationId: request.GetAllocationId(), AllocationLeaseId: request.GetAllocationLeaseId(),
			AllocationLeaseGeneration: request.GetAllocationLeaseGeneration(), TargetRevision: command.GetTargetRevision(), AssignmentId: request.GetAssignmentId(),
			Duration: durationpb.New(time.Duration(request.GetDurationSeconds()) * time.Second)})
		if err == nil {
			result.Result = &remotev1.CommandResult_HostLoopbackFrontendLease{HostLoopbackFrontendLease: response}
		}
	case *remotev1.ControllerCommand_RenewHostLoopbackFrontend:
		request := value.RenewHostLoopbackFrontend
		var response *hostv1.HostLoopbackFrontendLease
		response, err = handler.client.RenewHostLoopbackFrontend(ctx, &hostv1.RenewHostLoopbackFrontendRequest{RegistrationId: handler.registrationID,
			ConnectionInstance: handler.connectionInstance, ConnectionEpoch: handler.connectionEpoch, FrontendLeaseId: request.GetFrontendLeaseId(), Generation: request.GetGeneration(),
			AllocationLeaseId: request.GetAllocationLeaseId(), AllocationLeaseGeneration: request.GetAllocationLeaseGeneration(),
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
		result.CapacityDimension = capacityDimensionFor(err)
		return fail(result, reasonFor(err), safeErrorMessage(err))
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
	result.Outcome = remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED
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
				case "POOL_PER_ENVIRONMENT_LIMIT":
					return remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED
				case "POOL_ENVIRONMENT_LIMIT":
					return remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT
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
