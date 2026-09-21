package mcpappliance

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"github.com/AFK-surf/agent-vmm/internal/mcpserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clientCleanupOwner struct {
	controller           *Controller
	live                 *controlConnection
	clientID             string
	allocationID         string
	allocationGeneration uint64

	mu        sync.Mutex
	execution chan bool
	succeeded bool
}

func (owner *clientCleanupOwner) PlanCleanup(ctx context.Context, kind string) (mcpserver.CleanupPlan, error) {
	remoteKind, err := cleanupKind(kind)
	if err != nil {
		return mcpserver.CleanupPlan{}, err
	}
	commandID, err := randomID(owner.clientID + "-cleanup-plan-")
	if err != nil {
		return mcpserver.CleanupPlan{}, err
	}
	commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
	result, err := owner.controller.dispatchOn(commandContext, owner.live, &remotev1.ControllerCommand{CommandId: commandID, Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: owner.allocationID, Kind: remoteKind}}})
	cancel()
	if err != nil {
		owner.controller.logger.Warn("plan MCP cleanup", "allocation", owner.allocationID, "kind", kind, "error", err)
		return mcpserver.CleanupPlan{}, err
	}
	plan := result.GetCleanupPlan()
	if plan == nil || plan.GetAllocationId() != owner.allocationID || plan.GetRevision() == 0 || plan.GetKind() != remoteKind || len(plan.GetImpacts()) > 1000 {
		return mcpserver.CleanupPlan{}, errors.New("cleanup plan result is incomplete")
	}
	projection := mcpserver.CleanupPlan{Kind: kind, Revision: plan.GetRevision(), VolumeIDs: append([]string(nil), plan.GetVolumeIds()...), Impacts: make([]mcpserver.CleanupImpact, 0, len(plan.GetImpacts()))}
	slices.Sort(projection.VolumeIDs)
	impactVolumes := make([]string, 0, len(projection.VolumeIDs))
	for _, impact := range plan.GetImpacts() {
		resource := impact.GetResource()
		if resource == nil || resource.GetKind() == "" || resource.GetResourceId() == "" || impact.GetEffect() == "" {
			return mcpserver.CleanupPlan{}, errors.New("cleanup plan impact is incomplete")
		}
		projection.Impacts = append(projection.Impacts, mcpserver.CleanupImpact{Resource: mcpserver.Resource{Kind: resource.GetKind(), ID: resource.GetResourceId()}, Effect: impact.GetEffect(), Irreversible: impact.GetIrreversible(), Blocker: impact.GetBlocker()})
		if resource.GetKind() == "volume" {
			impactVolumes = append(impactVolumes, resource.GetResourceId())
		}
	}
	slices.Sort(impactVolumes)
	if hasDuplicate(projection.VolumeIDs) || !slices.Equal(projection.VolumeIDs, impactVolumes) {
		return mcpserver.CleanupPlan{}, errors.New("cleanup plan volume set is incomplete")
	}
	return projection, nil
}

func hasDuplicate(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}

func (owner *clientCleanupOwner) ExecuteCleanup(ctx context.Context, execution mcpserver.CleanupExecution) (err error) {
	remoteKind, err := cleanupKind(execution.Kind)
	if err != nil {
		return err
	}
	outcome, err := owner.beginExecution()
	if err != nil {
		return err
	}
	succeeded := false
	defer func() { owner.finishExecution(outcome, succeeded) }()
	commandID, err := randomID(owner.clientID + "-cleanup-execute-")
	if err != nil {
		return err
	}
	volumes := append([]string(nil), execution.ConfirmedVolumeIDs...)
	slices.Sort(volumes)
	commandContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
	result, err := owner.controller.dispatchOn(commandContext, owner.live, &remotev1.ControllerCommand{CommandId: commandID, TargetRevision: execution.Revision, Command: &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: owner.allocationID, PlanRevision: execution.Revision, ConfirmedVolumeIds: volumes, Kind: remoteKind}}})
	cancel()
	if err != nil {
		return err
	}
	allocation := result.GetAllocation()
	if allocation == nil || allocation.GetAllocationId() != owner.allocationID {
		return errors.New("cleanup result is incomplete")
	}
	switch remoteKind {
	case hostv1.CleanupKind_CLEANUP_KIND_RESET:
		if allocation.GetState() != hostv1.AllocationState_ALLOCATION_STATE_RETAINED {
			return errors.New("reset result has an invalid allocation state")
		}
	case hostv1.CleanupKind_CLEANUP_KIND_DISCARD:
		if allocation.GetState() != hostv1.AllocationState_ALLOCATION_STATE_DISCARDED {
			return errors.New("discard result has an invalid allocation state")
		}
	}
	succeeded = true
	return nil
}

func cleanupKind(kind string) (hostv1.CleanupKind, error) {
	switch kind {
	case "reset":
		return hostv1.CleanupKind_CLEANUP_KIND_RESET, nil
	case "discard":
		return hostv1.CleanupKind_CLEANUP_KIND_DISCARD, nil
	default:
		return hostv1.CleanupKind_CLEANUP_KIND_UNSPECIFIED, status.Error(codes.InvalidArgument, "invalid cleanup kind")
	}
}

func (owner *clientCleanupOwner) beginExecution() (chan bool, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.execution != nil {
		return nil, status.Error(codes.FailedPrecondition, "cleanup is already executing")
	}
	owner.execution = make(chan bool, 1)
	return owner.execution, nil
}

func (owner *clientCleanupOwner) finishExecution(outcome chan bool, succeeded bool) {
	owner.mu.Lock()
	owner.succeeded = succeeded
	outcome <- succeeded
	close(outcome)
	if !succeeded && owner.execution == outcome {
		owner.execution = nil
	}
	owner.mu.Unlock()
}

func (owner *clientCleanupOwner) completedSuccessfully() bool {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.succeeded
}

func (owner *clientCleanupOwner) waitForExecution() bool {
	owner.mu.Lock()
	outcome := owner.execution
	owner.mu.Unlock()
	if outcome == nil {
		return false
	}
	return <-outcome
}
