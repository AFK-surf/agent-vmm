//go:build darwin

package host

import (
	"context"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

const maxExecutionHolders = 64

type workloadExecution struct {
	id         string
	authority  string
	instanceID string
	holders    map[string]time.Time
}

func executionHolderID(request *hostv1.ExecutionRequest) string {
	if request.GetHolderId() != "" {
		return request.GetHolderId()
	}
	return request.GetExecutionId()
}

func (execution *workloadExecution) pruneExpiredHolders(now time.Time) {
	for holderID, deadline := range execution.holders {
		if !deadline.IsZero() && !now.Before(deadline) {
			delete(execution.holders, holderID)
		}
	}
}

func expireWorkloadExecutionLocked(run *workloadRun, now time.Time) bool {
	if run.execution == nil {
		return false
	}
	executionID := run.execution.id
	run.execution.pruneExpiredHolders(now)
	if len(run.execution.holders) != 0 {
		return false
	}
	run.execution = nil
	run.idleSince = now
	if !run.stopping {
		run.stopping = true
		run.stopDeadline = now.Add(workloadStopBudget)
		run.stopRequestID = derivedRequestID("expired-execution-" + executionID)
	}
	return true
}

func executionAuthority(identity auth.Identity) string {
	if identity.LeaseID != "" {
		return identity.LeaseID
	}
	return identity.SessionID
}

func (controller *Controller) currentWorkloadRunLocked(identity auth.Identity, containerID, instanceID string) (*workloadRun, error) {
	if containerID == "" || instanceID == "" {
		return nil, invalidField("container/instance", "current container instance is required")
	}
	if !controller.workloadRunsRecovered {
		return nil, retryable("VM_ADMISSION_CLOSED", "workload recovery is incomplete", time.Second)
	}
	if identity.LeaseID != "" {
		lease := controller.remoteLeases[identity.LeaseID]
		if lease == nil || lease.environmentID != identity.EnvironmentID || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE || !time.Now().Before(lease.expiresAt) {
			return nil, resourceError(codes.PermissionDenied, "STALE_LEASE", "allocation", identity.AllocationID, "execution lease is no longer current")
		}
	}
	run := controller.workloadRuns[workloadRunKey(identity.EnvironmentID, containerID)]
	if run == nil || run.instanceID != instanceID {
		return nil, resourceError(codes.Aborted, "STALE_CONTAINER_INSTANCE", "container", containerID, "container instance changed")
	}
	return run, nil
}

func (server *agentServer) AcquireExecution(ctx context.Context, request *hostv1.ExecutionRequest) (*hostv1.ExecutionResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetExecutionId() == "" || len(request.GetExecutionId()) > 256 {
		return nil, invalidField("execution_id", "original execution identity is required")
	}
	holderID := executionHolderID(request)
	if holderID == "" || len(holderID) > 256 {
		return nil, invalidField("holder_id", "execution holder identity is required")
	}
	var deadline time.Time
	if request.GetDeadlineUnixNano() != 0 {
		deadline = time.Unix(0, request.GetDeadlineUnixNano())
		if !time.Now().Before(deadline) {
			return nil, resourceError(codes.DeadlineExceeded, "EXECUTION_DEADLINE_EXCEEDED", "execution", "current", "execution deadline has expired")
		}
	}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	server.controller.mu.Lock()
	defer server.controller.mu.Unlock()
	run, err := server.controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if expireWorkloadExecutionLocked(run, time.Now()) {
		return nil, pendingWorkloadError(run, time.Now())
	}
	if run.starting || run.stopping || run.quiescing || run.inFlight {
		return nil, pendingWorkloadError(run, time.Now())
	}
	authority := executionAuthority(identity)
	acquired := false
	if run.execution != nil {
		if run.execution.id != request.GetExecutionId() || run.execution.authority != authority || run.execution.instanceID != run.instanceID {
			return nil, resourceError(codes.ResourceExhausted, "WORKLOAD_EXECUTION_BUSY", "container", run.containerID, "another execution still owns this workload")
		}
		if _, exists := run.execution.holders[holderID]; !exists {
			if len(run.execution.holders) >= maxExecutionHolders {
				return nil, resourceError(codes.ResourceExhausted, "WORKLOAD_EXECUTION_HOLDERS_FULL", "container", run.containerID, "execution holder limit reached")
			}
			run.execution.holders[holderID] = deadline
			acquired = true
		}
	} else {
		run.execution = &workloadExecution{id: request.GetExecutionId(), authority: authority, instanceID: run.instanceID, holders: map[string]time.Time{holderID: deadline}}
		run.idleSince = time.Time{}
		acquired = true
	}
	return &hostv1.ExecutionResponse{ExecutionId: run.execution.id, ContainerInstanceId: run.instanceID, Acquired: acquired}, nil
}

func (server *agentServer) ReleaseExecution(ctx context.Context, request *hostv1.ExecutionRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	server.controller.mu.Lock()
	defer server.controller.mu.Unlock()
	run, err := server.controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if run.execution != nil {
		if run.execution.id != request.GetExecutionId() || run.execution.authority != executionAuthority(identity) || run.execution.instanceID != run.instanceID {
			return nil, resourceError(codes.Aborted, "STALE_EXECUTION", "container", run.containerID, "execution completion is stale")
		}
		holderID := executionHolderID(request)
		delete(run.execution.holders, holderID)
		if len(run.execution.holders) == 0 {
			run.execution = nil
			run.idleSince = time.Now()
		}
	}
	return &emptypb.Empty{}, nil
}

// The Server asks its Runtime to quiesce only after this closes Host admission.
// Cancellation names the same command and instance; it cannot reopen a later stop.
func (server *agentServer) QuiesceContainer(ctx context.Context, request *hostv1.QuiesceContainerRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "quiesce command identity is required")
	}
	if request.GetMinimumIdleSeconds() > 86_400 {
		return nil, invalidField("minimum_idle_seconds", "minimum idle period must not exceed one day")
	}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	server.controller.mu.Lock()
	defer server.controller.mu.Unlock()
	run, err := server.controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if request.GetCancel() {
		if run.stopping || (run.quiescing && run.quiesceRequestID != request.GetRequestId()) {
			return nil, resourceError(codes.Aborted, "STALE_QUIESCE", "container", run.containerID, "quiesce command is no longer current")
		}
		if run.quiescing {
			run.quiescing = false
			run.quiesceRequestID = ""
			run.stopDeadline = time.Time{}
		}
	} else {
		if run.execution != nil {
			return nil, resourceError(codes.ResourceExhausted, "WORKLOAD_EXECUTION_BUSY", "container", run.containerID, "workload still has an active execution")
		}
		minimumIdle := time.Duration(request.GetMinimumIdleSeconds()) * time.Second
		if run.idleSince.IsZero() || time.Since(run.idleSince) < minimumIdle {
			remaining := time.Until(run.idleSince.Add(minimumIdle))
			if run.idleSince.IsZero() || remaining < time.Second {
				remaining = time.Second
			}
			return nil, resourceErrorWithMetadata(codes.ResourceExhausted, "WORKLOAD_NOT_IDLE", "container", run.containerID,
				"workload has not completed its minimum idle period", map[string]string{"retry_after_seconds": fmt.Sprintf("%d", int64(remaining.Round(time.Second)/time.Second))})
		}
		if run.starting || run.stopping || run.inFlight || run.quiescing && run.quiesceRequestID != request.GetRequestId() {
			return nil, pendingWorkloadError(run, time.Now())
		}
		if run.quiescing && !time.Now().Before(run.stopDeadline) {
			return nil, pendingWorkloadError(run, time.Now())
		}
		if !run.quiescing {
			run.quiescing = true
			run.quiesceRequestID = request.GetRequestId()
			run.stopDeadline = time.Now().Add(workloadStopBudget)
		}
	}
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: run.containerID, InstanceId: run.instanceID, State: hostv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}

func (controller *Controller) validateQuiescedStop(identity auth.Identity, request *hostv1.StopContainerRequest) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	run, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return err
	}
	if !run.quiescing || run.quiesceRequestID != request.GetQuiesceRequestId() || run.execution != nil {
		return resourceError(codes.Aborted, "STALE_QUIESCE", "container", run.containerID, "safe stop does not own current execution admission")
	}
	if !time.Now().Before(run.stopDeadline) {
		return pendingWorkloadError(run, time.Now())
	}
	return nil
}
