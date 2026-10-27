//go:build darwin || linux

package workload

import (
	"context"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxWorkloadExecutions = 64

type workloadExecution struct {
	id         string
	authority  string
	owner      string
	instanceID string
	kind       hostv1.ExecutionKind
	state      hostv1.ExecutionState
	acquiredAt time.Time
	deadline   time.Time
}

type ExecutionToken struct {
	environmentID string
	containerID   string
	instanceID    string
	executionID   string
	authority     string
	owner         string
}

func validExecutionKind(kind hostv1.ExecutionKind) bool {
	switch kind {
	case hostv1.ExecutionKind_EXECUTION_KIND_MAIN_EXECUTION,
		hostv1.ExecutionKind_EXECUTION_KIND_AUTH_OPERATION,
		hostv1.ExecutionKind_EXECUTION_KIND_MIGRATION_EXPORT,
		hostv1.ExecutionKind_EXECUTION_KIND_MIGRATION_IMPORT,
		hostv1.ExecutionKind_EXECUTION_KIND_EXEC,
		hostv1.ExecutionKind_EXECUTION_KIND_PROCESS,
		hostv1.ExecutionKind_EXECUTION_KIND_BUILD:
		return true
	default:
		return false
	}
}

func markExpiredExecutionsUnknownLocked(run *Run, now time.Time) {
	for _, execution := range run.executions {
		if execution.state == hostv1.ExecutionState_EXECUTION_STATE_ACTIVE && !execution.deadline.IsZero() && !now.Before(execution.deadline) {
			execution.state = hostv1.ExecutionState_EXECUTION_STATE_UNKNOWN
		}
	}
}

func executionDeadline(request *hostv1.AcquireExecutionRequest, now time.Time) (time.Time, error) {
	if request.GetKind() == hostv1.ExecutionKind_EXECUTION_KIND_MAIN_EXECUTION {
		if request.GetDeadlineUnixNano() != 0 {
			return time.Time{}, hostv1.InvalidField("deadline_unix_nano", "main execution deadline must be zero")
		}
		return time.Time{}, nil
	}
	if request.GetDeadlineUnixNano() == 0 {
		return time.Time{}, hostv1.InvalidField("deadline_unix_nano", "operation deadline is required")
	}
	deadline := time.Unix(0, request.GetDeadlineUnixNano())
	if !now.Before(deadline) {
		return time.Time{}, status.Error(codes.DeadlineExceeded, "execution deadline has expired")
	}
	return deadline, nil
}

func executionAuthority(identity auth.Identity) string {
	// The allocation is the stable remote owner across controller sessions.
	// Local callers fall back to their session.
	if identity.AllocationID != "" {
		return identity.AllocationID
	}
	return identity.SessionID
}

func executionOwner(identity auth.Identity) string {
	if identity.ExecutionOwnerID != "" {
		return identity.ExecutionOwnerID
	}
	return identity.SessionID
}

func (controller *Owner) AcquireStreamExecution(ctx context.Context, identity auth.Identity, containerID, instanceID, executionID string, kind hostv1.ExecutionKind, deadlineUnixNano int64) (*ExecutionToken, error) {
	if executionID == "" || len(executionID) > 256 {
		return nil, hostv1.InvalidField("execution_id", "activity identity is required")
	}
	if kind != hostv1.ExecutionKind_EXECUTION_KIND_EXEC && kind != hostv1.ExecutionKind_EXECUTION_KIND_PROCESS && kind != hostv1.ExecutionKind_EXECUTION_KIND_BUILD {
		return nil, hostv1.InvalidField("execution_kind", "stream execution kind is required")
	}
	now := time.Now()
	deadline, err := executionDeadline(&hostv1.AcquireExecutionRequest{Kind: kind, DeadlineUnixNano: deadlineUnixNano}, now)
	if err != nil {
		return nil, err
	}
	unlock, lockErr := controller.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	run, err := controller.currentWorkloadRunLocked(identity, containerID, instanceID)
	if err != nil {
		return nil, err
	}
	markExpiredExecutionsUnknownLocked(run, now)
	if run.starting || run.stopping || run.quiescing || run.inFlight {
		return nil, pendingWorkloadError(run, now)
	}
	authority := executionAuthority(identity)
	owner := executionOwner(identity)
	if execution := run.executions[executionID]; execution != nil {
		if execution.authority != authority || execution.owner != owner || execution.instanceID != run.instanceID || execution.kind != kind {
			return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_EXECUTION, "activity identity belongs to another owner or target", hostv1.WithResource("container", run.containerID))
		}
		return nil, status.Error(codes.Aborted, "activity was already accepted; observe its terminal result")
	}
	if len(run.executions) >= maxWorkloadExecutions {
		return nil, status.Error(codes.ResourceExhausted, "workload activity limit reached")
	}
	if run.executions == nil {
		run.executions = make(map[string]*workloadExecution)
	}
	run.executions[executionID] = &workloadExecution{id: executionID, authority: authority, owner: owner, instanceID: run.instanceID, kind: kind, state: hostv1.ExecutionState_EXECUTION_STATE_ACTIVE, acquiredAt: now, deadline: deadline}
	run.idleSince = time.Time{}
	return &ExecutionToken{environmentID: identity.EnvironmentID, containerID: run.containerID, instanceID: run.instanceID, executionID: executionID, authority: authority, owner: owner}, nil
}

// ReleaseStreamExecution consumes only terminal evidence from the Host-owned
// guest stream. It intentionally does not require the control lease/session to
// remain current after the operation was accepted.
func (controller *Owner) ReleaseStreamExecution(token *ExecutionToken) {
	if token == nil {
		return
	}
	unlock, lockErr := controller.LockEnvironment(context.Background(), token.environmentID)
	if lockErr != nil {
		return
	}

	defer unlock()
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	run := controller.workloadRuns[workloadRunKey(token.environmentID, token.containerID)]
	if run == nil || run.instanceID != token.instanceID {
		return
	}
	execution := run.executions[token.executionID]
	if execution == nil || execution.authority != token.authority || execution.owner != token.owner || execution.instanceID != token.instanceID {
		return
	}
	delete(run.executions, token.executionID)
	if len(run.executions) == 0 {
		run.idleSince = time.Now()
	}
}

func (controller *Owner) currentWorkloadRunLocked(identity auth.Identity, containerID, instanceID string) (*Run, error) {
	if containerID == "" || instanceID == "" {
		return nil, hostv1.InvalidField("container/instance", "current container instance is required")
	}
	if !controller.workloadRunsRecovered {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED, "workload recovery is incomplete", hostv1.WithRetry(time.Second))
	}
	run := controller.workloadRuns[workloadRunKey(identity.EnvironmentID, containerID)]
	if run == nil || run.instanceID != instanceID {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_CONTAINER_INSTANCE, "container instance changed", hostv1.WithResource("container", containerID))
	}
	return run, nil
}

func (controller *Owner) AcquireExecution(ctx context.Context, request *hostv1.AcquireExecutionRequest) (*hostv1.AcquireExecutionResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetExecutionId() == "" || len(request.GetExecutionId()) > 256 {
		return nil, hostv1.InvalidField("execution_id", "activity identity is required")
	}
	if request.GetHolderId() != "" {
		return nil, hostv1.InvalidField("holder_id", "execution holders are not supported")
	}
	if !validExecutionKind(request.GetKind()) {
		return nil, hostv1.InvalidField("kind", "execution kind is required")
	}
	now := time.Now()
	unlock, lockErr := controller.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	run, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if controller.workloadReclaim != nil && controller.workloadReclaim.run == run && !run.stopping && !run.quiescing {
		controller.clearWorkloadReclaimLocked("demand")
	}
	markExpiredExecutionsUnknownLocked(run, now)
	if run.starting || run.stopping || run.quiescing || run.inFlight {
		return nil, pendingWorkloadError(run, time.Now())
	}
	authority := executionAuthority(identity)
	owner := executionOwner(identity)
	acquired := false
	if execution := run.executions[request.GetExecutionId()]; execution != nil {
		if execution.authority != authority || execution.owner != owner || execution.instanceID != run.instanceID || execution.kind != request.GetKind() {
			return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_EXECUTION, "activity identity belongs to another owner or target", hostv1.WithResource("container", run.containerID))
		}
	} else {
		deadline, err := executionDeadline(request, now)
		if err != nil {
			return nil, err
		}
		if len(run.executions) >= maxWorkloadExecutions {
			return nil, status.Error(codes.ResourceExhausted, "workload activity limit reached")
		}
		if run.executions == nil {
			run.executions = make(map[string]*workloadExecution)
		}
		run.executions[request.GetExecutionId()] = &workloadExecution{id: request.GetExecutionId(), authority: authority, owner: owner, instanceID: run.instanceID, kind: request.GetKind(), state: hostv1.ExecutionState_EXECUTION_STATE_ACTIVE, acquiredAt: now, deadline: deadline}
		run.idleSince = time.Time{}
		acquired = true
	}
	return &hostv1.AcquireExecutionResponse{ExecutionId: request.GetExecutionId(), ContainerInstanceId: run.instanceID, Acquired: acquired}, nil
}

func (controller *Owner) ReleaseExecution(ctx context.Context, request *hostv1.ReleaseExecutionRequest) (*hostv1.ReleaseExecutionResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	unlock, lockErr := controller.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	run, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if request.GetHolderId() != "" {
		return nil, hostv1.InvalidField("holder_id", "execution holders are not supported")
	}
	if request.GetExecutionId() == "" {
		return nil, hostv1.InvalidField("execution_id", "activity identity is required")
	}
	if execution := run.executions[request.GetExecutionId()]; execution != nil {
		if execution.authority != executionAuthority(identity) || execution.owner != executionOwner(identity) || execution.instanceID != run.instanceID {
			return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_EXECUTION, "execution completion is stale", hostv1.WithResource("container", run.containerID))
		}
		delete(run.executions, request.GetExecutionId())
		if len(run.executions) == 0 {
			run.idleSince = time.Now()
		}
	}
	return &hostv1.ReleaseExecutionResponse{}, nil
}

func (controller *Owner) ListExecutions(ctx context.Context, request *hostv1.ListExecutionsRequest) (*hostv1.ListExecutionsResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetContainerId() == "" || request.GetExpectedInstanceId() == "" {
		return nil, hostv1.InvalidField("container/instance", "exact container instance is required")
	}
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	if !controller.workloadRunsRecovered {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED, "workload recovery is incomplete", hostv1.WithRetry(time.Second))
	}
	key := workloadRunKey(identity.EnvironmentID, request.GetContainerId())
	run := controller.workloadRuns[key]
	if run == nil {
		status := hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_NOT_FOUND
		if controller.stoppedWorkloadInstances[key] == request.GetExpectedInstanceId() {
			status = hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STOPPED
		}
		return &hostv1.ListExecutionsResponse{Status: status, AllocationAuthority: executionAuthority(identity)}, nil
	}
	if run.instanceID != request.GetExpectedInstanceId() {
		return &hostv1.ListExecutionsResponse{Status: hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_STALE, ContainerInstanceId: run.instanceID, AllocationAuthority: executionAuthority(identity)}, nil
	}
	if run.stopping {
		return &hostv1.ListExecutionsResponse{Status: hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_RECOVERING, ContainerInstanceId: run.instanceID, AllocationAuthority: executionAuthority(identity)}, nil
	}
	markExpiredExecutionsUnknownLocked(run, time.Now())
	if len(run.executions) == 0 {
		return &hostv1.ListExecutionsResponse{Status: hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_EMPTY, ContainerInstanceId: run.instanceID, AllocationAuthority: executionAuthority(identity)}, nil
	}
	ids := make([]string, 0, len(run.executions))
	for id := range run.executions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]*hostv1.Execution, 0, len(ids))
	for _, id := range ids {
		execution := run.executions[id]
		item := &hostv1.Execution{ExecutionId: execution.id, ContainerInstanceId: execution.instanceID, Kind: execution.kind, State: execution.state, AcquiredUnixNano: execution.acquiredAt.UnixNano(), Owner: execution.owner}
		if !execution.deadline.IsZero() {
			item.DeadlineUnixNano = execution.deadline.UnixNano()
		}
		result = append(result, item)
	}
	return &hostv1.ListExecutionsResponse{Status: hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_PRESENT, ContainerInstanceId: run.instanceID, Executions: result, AllocationAuthority: executionAuthority(identity)}, nil
}

// The Server asks its Runtime to quiesce only after this closes Host admission.
// Cancellation names the same command and instance; it cannot reopen a later stop.
func (controller *Owner) QuiesceContainer(ctx context.Context, request *hostv1.QuiesceContainerRequest) (*hostv1.QuiesceContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "quiesce command identity is required")
	}
	if request.GetMinimumIdleSeconds() > 86_400 {
		return nil, hostv1.InvalidField("minimum_idle_seconds", "minimum idle period must not exceed one day")
	}
	unlock, lockErr := controller.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	run, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if request.GetCancel() {
		if run.stopping || (run.quiescing && run.quiesceRequestID != request.GetRequestId()) {
			return nil, status.Error(codes.Aborted, "quiesce command is no longer current")
		}
		if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
			controller.clearWorkloadReclaimLocked("consumer_cancelled")
		}
		if run.quiescing {
			run.quiescing = false
			run.quiesceRequestID = ""
			run.stopDeadline = time.Time{}
		}
	} else {
		if len(run.executions) != 0 {
			return nil, hostv1.NewError(codes.ResourceExhausted, hostv1.ErrorReason_ERROR_REASON_WORKLOAD_EXECUTION_BUSY, "workload still has an active execution", hostv1.WithResource("container", run.containerID))
		}
		minimumIdle := time.Duration(request.GetMinimumIdleSeconds()) * time.Second
		if run.idleSince.IsZero() || time.Since(run.idleSince) < minimumIdle {
			remaining := time.Until(run.idleSince.Add(minimumIdle))
			if run.idleSince.IsZero() || remaining < time.Second {
				remaining = time.Second
			}
			return nil, status.Error(codes.ResourceExhausted, "workload has not completed its minimum idle period")
		}
		if request.GetMinimumIdleSeconds() > 0 {
			if err := controller.validateWorkloadReclaimLocked(run, time.Now()); err != nil {
				return nil, err
			}
		}
		if run.starting || run.stopping || run.inFlight || run.quiescing && run.quiesceRequestID != request.GetRequestId() {
			return nil, pendingWorkloadError(run, time.Now())
		}
		if run.quiescing && !time.Now().Before(run.stopDeadline) {
			return nil, pendingWorkloadError(run, time.Now())
		}
		if !run.quiescing {
			if request.GetMinimumIdleSeconds() == 0 {
				if candidate := controller.workloadReclaim; candidate != nil && candidate.run == run {
					controller.clearWorkloadReclaimLocked("explicit_repair")
				}
			}
			run.quiescing = true
			run.quiesceRequestID = request.GetRequestId()
			run.stopDeadline = time.Now().Add(workloadStopBudget)
			if candidate := controller.workloadReclaim; candidate != nil && candidate.run == run {
				if deadline := time.UnixMilli(candidate.value.GetExpiresUnixMillis()); deadline.Before(run.stopDeadline) {
					run.stopDeadline = deadline
				}
			}
		}
	}
	return &hostv1.QuiesceContainerResponse{Container: &hostv1.Container{Id: run.containerID, InstanceId: run.instanceID, State: hostv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}

func (controller *Owner) ValidateExplicitStop(identity auth.Identity, request *hostv1.StopContainerRequest) error {
	if request.GetQuiesceRequestId() != "" {
		return controller.ValidateQuiescedStop(identity, request)
	}
	if request.GetExpectedInstanceId() == "" {
		return nil
	}
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	_, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	return err
}

func (controller *Owner) ValidateQuiescedStop(identity auth.Identity, request *hostv1.StopContainerRequest) error {
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	run, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return err
	}
	if !run.quiescing || run.quiesceRequestID != request.GetQuiesceRequestId() || len(run.executions) != 0 {
		return status.Error(codes.Aborted, "safe stop does not own current execution admission")
	}
	if !time.Now().Before(run.stopDeadline) {
		return pendingWorkloadError(run, time.Now())
	}
	if candidate := controller.workloadReclaim; candidate != nil && candidate.run == run {
		return controller.validateWorkloadReclaimLocked(run, time.Now())
	}
	return nil
}

func (controller *Owner) validateWorkloadReclaimLocked(run *Run, now time.Time) error {
	candidate := controller.workloadReclaim
	if candidate == nil || candidate.run != run || candidate.value.GetContainerInstanceId() != run.instanceID ||
		now.UnixMilli() >= candidate.value.GetExpiresUnixMillis() || !controller.workloadMemoryPressure ||
		now.Sub(controller.workloadPressureAt) > 2*workloadSampleInterval || run.usage == nil || run.usage.GetUnavailable() ||
		!workloadUsageFresh(run.usage, now) {
		return status.Error(codes.Aborted, "current pressure reclaim candidate and fresh instance observation are required")
	}
	return nil
}

func (controller *Owner) ReleaseObservedExecution(identity auth.Identity, containerID, instanceID, executionID string) {
	controller.ReleaseStreamExecution(&ExecutionToken{environmentID: identity.EnvironmentID, containerID: containerID, instanceID: instanceID, executionID: executionID, authority: executionAuthority(identity), owner: executionOwner(identity)})
}
