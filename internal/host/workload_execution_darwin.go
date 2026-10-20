//go:build darwin

package host

import (
	"context"
	"fmt"
	"sort"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
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

type workloadExecutionToken struct {
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

func markExpiredExecutionsUnknownLocked(run *workloadRun, now time.Time) {
	for _, execution := range run.executions {
		if execution.state == hostv1.ExecutionState_EXECUTION_STATE_ACTIVE && !execution.deadline.IsZero() && !now.Before(execution.deadline) {
			execution.state = hostv1.ExecutionState_EXECUTION_STATE_UNKNOWN
		}
	}
}

func executionDeadline(request *hostv1.ExecutionRequest, now time.Time) (time.Time, error) {
	if request.GetKind() == hostv1.ExecutionKind_EXECUTION_KIND_MAIN_EXECUTION {
		if request.GetDeadlineUnixNano() != 0 {
			return time.Time{}, invalidField("deadline_unix_nano", "main execution deadline must be zero")
		}
		return time.Time{}, nil
	}
	if request.GetDeadlineUnixNano() == 0 {
		return time.Time{}, invalidField("deadline_unix_nano", "operation deadline is required")
	}
	deadline := time.Unix(0, request.GetDeadlineUnixNano())
	if !now.Before(deadline) {
		return time.Time{}, resourceError(codes.DeadlineExceeded, "EXECUTION_DEADLINE_EXCEEDED", "execution", "current", "execution deadline has expired")
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

func (controller *Controller) acquireStreamExecution(identity auth.Identity, containerID, instanceID, executionID string, kind hostv1.ExecutionKind, deadlineUnixNano int64) (*workloadExecutionToken, error) {
	if executionID == "" || len(executionID) > 256 {
		return nil, invalidField("execution_id", "activity identity is required")
	}
	if kind != hostv1.ExecutionKind_EXECUTION_KIND_EXEC && kind != hostv1.ExecutionKind_EXECUTION_KIND_PROCESS && kind != hostv1.ExecutionKind_EXECUTION_KIND_BUILD {
		return nil, invalidField("execution_kind", "stream execution kind is required")
	}
	now := time.Now()
	deadline, err := executionDeadline(&hostv1.ExecutionRequest{Kind: kind, DeadlineUnixNano: deadlineUnixNano}, now)
	if err != nil {
		return nil, err
	}
	unlock := controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
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
			return nil, resourceError(codes.Aborted, "STALE_EXECUTION", "container", run.containerID, "activity identity belongs to another owner or target")
		}
		return nil, resourceError(codes.Aborted, "EXECUTION_OUTCOME_UNKNOWN", "execution", executionID, "activity was already accepted; observe its terminal result")
	}
	if len(run.executions) >= maxWorkloadExecutions {
		return nil, resourceError(codes.ResourceExhausted, "WORKLOAD_EXECUTIONS_FULL", "container", run.containerID, "workload activity limit reached")
	}
	if run.executions == nil {
		run.executions = make(map[string]*workloadExecution)
	}
	run.executions[executionID] = &workloadExecution{id: executionID, authority: authority, owner: owner, instanceID: run.instanceID, kind: kind, state: hostv1.ExecutionState_EXECUTION_STATE_ACTIVE, acquiredAt: now, deadline: deadline}
	run.idleSince = time.Time{}
	return &workloadExecutionToken{environmentID: identity.EnvironmentID, containerID: run.containerID, instanceID: run.instanceID, executionID: executionID, authority: authority, owner: owner}, nil
}

// releaseStreamExecution consumes only terminal evidence from the Host-owned
// guest stream. It intentionally does not require the control lease/session to
// remain current after the operation was accepted.
func (controller *Controller) releaseStreamExecution(token *workloadExecutionToken) {
	if token == nil {
		return
	}
	unlock := controller.lockEnvironment(token.environmentID)
	defer unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
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

func (controller *Controller) currentWorkloadRunLocked(identity auth.Identity, containerID, instanceID string) (*workloadRun, error) {
	if containerID == "" || instanceID == "" {
		return nil, invalidField("container/instance", "current container instance is required")
	}
	if !controller.workloadRunsRecovered {
		return nil, retryable("VM_ADMISSION_CLOSED", "workload recovery is incomplete", time.Second)
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
		return nil, invalidField("execution_id", "activity identity is required")
	}
	if request.GetHolderId() != "" {
		return nil, invalidField("holder_id", "execution holders are not supported")
	}
	if !validExecutionKind(request.GetKind()) {
		return nil, invalidField("kind", "execution kind is required")
	}
	now := time.Now()
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	server.controller.mu.Lock()
	defer server.controller.mu.Unlock()
	run, err := server.controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return nil, err
	}
	if server.controller.workloadReclaim != nil && server.controller.workloadReclaim.run == run && !run.stopping && !run.quiescing {
		server.controller.clearWorkloadReclaimLocked("demand")
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
			return nil, resourceError(codes.Aborted, "STALE_EXECUTION", "container", run.containerID, "activity identity belongs to another owner or target")
		}
	} else {
		deadline, err := executionDeadline(request, now)
		if err != nil {
			return nil, err
		}
		if len(run.executions) >= maxWorkloadExecutions {
			return nil, resourceError(codes.ResourceExhausted, "WORKLOAD_EXECUTIONS_FULL", "container", run.containerID, "workload activity limit reached")
		}
		if run.executions == nil {
			run.executions = make(map[string]*workloadExecution)
		}
		run.executions[request.GetExecutionId()] = &workloadExecution{id: request.GetExecutionId(), authority: authority, owner: owner, instanceID: run.instanceID, kind: request.GetKind(), state: hostv1.ExecutionState_EXECUTION_STATE_ACTIVE, acquiredAt: now, deadline: deadline}
		run.idleSince = time.Time{}
		acquired = true
	}
	return &hostv1.ExecutionResponse{ExecutionId: request.GetExecutionId(), ContainerInstanceId: run.instanceID, Acquired: acquired}, nil
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
	if request.GetHolderId() != "" {
		return nil, invalidField("holder_id", "execution holders are not supported")
	}
	if request.GetExecutionId() == "" {
		return nil, invalidField("execution_id", "activity identity is required")
	}
	if execution := run.executions[request.GetExecutionId()]; execution != nil {
		if execution.authority != executionAuthority(identity) || execution.owner != executionOwner(identity) || execution.instanceID != run.instanceID {
			return nil, resourceError(codes.Aborted, "STALE_EXECUTION", "container", run.containerID, "execution completion is stale")
		}
		delete(run.executions, request.GetExecutionId())
		if len(run.executions) == 0 {
			run.idleSince = time.Now()
		}
	}
	return &emptypb.Empty{}, nil
}

func (server *agentServer) ListExecutions(ctx context.Context, request *hostv1.ListExecutionsRequest) (*hostv1.ListExecutionsResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetContainerId() == "" || request.GetExpectedInstanceId() == "" {
		return nil, invalidField("container/instance", "exact container instance is required")
	}
	server.controller.mu.Lock()
	defer server.controller.mu.Unlock()
	if !server.controller.workloadRunsRecovered {
		return nil, retryable("VM_ADMISSION_CLOSED", "workload recovery is incomplete", time.Second)
	}
	key := workloadRunKey(identity.EnvironmentID, request.GetContainerId())
	run := server.controller.workloadRuns[key]
	if run == nil {
		status := hostv1.ExecutionListStatus_EXECUTION_LIST_STATUS_NOT_FOUND
		if server.controller.stoppedWorkloadInstances[key] == request.GetExpectedInstanceId() {
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
		if server.controller.workloadReclaim != nil && server.controller.workloadReclaim.run == run {
			server.controller.clearWorkloadReclaimLocked("consumer_cancelled")
		}
		if run.quiescing {
			run.quiescing = false
			run.quiesceRequestID = ""
			run.stopDeadline = time.Time{}
		}
	} else {
		if len(run.executions) != 0 {
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
		if request.GetMinimumIdleSeconds() > 0 {
			if err := server.controller.validateWorkloadReclaimLocked(run, time.Now()); err != nil {
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
				if candidate := server.controller.workloadReclaim; candidate != nil && candidate.run == run {
					server.controller.clearWorkloadReclaimLocked("explicit_repair")
				}
			}
			run.quiescing = true
			run.quiesceRequestID = request.GetRequestId()
			run.stopDeadline = time.Now().Add(workloadStopBudget)
			if candidate := server.controller.workloadReclaim; candidate != nil && candidate.run == run {
				if deadline := time.UnixMilli(candidate.value.GetExpiresUnixMillis()); deadline.Before(run.stopDeadline) {
					run.stopDeadline = deadline
				}
			}
		}
	}
	return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: run.containerID, InstanceId: run.instanceID, State: hostv1.ContainerState_CONTAINER_STATE_RUNNING}}, nil
}

func (controller *Controller) validateExplicitStop(identity auth.Identity, request *hostv1.StopContainerRequest) error {
	if request.GetQuiesceRequestId() != "" {
		return controller.validateQuiescedStop(identity, request)
	}
	if request.GetExpectedInstanceId() == "" {
		return nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	_, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	return err
}

func (controller *Controller) validateQuiescedStop(identity auth.Identity, request *hostv1.StopContainerRequest) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	run, err := controller.currentWorkloadRunLocked(identity, request.GetContainerId(), request.GetExpectedInstanceId())
	if err != nil {
		return err
	}
	if !run.quiescing || run.quiesceRequestID != request.GetQuiesceRequestId() || len(run.executions) != 0 {
		return resourceError(codes.Aborted, "STALE_QUIESCE", "container", run.containerID, "safe stop does not own current execution admission")
	}
	if !time.Now().Before(run.stopDeadline) {
		return pendingWorkloadError(run, time.Now())
	}
	if candidate := controller.workloadReclaim; candidate != nil && candidate.run == run {
		return controller.validateWorkloadReclaimLocked(run, time.Now())
	}
	return nil
}

func (controller *Controller) validateWorkloadReclaimLocked(run *workloadRun, now time.Time) error {
	candidate := controller.workloadReclaim
	if candidate == nil || candidate.run != run || candidate.value.GetContainerInstanceId() != run.instanceID ||
		now.UnixMilli() >= candidate.value.GetExpiresUnixMillis() || !controller.workloadMemoryPressure ||
		now.Sub(controller.workloadPressureAt) > 2*workloadSampleInterval || run.usage == nil || run.usage.GetUnavailable() ||
		!workloadUsageFresh(run.usage, now) {
		return resourceError(codes.Aborted, "RECLAIM_CANDIDATE_EXPIRED", "container", run.containerID, "current pressure reclaim candidate and fresh instance observation are required")
	}
	return nil
}
