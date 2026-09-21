//go:build darwin

package host

import (
	"context"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc/codes"
)

const (
	workloadStartBudget    = 120 * time.Second
	workloadStopBudget     = 30 * time.Second
	workloadSampleInterval = 5 * time.Second
	// Stopped-instance evidence is only a bounded aid for owners that still
	// have an execution to settle. Omitted evidence is reported as not_found,
	// never as proof that an instance stopped.
	maxStoppedWorkloadInstances = 256
)

// The environment lock serializes lifecycle commands. An unknown start retains
// its fence until the original deadline so an early observation cannot reopen it.
type workloadRun struct {
	environmentID       string
	registrationID      string
	namespaceID         string
	containerID         string
	instanceID          string
	starting            bool
	stopping            bool
	inFlight            bool
	uncertainStartUntil time.Time
	stopDeadline        time.Time
	stopRequestID       string
	stopAttempt         uint32
	usage               *vmmdv1.Usage
	executions          map[string]*workloadExecution
	idleSince           time.Time
	quiescing           bool
	quiesceRequestID    string
}

func workloadRunKey(environmentID, containerID string) string {
	return environmentID + "\x00" + containerID
}

func (controller *Controller) resetWorkloadRunsLocked(runs map[string]*workloadRun) {
	controller.workloadRuns = runs
	controller.workloadRunOrder = controller.workloadRunOrder[:0]
	controller.workloadRunIndex = make(map[string]int, len(runs))
	for key := range runs {
		controller.workloadRunIndex[key] = len(controller.workloadRunOrder)
		controller.workloadRunOrder = append(controller.workloadRunOrder, key)
	}
	controller.workloadSampleOffset = 0
}

func (controller *Controller) setWorkloadRunLocked(key string, run *workloadRun) {
	if controller.workloadRuns == nil {
		controller.workloadRuns = make(map[string]*workloadRun)
	}
	if controller.workloadRunIndex == nil {
		controller.workloadRunIndex = make(map[string]int)
	}
	if _, exists := controller.workloadRuns[key]; !exists {
		controller.workloadRunIndex[key] = len(controller.workloadRunOrder)
		controller.workloadRunOrder = append(controller.workloadRunOrder, key)
	}
	controller.workloadRuns[key] = run
}

func (controller *Controller) deleteWorkloadRunLocked(key string) {
	if _, exists := controller.workloadRuns[key]; !exists {
		return
	}
	delete(controller.workloadRuns, key)
	index, indexed := controller.workloadRunIndex[key]
	if !indexed || index >= len(controller.workloadRunOrder) {
		controller.resetWorkloadRunsLocked(controller.workloadRuns)
		return
	}
	last := len(controller.workloadRunOrder) - 1
	lastKey := controller.workloadRunOrder[last]
	controller.workloadRunOrder[index] = lastKey
	controller.workloadRunOrder = controller.workloadRunOrder[:last]
	delete(controller.workloadRunIndex, key)
	if index < last {
		controller.workloadRunIndex[lastKey] = index
	}
	if len(controller.workloadRunOrder) == 0 {
		controller.workloadSampleOffset = 0
	} else if controller.workloadSampleOffset >= len(controller.workloadRunOrder) {
		controller.workloadSampleOffset %= len(controller.workloadRunOrder)
	}
}

func (controller *Controller) workloadRunPageLocked(limit int, now time.Time) []*workloadRun {
	if len(controller.workloadRunOrder) != len(controller.workloadRuns) {
		controller.resetWorkloadRunsLocked(controller.workloadRuns)
	}
	count := len(controller.workloadRunOrder)
	if count == 0 {
		return nil
	}
	runs := make([]*workloadRun, 0, min(limit, count))
	for scanned := 0; scanned < count && scanned < limit; scanned++ {
		if controller.workloadSampleOffset >= len(controller.workloadRunOrder) {
			controller.workloadSampleOffset = 0
		}
		key := controller.workloadRunOrder[controller.workloadSampleOffset]
		controller.workloadSampleOffset++
		run := controller.workloadRuns[key]
		if run == nil {
			continue
		}
		markExpiredExecutionsUnknownLocked(run, now)
		if !run.inFlight && (controller.workloadReclaim == nil || controller.workloadReclaim.run != run) {
			runs = append(runs, run)
		}
	}
	return runs
}

func (controller *Controller) admitWorkloadStart(environmentID, namespaceID, containerID string, now time.Time, registrationID ...string) (*workloadRun, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !controller.workloadRunsRecovered {
		return nil, retryable("VM_ADMISSION_CLOSED", "workload recovery is incomplete", time.Second)
	}
	key := workloadRunKey(environmentID, containerID)
	if current := controller.workloadRuns[key]; current != nil {
		return nil, pendingWorkloadError(current, now)
	}
	owner := ""
	if len(registrationID) != 0 {
		owner = registrationID[0]
	}
	run := &workloadRun{environmentID: environmentID, registrationID: owner, namespaceID: namespaceID, containerID: containerID, starting: true, inFlight: true}
	controller.setWorkloadRunLocked(key, run)
	delete(controller.stoppedWorkloadInstances, key)
	return run, nil
}

func (controller *Controller) observeWorkloadRunLocked(run *workloadRun, container *vmmdv1.Container, now time.Time) {
	if container == nil || run.inFlight {
		return
	}
	key := workloadRunKey(run.environmentID, run.containerID)
	if controller.workloadRuns[key] != run {
		return
	}
	switch container.GetState() {
	case vmmdv1.ContainerState_CONTAINER_STATE_RUNNING:
		if run.instanceID != container.GetInstanceId() {
			if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
				controller.clearWorkloadReclaimLocked("instance_changed")
			}
			run.idleSince = now
		}
		run.instanceID = container.GetInstanceId()
		run.starting = false
		run.uncertainStartUntil = time.Time{}
	case vmmdv1.ContainerState_CONTAINER_STATE_CREATED, vmmdv1.ContainerState_CONTAINER_STATE_STOPPED:
		if now.Before(run.uncertainStartUntil) {
			return
		}
		if run.instanceID != "" && container.GetInstanceId() != run.instanceID {
			return
		}
		if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
			controller.clearWorkloadReclaimLocked("instance_stopped")
		}
		if run.instanceID != "" && len(run.executions) != 0 {
			if controller.stoppedWorkloadInstances == nil {
				controller.stoppedWorkloadInstances = make(map[string]string)
			}
			if _, present := controller.stoppedWorkloadInstances[key]; present || len(controller.stoppedWorkloadInstances) < maxStoppedWorkloadInstances {
				controller.stoppedWorkloadInstances[key] = run.instanceID
			}
		}
		controller.deleteWorkloadRunLocked(key)
	}
}

func (controller *Controller) startWorkloadContainer(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StartContainerRequest, registrationID ...string) (*vmmdv1.StartContainerResponse, error) {
	unlock := controller.lockEnvironment(environmentID)
	defer unlock()
	ctx, cancel := context.WithTimeout(ctx, workloadStartBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: request.GetActorNamespaceId(), ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	controller.mu.Lock()
	current := controller.workloadRuns[workloadRunKey(environmentID, request.GetContainerId())]
	if current != nil && !current.starting && !current.stopping && !current.quiescing && !current.inFlight && observed.GetContainer().GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING && current.instanceID == observed.GetContainer().GetInstanceId() {
		controller.mu.Unlock()
		return &vmmdv1.StartContainerResponse{Container: observed.GetContainer()}, nil
	}
	if current != nil {
		controller.observeWorkloadRunLocked(current, observed.GetContainer(), time.Now())
	}
	controller.mu.Unlock()
	run, err := controller.admitWorkloadStart(environmentID, request.GetActorNamespaceId(), request.GetContainerId(), time.Now(), registrationID...)
	if err != nil {
		return nil, err
	}
	response, err := client.StartContainer(ctx, request)
	controller.mu.Lock()
	run.inFlight = false
	if err != nil && run.starting {
		run.uncertainStartUntil = deadline
	}
	if err == nil {
		controller.observeWorkloadRunLocked(run, response.GetContainer(), time.Now())
	}
	controller.mu.Unlock()
	return response, err
}

// Caller holds the environment lock, including lease revocation and cleanup.
func (controller *Controller) stopWorkloadContainerLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, workloadStopBudget)
	defer cancel()
	controller.mu.Lock()
	run := controller.workloadRuns[workloadRunKey(environmentID, request.GetContainerId())]
	if run != nil {
		run.stopping = true
		run.inFlight = true
		if run.stopDeadline.IsZero() {
			run.stopDeadline = time.Now().Add(workloadStopBudget)
			run.stopRequestID = request.GetRequestId()
		}
	}
	controller.mu.Unlock()
	response, err := client.StopContainer(ctx, request)
	controller.mu.Lock()
	if run != nil {
		run.inFlight = false
		if err == nil {
			controller.observeWorkloadRunLocked(run, response.GetContainer(), time.Now())
		}
	}
	controller.mu.Unlock()
	return response, err
}

func pendingWorkloadError(run *workloadRun, now time.Time) error {
	if (run.stopping || run.quiescing) && !run.stopDeadline.IsZero() && !now.Before(run.stopDeadline) || run.starting && !run.uncertainStartUntil.IsZero() && !now.Before(run.uncertainStartUntil) {
		return resourceError(codes.FailedPrecondition, "WORKLOAD_STOP_UNRESOLVED", "container", run.containerID, "sandbox state remains unknown after the recovery budget; inspect or stop the existing instance before retrying")
	}
	return resourceError(codes.FailedPrecondition, "LIFECYCLE_CONFLICT", "container", run.containerID, "workload lifecycle operation is still in progress")
}
