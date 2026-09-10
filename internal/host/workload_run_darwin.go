//go:build darwin

package host

import (
	"context"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc/codes"
)

const (
	maxActiveWorkloads     = 4
	workloadStartBudget    = 120 * time.Second
	workloadStopBudget     = 30 * time.Second
	workloadSampleInterval = 5 * time.Second
)

// Controller.mu owns admission; the environment lock serializes its commands.
// Unknown starts retain their slot through the original RPC deadline, even if
// an observation reaches the Guest before the cancelled start request does.
type workloadRun struct {
	environmentID       string
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
	execution           *workloadExecution
	idleSince           time.Time
	quiescing           bool
	quiesceRequestID    string
}

func workloadRunKey(environmentID, containerID string) string {
	return environmentID + "\x00" + containerID
}

func workloadCapacityError(dimension resourcev2.CapacityDimension) error {
	return resourceErrorWithMetadata(codes.ResourceExhausted, "RESOURCE_CAPACITY_EXHAUSTED", "workload_pool", string(dimension),
		"workload is waiting for node capacity", map[string]string{"resource_dimension": string(dimension)})
}

func (controller *Controller) admitWorkloadStart(environmentID, namespaceID, containerID string, now time.Time) (*workloadRun, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !controller.workloadRunsRecovered {
		return nil, retryable("VM_ADMISSION_CLOSED", "workload recovery is incomplete", time.Second)
	}
	key := workloadRunKey(environmentID, containerID)
	if current := controller.workloadRuns[key]; current != nil {
		return nil, pendingWorkloadError(current, now)
	}
	if len(controller.workloadRuns) >= maxActiveWorkloads {
		return nil, workloadCapacityError(resourcev2.CapacityRunSlot)
	}
	for _, run := range controller.workloadRuns {
		if run.starting {
			return nil, workloadCapacityError(resourcev2.CapacityRunSlot)
		}
	}
	if now.Sub(controller.workloadPressureAt) > 2*workloadSampleInterval || controller.workloadMemoryPressure {
		return nil, workloadCapacityError(resourcev2.CapacityMemoryPressure)
	}
	run := &workloadRun{environmentID: environmentID, namespaceID: namespaceID, containerID: containerID, starting: true, inFlight: true}
	controller.workloadRuns[key] = run
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
			run.idleSince = now
		}
		run.instanceID = container.GetInstanceId()
		run.starting = false
		run.uncertainStartUntil = time.Time{}
	case vmmdv1.ContainerState_CONTAINER_STATE_CREATED, vmmdv1.ContainerState_CONTAINER_STATE_STOPPED:
		if now.Before(run.uncertainStartUntil) {
			return
		}
		delete(controller.workloadRuns, key)
	}
}

func (controller *Controller) startWorkloadContainer(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StartContainerRequest) (*vmmdv1.StartContainerResponse, error) {
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
	run, err := controller.admitWorkloadStart(environmentID, request.GetActorNamespaceId(), request.GetContainerId(), time.Now())
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
	return workloadCapacityError(resourcev2.CapacityRunSlot)
}
