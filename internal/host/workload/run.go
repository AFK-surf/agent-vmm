//go:build darwin || linux

package workload

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	StartBudget            = 120 * time.Second
	workloadStopBudget     = 30 * time.Second
	workloadSampleInterval = 5 * time.Second
	// Stopped-instance evidence is only a bounded aid for owners that still
	// have an execution to settle. Omitted evidence is reported as not_found,
	// never as proof that an instance stopped.
	maxStoppedWorkloadInstances = 256
)

// The environment lock serializes lifecycle commands. An unknown start retains
// its fence until the original deadline so an early observation cannot reopen it.
type Run struct {
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
	stopTimeoutSeconds  *uint32
	usage               *vmmdv1.Usage
	executions          map[string]*workloadExecution
	idleSince           time.Time
	quiescing           bool
	quiesceRequestID    string
}

func workloadRunKey(environmentID, containerID string) string {
	return environmentID + "\x00" + containerID
}

func (controller *Owner) resetWorkloadRunsLocked(runs map[string]*Run) {
	controller.workloadRuns = runs
	controller.workloadRunOrder = controller.workloadRunOrder[:0]
	controller.workloadRunIndex = make(map[string]int, len(runs))
	for key := range runs {
		controller.workloadRunIndex[key] = len(controller.workloadRunOrder)
		controller.workloadRunOrder = append(controller.workloadRunOrder, key)
	}
	controller.workloadSampleOffset = 0
}

func (controller *Owner) setWorkloadRunLocked(key string, run *Run) {
	if controller.workloadRuns == nil {
		controller.workloadRuns = make(map[string]*Run)
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

func (controller *Owner) deleteWorkloadRunLocked(key string) {
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

func (controller *Owner) workloadRunPageLocked(limit int, now time.Time) []*Run {
	if len(controller.workloadRunOrder) != len(controller.workloadRuns) {
		controller.resetWorkloadRunsLocked(controller.workloadRuns)
	}
	count := len(controller.workloadRunOrder)
	if count == 0 {
		return nil
	}
	runs := make([]*Run, 0, min(limit, count))
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

func (controller *Owner) admitWorkloadStart(environmentID, namespaceID, containerID string, now time.Time, registrationID ...string) (*Run, error) {
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	if !controller.workloadRunsRecovered {
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED, "workload recovery is incomplete", hostv1.WithRetry(time.Second))
	}
	key := workloadRunKey(environmentID, containerID)
	if current := controller.workloadRuns[key]; current != nil {
		return nil, pendingWorkloadError(current, now)
	}
	owner := ""
	if len(registrationID) != 0 {
		owner = registrationID[0]
	}
	run := &Run{environmentID: environmentID, registrationID: owner, namespaceID: namespaceID, containerID: containerID, starting: true, inFlight: true}
	controller.setWorkloadRunLocked(key, run)
	delete(controller.stoppedWorkloadInstances, key)
	return run, nil
}

func (controller *Owner) observeWorkloadRunLocked(run *Run, container *vmmdv1.Container, now time.Time) {
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
		controller.settleStoppedWorkloadRunLocked(run)
	}
}

func (controller *Owner) settleStoppedWorkloadRunLocked(run *Run) {
	key := workloadRunKey(run.environmentID, run.containerID)
	if controller.workloadRuns[key] != run {
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

func (controller *Owner) StartWorkloadContainer(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StartContainerRequest, registrationID ...string) (*vmmdv1.StartContainerResponse, error) {
	unlock, lockErr := controller.LockEnvironment(ctx, environmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	return controller.StartWorkloadContainerLocked(ctx, client, environmentID, request, registrationID...)
}

func (controller *Owner) StartWorkloadContainerLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StartContainerRequest, registrationID ...string) (*vmmdv1.StartContainerResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, StartBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	run, observed, err := controller.PrepareWorkloadStartLocked(ctx, client, environmentID, request, registrationID...)
	if err != nil {
		return nil, err
	}
	if err := controller.store.SetManagedContainerRunning(ctx, environmentID, request.GetContainerId(), observed.GetGenerationId(), true); err != nil {
		controller.ReleaseUnsentWorkloadStart(run)
		return nil, err
	}
	return controller.SendWorkloadStartLocked(ctx, client, environmentID, request, run, observed, deadline)
}

func (controller *Owner) PrepareWorkloadStartLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StartContainerRequest, registrationID ...string) (*Run, *vmmdv1.Container, error) {
	observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: request.GetActorNamespaceId(), ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, nil, err
	}
	container := observed.GetContainer()
	if request.GetExpectedGenerationId() != "" && request.GetExpectedGenerationId() != container.GetGenerationId() {
		return nil, nil, status.Error(codes.FailedPrecondition, "container generation changed")
	}
	if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
		return nil, nil, status.Error(codes.FailedPrecondition, "previous sandbox has not stopped; stop and reconcile it before restarting")
	}
	if err := controller.hostShares.AuthorizeHostShareMounts(ctx, environmentID, container.GetVolumes()); err != nil {
		return nil, nil, err
	}
	controller.mu.Lock()
	current := controller.workloadRuns[workloadRunKey(environmentID, request.GetContainerId())]
	if current != nil && !current.starting && !current.stopping && !current.quiescing && !current.inFlight && container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING && current.instanceID == container.GetInstanceId() {
		controller.unlockWorkloadChanged()
		return nil, container, nil
	}
	if current != nil {
		controller.observeWorkloadRunLocked(current, container, time.Now())
	}
	controller.unlockWorkloadChanged()
	run, err := controller.admitWorkloadStart(environmentID, request.GetActorNamespaceId(), request.GetContainerId(), time.Now(), registrationID...)
	return run, container, err
}

func (controller *Owner) ReleaseUnsentWorkloadStart(run *Run) {
	if run == nil {
		return
	}
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	key := workloadRunKey(run.environmentID, run.containerID)
	if controller.workloadRuns[key] == run {
		controller.deleteWorkloadRunLocked(key)
	}
}

func (controller *Owner) SendWorkloadStartLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StartContainerRequest, run *Run, observed *vmmdv1.Container, deadline time.Time) (*vmmdv1.StartContainerResponse, error) {
	if run == nil {
		if err := controller.projectionMu.Lock(ctx); err != nil {
			return nil, err
		}

		err := controller.services.ProjectServiceContainerLocked(ctx, client, environmentID, request.GetContainerId(), observed.GetGenerationId(), observed.GetInstanceId())
		controller.projectionMu.Unlock()
		return &vmmdv1.StartContainerResponse{Container: observed}, err
	}
	response, err := client.StartContainer(ctx, request)
	if guestconn.MutationAdmissionRejected(err) {
		completion, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		clearErr := controller.store.SetManagedContainerRunning(completion, environmentID, request.GetContainerId(), observed.GetGenerationId(), false)
		controller.ReleaseUnsentWorkloadStart(run)
		return nil, errors.Join(err, clearErr)
	}
	controller.mu.Lock()
	run.inFlight = false
	if err != nil && run.starting {
		run.uncertainStartUntil = deadline
	}
	if err == nil {
		controller.observeWorkloadRunLocked(run, response.GetContainer(), time.Now())
	}
	controller.unlockWorkloadChanged()
	if err == nil {
		if err := controller.projectionMu.Lock(ctx); err != nil {
			return nil, err
		}

		container := response.GetContainer()
		err = controller.services.ProjectServiceContainerLocked(ctx, client, environmentID, request.GetContainerId(), container.GetGenerationId(), container.GetInstanceId())
		controller.projectionMu.Unlock()
	}
	return response, err
}

func (controller *Owner) AcceptWorkloadStopLocked(ctx context.Context, environmentID string, request *vmmdv1.StopContainerRequest) error {
	return controller.acceptWorkloadStopIntentLocked(ctx, environmentID, request, true)
}

func (controller *Owner) acceptWorkloadStopIntentLocked(ctx context.Context, environmentID string, request *vmmdv1.StopContainerRequest, explicit bool) error {
	if explicit {
		managed, err := controller.store.GetManagedContainer(ctx, environmentID, request.GetContainerId())
		if err == nil {
			if err := controller.store.SetManagedContainerRunning(ctx, environmentID, request.GetContainerId(), managed.ContainerGenerationID, false); err != nil {
				return err
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			if err := controller.store.SetManagedContainerRunning(ctx, environmentID, request.GetContainerId(), "", false); err != nil {
				return err
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	controller.mu.Lock()
	run := controller.workloadRuns[workloadRunKey(environmentID, request.GetContainerId())]
	if run != nil {
		if !run.stopping {
			run.stopRequestID = request.GetRequestId()
			run.stopTimeoutSeconds = proto.Clone(request).(*vmmdv1.StopContainerRequest).TimeoutSeconds
		}
		run.stopping = true
		if run.stopDeadline.IsZero() {
			run.stopDeadline = time.Now().Add(workloadStopBudget)
		}
	}
	controller.unlockWorkloadChanged()
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return err
	}

	defer controller.projectionMu.Unlock()
	return controller.services.RestoreContainerServiceListeners(ctx, environmentID, request.GetContainerId())
}

// Caller holds the environment lock, including lease revocation and cleanup.
func (controller *Owner) StopWorkloadContainerLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	return controller.StopWorkloadContainerWithIntentLocked(ctx, client, environmentID, request, true)
}

func (controller *Owner) StopWorkloadContainerWithIntentLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, request *vmmdv1.StopContainerRequest, explicit bool) (*vmmdv1.StopContainerResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, workloadStopBudget)
	defer cancel()
	if err := controller.acceptWorkloadStopIntentLocked(ctx, environmentID, request, explicit); err != nil {
		return nil, err
	}
	controller.mu.Lock()
	run := controller.workloadRuns[workloadRunKey(environmentID, request.GetContainerId())]
	if run != nil {
		run.inFlight = true
	}
	if run != nil && request.GetRequestId() != "" {
		originalRequestID := request.GetRequestId()
		request = proto.Clone(request).(*vmmdv1.StopContainerRequest)
		request.RequestId = opreceipt.DerivedRequestID(fmt.Sprintf("stop-instance/%q/%q/%q/%q/%q", environmentID, request.GetActorNamespaceId(), request.GetContainerId(), originalRequestID, run.instanceID))
	}
	controller.unlockWorkloadChanged()
	defer func() {
		controller.mu.Lock()
		if run != nil {
			run.inFlight = false
		}
		controller.unlockWorkloadChanged()
	}()
	observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: request.GetActorNamespaceId(), ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	if explicit {
		if err := controller.store.SetManagedContainerRunning(ctx, environmentID, request.GetContainerId(), observed.GetContainer().GetGenerationId(), false); err != nil {
			return nil, err
		}
	}
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	projectionErr := controller.services.ProjectServiceContainerLocked(ctx, client, environmentID, request.GetContainerId(), observed.GetContainer().GetGenerationId(), "")
	controller.projectionMu.Unlock()
	if projectionErr != nil {
		return nil, projectionErr
	}
	response, err := client.StopContainer(ctx, request)
	controller.mu.Lock()
	if run != nil {
		run.inFlight = false
		if err == nil {
			observed := response.GetContainer()
			if observed.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && observed.GetInstanceId() == "" {
				controller.settleStoppedWorkloadRunLocked(run)
			} else {
				controller.observeWorkloadRunLocked(run, observed, time.Now())
			}
		}
	}
	controller.unlockWorkloadChanged()
	return response, err
}

func pendingWorkloadError(run *Run, now time.Time) error {
	if (run.stopping || run.quiescing) && !run.stopDeadline.IsZero() && !now.Before(run.stopDeadline) || run.starting && !run.uncertainStartUntil.IsZero() && !now.Before(run.uncertainStartUntil) {
		return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_WORKLOAD_STOP_UNRESOLVED, "sandbox state remains unknown after the recovery budget; inspect or stop the existing instance before retrying", hostv1.WithResource("container", run.containerID))
	}
	return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_LIFECYCLE_CONFLICT, "workload lifecycle operation is still in progress", hostv1.WithResource("container", run.containerID))
}
