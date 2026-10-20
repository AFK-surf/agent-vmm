//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Recovery is a one-time bounded inventory read before VM admission opens.
// Steady-state sampling reads only the admitted set, never stored workloads.
func (controller *Controller) recoverWorkloadRuns(ctx context.Context) error {
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	return controller.recoverRunningWorkloadsFromGuest(ctx, client)
}

func (controller *Controller) recoverRunningWorkloadsFromGuest(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	environments, err := controller.store.ListEnvironments(ctx)
	if err != nil {
		return fmt.Errorf("read namespace projection owners: %w", err)
	}
	for _, environment := range environments {
		if environment.OwnerKind == store.OwnerRemoteRegistration {
			_, err := client.GetNamespace(ctx, &vmmdv1.GetNamespaceRequest{NamespaceId: environment.NamespaceID})
			if status.Code(err) == codes.NotFound {
				continue
			}
			if err != nil {
				return fmt.Errorf("observe allocation environment %s namespace %s: %w", environment.ID, environment.NamespaceID, err)
			}
		}
		if err := controller.projectEnvironmentFromGuest(ctx, client, environment); err != nil {
			return fmt.Errorf("project environment %s namespace %s: %w", environment.ID, environment.NamespaceID, err)
		}
	}
	if err := controller.recoverWorkloadRunsFromGuest(ctx, client); err != nil {
		return err
	}
	if err := controller.reconcileHostShares(ctx, client); err != nil {
		log.Printf("host share recovery failed: %v", err)
	}
	if err := controller.restoreManagedServiceContainers(ctx, client); err != nil {
		log.Printf("service container recovery failed: %v", err)
	}
	return nil
}

func (controller *Controller) recoverWorkloadRunsFromGuest(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	containers, err := controller.store.ListManagedContainers(ctx)
	if err != nil {
		return err
	}
	managed := make(map[string]string)
	for _, container := range containers {
		if container.DesiredRunning {
			managed[workloadRunKey(container.EnvironmentID, container.ContainerID)] = container.ContainerGenerationID
		}
	}
	runs := make(map[string]*workloadRun)
	stopped := make(map[string]string)
	cursor := ""
	for {
		page, err := controller.store.ListWorkloadEnvironmentPage(ctx, cursor, 32)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for _, environment := range page {
			cursor = environment.ID
			response, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
			if err != nil {
				if status.Code(err) == codes.NotFound && environment.OwnerKind == store.OwnerRemoteRegistration {
					_, namespaceErr := client.GetNamespace(ctx, &vmmdv1.GetNamespaceRequest{NamespaceId: environment.NamespaceID})
					if status.Code(namespaceErr) == codes.NotFound {
						continue
					}
					if namespaceErr != nil {
						return fmt.Errorf("confirm allocation environment %s namespace %s: %w", environment.ID, environment.NamespaceID, namespaceErr)
					}
				}
				return fmt.Errorf("list workload containers environment %s namespace %s: %w", environment.ID, environment.NamespaceID, err)
			}
			for _, container := range response.GetContainers() {
				if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_CREATED || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
					if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && container.GetInstanceId() == "" &&
						container.GetStartRequestInstanceId() != "" && container.GetStartRequestSha256() != "" && container.GetStartedAtUnixNano() != 0 &&
						len(stopped) < maxStoppedWorkloadInstances {
						stopped[workloadRunKey(environment.ID, container.GetId())] = container.GetStartRequestInstanceId()
					}
					continue
				}
				run := &workloadRun{environmentID: environment.ID, registrationID: environment.RegistrationID, namespaceID: environment.NamespaceID, containerID: container.GetId(), instanceID: container.GetInstanceId(), stopping: true, stopDeadline: time.Now().Add(workloadStopBudget), stopRequestID: derivedRequestID("recover-stop-" + environment.ID + "-" + container.GetId() + "-" + container.GetInstanceId()), stopTimeoutSeconds: optional(uint32(0))}
				if generation, present := managed[workloadRunKey(environment.ID, container.GetId())]; present && generation == container.GetGenerationId() {
					run.stopping = false
					run.stopDeadline = time.Time{}
					run.stopRequestID = ""
				}
				runs[workloadRunKey(environment.ID, container.GetId())] = run
			}
		}
	}
	controller.mu.Lock()
	controller.clearWorkloadReclaimLocked("recovery")
	controller.resetWorkloadRunsLocked(runs)
	controller.stoppedWorkloadInstances = stopped
	controller.workloadOOMKills, controller.workloadOOMEvents = 0, 0
	controller.workloadPressureHigh, controller.workloadPressureLow = 0, 0
	controller.workloadMemoryPressure = false
	controller.workloadRunsRecovered = true
	controller.mu.Unlock()
	return controller.sampleWorkloadRuns(ctx, client)
}

func (controller *Controller) workloadObservationLoop(ctx context.Context) {
	ticker := time.NewTicker(workloadSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			controller.mu.Lock()
			recovered := controller.workloadRunsRecovered
			controller.mu.Unlock()
			if !recovered {
				continue
			}
			sampleContext, cancel := context.WithTimeout(ctx, workloadSampleInterval)
			client, connection, err := controller.guestClient(sampleContext)
			if err == nil {
				_ = controller.reconcileHostShares(sampleContext, client)
				_ = controller.sampleWorkloadRuns(sampleContext, client)
				_ = controller.restoreManagedServiceContainers(sampleContext, client)
				controller.projectionMu.Lock()
				_ = controller.restoreServiceProjectionsLocked(sampleContext, client)
				controller.projectionMu.Unlock()
				connection.Close()
			}
			cancel()
		}
	}
}

type workloadReclaimCandidate struct {
	run   *workloadRun
	value *hostv1.WorkloadReclaimCandidate
}

func (controller *Controller) clearWorkloadReclaimLocked(reason string) {
	if candidate := controller.workloadReclaim; candidate != nil {
		log.Printf("workload reclaim: layer=host state=cancelled reason=%s registration=%s allocation=%s instance=%s", reason, candidate.value.GetRegistrationId(), candidate.value.GetAllocationId(), candidate.value.GetContainerInstanceId())
		run := candidate.run
		if run.quiescing && !run.stopping {
			run.quiescing = false
			run.quiesceRequestID = ""
			run.stopDeadline = time.Time{}
		}
	}
	controller.workloadReclaim = nil
}

func (controller *Controller) observeWorkloadPressureLocked(response *vmmdv1.GetUsageResponse, now time.Time) {
	pool := response.GetWorkloadPool()
	limit := response.GetWorkloadMemoryLimitBytes()
	high := float64(pool.GetMemoryBytes())/float64(limit) >= .9 || response.GetWorkloadMemoryFullAvg10() >= 10
	low := float64(pool.GetMemoryBytes())/float64(limit) < .8 && response.GetWorkloadMemoryFullAvg10() < 1
	localOOM := response.GetWorkloadOomKills() > controller.workloadOOMKills || response.GetWorkloadOomEvents() > controller.workloadOOMEvents
	controller.workloadOOMKills = response.GetWorkloadOomKills()
	controller.workloadOOMEvents = response.GetWorkloadOomEvents()
	controller.workloadPressureAt = now
	if high {
		controller.workloadPressureHigh = min(3, controller.workloadPressureHigh+1)
	} else {
		controller.workloadPressureHigh = 0
	}
	if low {
		controller.workloadPressureLow = min(3, controller.workloadPressureLow+1)
	} else {
		controller.workloadPressureLow = 0
	}
	if localOOM || controller.workloadPressureHigh >= 3 {
		controller.workloadMemoryPressure = true
		controller.workloadPressureLow = 0
	} else if controller.workloadPressureLow >= 3 {
		controller.workloadMemoryPressure = false
	}
}

func (controller *Controller) sampleWorkloadRuns(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	ctx, cancel := context.WithTimeout(ctx, workloadSampleInterval)
	defer cancel()
	response, usageErr := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{})
	pressureAvailable := usageErr == nil && response.GetWorkloadPool() != nil &&
		!response.GetWorkloadPool().GetUnavailable() && response.GetWorkloadMemoryLimitBytes() != 0
	if !pressureAvailable {
		controller.mu.Lock()
		controller.workloadPressureHigh, controller.workloadPressureLow = 0, 0
		controller.clearWorkloadReclaimLocked("pool_unavailable")
		controller.mu.Unlock()
	}
	controller.mu.Lock()
	now := time.Now()
	if pressureAvailable {
		controller.observeWorkloadPressureLocked(response, now)
	}
	skipSelection := false
	hadCandidate := controller.workloadReclaim != nil
	if candidate := controller.workloadReclaim; candidate != nil {
		run := candidate.run
		if !controller.workloadMemoryPressure || now.UnixMilli() >= candidate.value.GetExpiresUnixMillis() ||
			controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] != run ||
			run.instanceID != candidate.value.GetContainerInstanceId() || len(run.executions) != 0 || run.starting || run.stopping {
			reason := "pressure_or_instance_changed"
			if now.UnixMilli() >= candidate.value.GetExpiresUnixMillis() {
				reason = "deadline_expired"
			}
			controller.clearWorkloadReclaimLocked(reason)
			skipSelection = true
		}
	}
	runs := make([]*workloadRun, 0, 16)
	if candidate := controller.workloadReclaim; candidate != nil {
		runs = append(runs, candidate.run)
	}
	runs = append(runs, controller.workloadRunPageLocked(16-len(runs), now)...)
	controller.mu.Unlock()
	workers := make(chan struct{}, 4)
	var pending sync.WaitGroup
	for _, run := range runs {
		select {
		case workers <- struct{}{}:
		case <-ctx.Done():
			continue
		}
		pending.Add(1)
		go func(run *workloadRun) {
			defer pending.Done()
			defer func() { <-workers }()
			observed, inspectErr := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: run.namespaceID, ContainerId: run.containerID})
			if inspectErr != nil {
				controller.mu.Lock()
				run.usage = nil
				if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
					controller.clearWorkloadReclaimLocked("inspect_unavailable")
				}
				controller.mu.Unlock()
				if status.Code(inspectErr) == codes.NotFound {
					controller.confirmAbsentWorkloadRun(run, time.Now())
				}
				return
			}
			usage, _ := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: run.namespaceID})
			controller.mu.Lock()
			controller.observeWorkloadRunLocked(run, observed.GetContainer(), time.Now())
			run.usage = usage.GetUsage()
			if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
				if !workloadUsageFresh(run.usage, time.Now()) {
					controller.clearWorkloadReclaimLocked("usage_unavailable")
				}
			}
			retry := controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] == run && run.stopping && !run.inFlight && time.Now().Before(run.stopDeadline)
			controller.mu.Unlock()
			if retry {
				controller.retryWorkloadStop(ctx, client, run)
			}
		}(run)
	}
	pending.Wait()
	controller.mu.Lock()
	skipSelection = skipSelection || hadCandidate && controller.workloadReclaim == nil
	controller.mu.Unlock()
	if pressureAvailable && !skipSelection && ctx.Err() == nil {
		controller.selectWorkloadReclaim(ctx, runs)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !pressureAvailable {
		return errors.New("workload pool pressure is unavailable")
	}
	return nil
}

func (controller *Controller) selectWorkloadReclaim(ctx context.Context, sampled []*workloadRun) {
	controller.mu.Lock()
	now := time.Now()
	var oldest *workloadRun
	if controller.workloadMemoryPressure && controller.workloadReclaim == nil {
		for _, run := range sampled {
			if !workloadReclaimEligible(run, now) {
				continue
			}
			if oldest == nil || run.idleSince.Before(oldest.idleSince) || run.idleSince.Equal(oldest.idleSince) && workloadRunKey(run.environmentID, run.containerID) < workloadRunKey(oldest.environmentID, oldest.containerID) {
				oldest = run
			}
		}
	}
	if oldest == nil || controller.store == nil {
		controller.mu.Unlock()
		return
	}
	instance := oldest.instanceID
	controller.mu.Unlock()
	environment, err := controller.store.GetEnvironment(ctx, oldest.environmentID)
	if err != nil || environment.RegistrationID == "" || environment.AllocationID == "" {
		return
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.workloadReclaim != nil || !controller.workloadMemoryPressure || oldest.instanceID != instance || !workloadReclaimEligible(oldest, time.Now()) ||
		controller.workloadRuns[workloadRunKey(oldest.environmentID, oldest.containerID)] != oldest {
		return
	}
	log.Printf("workload reclaim: layer=host state=selected registration=%s allocation=%s instance=%s", environment.RegistrationID, environment.AllocationID, instance)
	controller.workloadReclaim = &workloadReclaimCandidate{run: oldest, value: &hostv1.WorkloadReclaimCandidate{
		RegistrationId: environment.RegistrationID, AllocationId: environment.AllocationID, ContainerId: oldest.containerID,
		ContainerInstanceId: instance, ExpiresUnixMillis: now.Add(workloadStopBudget).UnixMilli(),
	}}
}

func workloadReclaimEligible(run *workloadRun, now time.Time) bool {
	if run.inFlight || run.starting || run.stopping || run.quiescing || len(run.executions) != 0 || run.idleSince.IsZero() ||
		now.Sub(run.idleSince) < time.Minute || run.usage == nil || run.usage.GetUnavailable() {
		return false
	}
	return workloadUsageFresh(run.usage, now)
}

func workloadUsageFresh(usage *vmmdv1.Usage, now time.Time) bool {
	if usage == nil || usage.GetUnavailable() || usage.GetSampledAtUnixNano() == 0 {
		return false
	}
	age := now.Sub(time.Unix(0, usage.GetSampledAtUnixNano()))
	return age >= -workloadSampleInterval && age <= 2*workloadSampleInterval
}

func (controller *Controller) confirmAbsentWorkloadRun(run *workloadRun, now time.Time) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	key := workloadRunKey(run.environmentID, run.containerID)
	if controller.workloadRuns[key] != run || run.inFlight {
		return
	}
	if !run.uncertainStartUntil.IsZero() && now.Before(run.uncertainStartUntil) {
		return
	}
	if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
		controller.clearWorkloadReclaimLocked("instance_absent")
	}
	controller.deleteWorkloadRunLocked(key)
}

// Only active namespaces contribute usage. Stored environments are not polled.
func (controller *Controller) remoteWorkloadUsage(_ context.Context, registrationID string, observedAt *timestamppb.Timestamp) *hostv1.Usage {
	now := time.Now()
	controller.mu.Lock()
	byEnvironment := make(map[string]*hostv1.Usage, len(controller.workloadRuns))
	for _, run := range controller.workloadRuns {
		if run.registrationID == registrationID {
			byEnvironment[run.environmentID] = usageFromGuest(run.usage)
		}
	}
	fresh := controller.workloadRunsRecovered && now.Sub(controller.workloadPressureAt) >= 0 && now.Sub(controller.workloadPressureAt) <= 2*workloadSampleInterval
	controller.mu.Unlock()
	usage := &hostv1.Usage{SampledAt: observedAt}
	for _, value := range byEnvironment {
		sampleAge := time.Duration(0)
		if value != nil && value.GetSampledAt() != nil {
			sampleAge = now.Sub(value.GetSampledAt().AsTime())
		}
		if value == nil || value.GetStale() || value.GetSampledAt() == nil || sampleAge < -workloadSampleInterval || sampleAge > 2*workloadSampleInterval {
			fresh = false
			continue
		}
		usage.CpuNanos += value.GetCpuNanos()
		usage.MemoryBytes += value.GetMemoryBytes()
		usage.Pids += value.GetPids()
		usage.ReadBytes += value.GetReadBytes()
		usage.WriteBytes += value.GetWriteBytes()
		usage.DiskBytes += value.GetDiskBytes()
		usage.NetworkRxBytes += value.GetNetworkRxBytes()
		usage.NetworkTxBytes += value.GetNetworkTxBytes()
	}
	if !fresh {
		return &hostv1.Usage{SampledAt: observedAt, Stale: true, UnavailableReason: "active workload metrics are unavailable"}
	}
	return usage
}

func (controller *Controller) retryWorkloadStop(ctx context.Context, client vmmdv1.VMMDServiceClient, run *workloadRun) {
	unlock, err := controller.lockEnvironmentContext(ctx, run.environmentID)
	if err != nil {
		return
	}
	defer unlock()
	controller.mu.Lock()
	if controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] != run || run.inFlight || !run.stopping || !time.Now().Before(run.stopDeadline) {
		controller.mu.Unlock()
		return
	}
	requestID := run.stopRequestID
	timeoutSeconds := run.stopTimeoutSeconds
	deadline := run.stopDeadline
	controller.mu.Unlock()
	stopContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	_, _ = controller.stopWorkloadContainerLocked(stopContext, client, run.environmentID, &vmmdv1.StopContainerRequest{RequestId: requestID, ActorNamespaceId: run.namespaceID, ContainerId: run.containerID, TimeoutSeconds: timeoutSeconds})
}
