//go:build darwin || linux

package workload

import (
	"context"
	"fmt"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Recovery is a one-time bounded inventory read before VM admission opens.
// Steady-state sampling reads only the admitted set, never stored workloads.
func (controller *Owner) RecoverWorkloadRuns(ctx context.Context) error {
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	return controller.RecoverRunningWorkloadsFromGuest(ctx, client)
}

func (controller *Owner) RecoverRunningWorkloadsFromGuest(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
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
		if err := controller.services.ProjectEnvironmentFromGuest(ctx, client, environment); err != nil {
			return fmt.Errorf("project environment %s namespace %s: %w", environment.ID, environment.NamespaceID, err)
		}
	}
	if err := controller.RecoverWorkloadRunsFromGuest(ctx, client); err != nil {
		return err
	}
	if err := controller.hostShares.ReconcileHostShares(ctx, client); err != nil {
		controller.logger.Warn("host share recovery failed", "error", err)
	}
	if err := controller.services.RestoreManagedServiceContainers(ctx, client); err != nil {
		controller.logger.Warn("service container recovery failed", "error", err)
	}
	return nil
}

// Running containers of owning environments are admitted as they are. Their
// lifetime ends only through an explicit stop, retirement, or disposal.
func (controller *Owner) RecoverWorkloadRunsFromGuest(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	now := time.Now()
	runs := make(map[string]*Run)
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
				runs[workloadRunKey(environment.ID, container.GetId())] = &Run{environmentID: environment.ID, registrationID: environment.RegistrationID, namespaceID: environment.NamespaceID, containerID: container.GetId(), instanceID: container.GetInstanceId(), idleSince: now}
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
	controller.unlockWorkloadChanged()
	return nil
}

func (controller *Owner) WorkloadObservationLoop(ctx context.Context) {
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
			client, err := controller.guestConnection.Client()
			if err == nil {
				_ = controller.hostShares.ReconcileHostShares(sampleContext, client)
				controller.sampleWorkloadRuns(sampleContext, client)
				_ = controller.services.RestoreManagedServiceContainers(sampleContext, client)
				if err := controller.projectionMu.Lock(sampleContext); err != nil {
					cancel()
					return
				}

				_ = controller.services.RestoreServiceProjectionsLocked(sampleContext, client)
				controller.projectionMu.Unlock()

			}
			cancel()
		}
	}
}

type workloadReclaimCandidate struct {
	run   *Run
	value *connectorv1.WorkloadReclaimCandidate
}

func (controller *Owner) clearWorkloadReclaimLocked(reason string) {
	if candidate := controller.workloadReclaim; candidate != nil {
		controller.logger.Info("workload reclaim", "reason", reason, "registration_id", candidate.value.GetRegistrationId(), "allocation_id", candidate.value.GetAllocationId(), "instance_id", candidate.value.GetContainerInstanceId(), "layer", "host", "state", "cancelled")
		run := candidate.run
		if run.quiescing && !run.stopping {
			run.quiescing = false
			run.quiesceRequestID = ""
			run.stopDeadline = time.Time{}
		}
	}
	controller.workloadReclaim = nil
}

func (controller *Owner) observeWorkloadPressureLocked(response *vmmdv1.GetUsageResponse, now time.Time) {
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

func (controller *Owner) sampleWorkloadRuns(ctx context.Context, client vmmdv1.VMMDServiceClient) {
	ctx, cancel := context.WithTimeout(ctx, workloadSampleInterval)
	defer cancel()
	response, usageErr := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{})
	pressureAvailable := usageErr == nil && response.GetWorkloadPool() != nil &&
		!response.GetWorkloadPool().GetUnavailable() && response.GetWorkloadMemoryLimitBytes() != 0
	if !pressureAvailable {
		controller.mu.Lock()
		controller.workloadPressureHigh, controller.workloadPressureLow = 0, 0
		controller.clearWorkloadReclaimLocked("pool_unavailable")
		controller.unlockWorkloadChanged()
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
	runs := make([]*Run, 0, 16)
	if candidate := controller.workloadReclaim; candidate != nil {
		runs = append(runs, candidate.run)
	}
	runs = append(runs, controller.workloadRunPageLocked(16-len(runs), now)...)
	controller.unlockWorkloadChanged()
	workers := make(chan struct{}, 4)
	var pending sync.WaitGroup
	for _, run := range runs {
		select {
		case workers <- struct{}{}:
		case <-ctx.Done():
			continue
		}
		pending.Add(1)
		go func(run *Run) {
			defer pending.Done()
			defer func() { <-workers }()
			controller.mu.Lock()
			instance := run.instanceID
			controller.mu.Unlock()
			boot := controller.manager.Snapshot().GetBootId()
			environment, environmentErr := controller.store.GetEnvironment(ctx, run.environmentID)
			observed, inspectErr := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: run.namespaceID, ContainerId: run.containerID})
			if inspectErr != nil {
				controller.mu.Lock()
				run.usage = nil
				if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
					controller.clearWorkloadReclaimLocked("inspect_unavailable")
				}
				controller.unlockWorkloadChanged()
				if status.Code(inspectErr) == codes.NotFound {
					controller.confirmAbsentWorkloadRun(run, time.Now())
				}
				return
			}
			usage, _ := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: run.namespaceID})
			controller.mu.Lock()
			current := controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] == run && run.instanceID == instance && observed.GetContainer().GetInstanceId() == instance
			controller.observeWorkloadRunLocked(run, observed.GetContainer(), time.Now())
			run.usage = usage.GetUsage()
			if current && environmentErr == nil && environment.NamespaceID == run.namespaceID && observed.GetContainer().GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				controller.rememberLocalResourceSampleLocked(environment, &hostv1.AgentEnvironment{
					Usage: guestconn.Usage(usage.GetUsage()), Containers: []*hostv1.Container{guestconn.Container(observed.GetContainer())}, InventoryObservedAt: timestamppb.Now(),
				}, boot)
			}
			if controller.workloadReclaim != nil && controller.workloadReclaim.run == run {
				if !workloadUsageFresh(run.usage, time.Now()) {
					controller.clearWorkloadReclaimLocked("usage_unavailable")
				}
			}
			retry := controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] == run && run.stopping && !run.inFlight && time.Now().Before(run.stopDeadline)
			controller.unlockWorkloadChanged()
			if retry {
				controller.retryWorkloadStop(ctx, client, run)
			}
		}(run)
	}
	pending.Wait()
	controller.mu.Lock()
	skipSelection = skipSelection || hadCandidate && controller.workloadReclaim == nil
	controller.unlockWorkloadChanged()
	if pressureAvailable && !skipSelection && ctx.Err() == nil {
		controller.selectWorkloadReclaim(ctx, runs)
	}
}

func (controller *Owner) selectWorkloadReclaim(ctx context.Context, sampled []*Run) {
	controller.mu.Lock()
	now := time.Now()
	var oldest *Run
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
		controller.unlockWorkloadChanged()
		return
	}
	instance := oldest.instanceID
	controller.unlockWorkloadChanged()
	environment, err := controller.store.GetEnvironment(ctx, oldest.environmentID)
	if err != nil || environment.RegistrationID == "" || environment.AllocationID == "" {
		return
	}
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
	if controller.workloadReclaim != nil || !controller.workloadMemoryPressure || oldest.instanceID != instance || !workloadReclaimEligible(oldest, time.Now()) ||
		controller.workloadRuns[workloadRunKey(oldest.environmentID, oldest.containerID)] != oldest {
		return
	}
	controller.logger.Info("workload reclaim", "registration_id", environment.RegistrationID, "allocation_id", environment.AllocationID, "instance_id", instance, "layer", "host", "state", "selected")
	controller.workloadReclaim = &workloadReclaimCandidate{run: oldest, value: &connectorv1.WorkloadReclaimCandidate{
		RegistrationId: environment.RegistrationID, AllocationId: environment.AllocationID, ContainerId: oldest.containerID,
		ContainerInstanceId: instance, ExpiresUnixMillis: now.Add(workloadStopBudget).UnixMilli(),
	}}
}

func workloadReclaimEligible(run *Run, now time.Time) bool {
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

func (controller *Owner) confirmAbsentWorkloadRun(run *Run, now time.Time) {
	controller.mu.Lock()
	defer controller.unlockWorkloadChanged()
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
func (controller *Owner) RemoteWorkloadUsage(_ context.Context, registrationID string, observedAt *timestamppb.Timestamp) *hostv1.Usage {
	now := time.Now()
	controller.mu.Lock()
	byEnvironment := make(map[string]*hostv1.Usage, len(controller.workloadRuns))
	for _, run := range controller.workloadRuns {
		if run.registrationID == registrationID {
			byEnvironment[run.environmentID] = guestconn.Usage(run.usage)
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

func (controller *Owner) retryWorkloadStop(ctx context.Context, client vmmdv1.VMMDServiceClient, run *Run) {
	unlock, lockErr := controller.LockEnvironment(ctx, run.environmentID)
	if lockErr != nil {
		return
	}
	defer unlock()
	controller.mu.Lock()
	if controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] != run || run.inFlight || !run.stopping || !time.Now().Before(run.stopDeadline) {
		controller.unlockWorkloadChanged()
		return
	}
	requestID := run.stopRequestID
	timeoutSeconds := run.stopTimeoutSeconds
	deadline := run.stopDeadline
	controller.unlockWorkloadChanged()
	stopContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	_, _ = controller.StopWorkloadContainerLocked(stopContext, client, run.environmentID, &vmmdv1.StopContainerRequest{RequestId: requestID, ActorNamespaceId: run.namespaceID, ContainerId: run.containerID, TimeoutSeconds: timeoutSeconds})
}
