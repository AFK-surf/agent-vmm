//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
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
	runs := make(map[string]*workloadRun)
	cursor, count := "", 0
	for {
		page, err := controller.store.ListWorkloadEnvironmentPage(ctx, cursor, 32)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for _, environment := range page {
			count++
			if count > resourcev2.MaxStoredWorkloads {
				return errors.New("workload recovery exceeds 256 stored environments; resolve excess inventory before admission")
			}
			response, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
			if err != nil {
				return err
			}
			for _, container := range response.GetContainers() {
				if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_CREATED || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
					continue
				}
				if len(runs) >= maxActiveWorkloads {
					return errors.New("workload recovery exceeds four active sandboxes; stop excess instances before admission")
				}
				run := &workloadRun{environmentID: environment.ID, namespaceID: environment.NamespaceID, containerID: container.GetId(), instanceID: container.GetInstanceId(), stopping: true, stopDeadline: time.Now().Add(workloadStopBudget), stopRequestID: derivedRequestID("recover-stop-" + environment.ID + "-" + container.GetId() + "-" + container.GetInstanceId())}
				runs[workloadRunKey(environment.ID, container.GetId())] = run
			}
			cursor = environment.ID
		}
	}
	controller.mu.Lock()
	controller.workloadRuns = runs
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
				_ = controller.sampleWorkloadRuns(sampleContext, client)
				connection.Close()
			}
			cancel()
		}
	}
}

func (controller *Controller) sampleWorkloadRuns(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	response, err := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{})
	if err != nil {
		return err
	}
	pool := response.GetWorkloadPool()
	if pool == nil || pool.GetUnavailable() || response.GetWorkloadMemoryLimitBytes() == 0 {
		reason := "workload pool pressure is unavailable"
		if pool != nil && pool.GetUnavailableReason() != "" {
			reason += ": " + pool.GetUnavailableReason()
		}
		return errors.New(reason)
	}
	controller.mu.Lock()
	now := time.Now()
	controller.workloadPressureAt = now
	controller.workloadMemoryPressure = pool.GetMemoryBytes() >= response.GetWorkloadMemoryLimitBytes()/10*9 || response.GetWorkloadMemoryFullAvg10() >= 10 || response.GetWorkloadOomKills() > controller.workloadOOMKills
	controller.workloadOOMKills = response.GetWorkloadOomKills()
	runs := make([]*workloadRun, 0, len(controller.workloadRuns))
	for _, run := range controller.workloadRuns {
		if run.execution != nil && !run.execution.deadline.IsZero() && !now.Before(run.execution.deadline) && !run.stopping {
			run.stopping = true
			run.stopDeadline = now.Add(workloadStopBudget)
			run.stopRequestID = derivedRequestID("expired-execution-" + run.execution.id)
		}
		if !run.inFlight {
			runs = append(runs, run)
		}
	}
	controller.mu.Unlock()
	usageByNamespace := make(map[string]*vmmdv1.Usage, len(runs))
	for _, run := range runs {
		response, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: run.namespaceID, ContainerId: run.containerID})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				controller.confirmAbsentWorkloadRun(run, time.Now())
			}
			continue
		}
		usage, sampled := usageByNamespace[run.namespaceID]
		if !sampled {
			response, _ := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: run.namespaceID})
			usage = response.GetUsage()
			usageByNamespace[run.namespaceID] = usage
		}
		controller.mu.Lock()
		controller.observeWorkloadRunLocked(run, response.GetContainer(), time.Now())
		run.usage = usage
		retry := controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] == run && run.stopping && !run.inFlight && time.Now().Before(run.stopDeadline)
		controller.mu.Unlock()
		if retry {
			controller.retryWorkloadStop(ctx, client, run)
		}

	}
	return nil
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
	delete(controller.workloadRuns, key)
}

// Only active namespaces contribute usage. Stored environments are not polled.
func (controller *Controller) remoteWorkloadUsage(ctx context.Context, registrationID string, observedAt *timestamppb.Timestamp) *hostv1.Usage {
	now := time.Now()
	controller.mu.Lock()
	byEnvironment := make(map[string]*hostv1.Usage, len(controller.workloadRuns))
	for _, run := range controller.workloadRuns {
		byEnvironment[run.environmentID] = usageFromGuest(run.usage)
	}
	fresh := controller.workloadRunsRecovered && now.Sub(controller.workloadPressureAt) >= 0 && now.Sub(controller.workloadPressureAt) <= 2*workloadSampleInterval
	controller.mu.Unlock()
	usage := &hostv1.Usage{SampledAt: observedAt}
	for environmentID, value := range byEnvironment {
		environment, err := controller.store.GetEnvironment(ctx, environmentID)
		if err != nil {
			fresh = false
			continue
		}
		if environment.RegistrationID != registrationID {
			continue
		}
		sampleAge := time.Duration(0)
		if value != nil && value.GetSampledAt() != nil {
			sampleAge = now.Sub(value.GetSampledAt().AsTime())
		}
		if value == nil || value.GetStale() || value.GetSampledAt() == nil || sampleAge < 0 || sampleAge > 2*workloadSampleInterval {
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
	unlock := controller.lockEnvironment(run.environmentID)
	defer unlock()
	controller.mu.Lock()
	if controller.workloadRuns[workloadRunKey(run.environmentID, run.containerID)] != run || run.inFlight || !run.stopping || !time.Now().Before(run.stopDeadline) {
		controller.mu.Unlock()
		return
	}
	run.stopAttempt++
	requestID := derivedRequestID(fmt.Sprintf("%s-retry-%d", run.stopRequestID, run.stopAttempt))
	deadline := run.stopDeadline
	controller.mu.Unlock()
	stopContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	immediate := uint32(0)
	_, _ = controller.stopWorkloadContainerLocked(stopContext, client, run.environmentID, &vmmdv1.StopContainerRequest{RequestId: requestID, ActorNamespaceId: run.namespaceID, ContainerId: run.containerID, TimeoutSeconds: &immediate})
}
