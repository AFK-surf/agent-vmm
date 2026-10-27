//go:build darwin || linux

package workload

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Owner struct {
	store                    *store.Store
	guestConnection          *guestconn.Connection
	manager                  interface{ Snapshot() *hostv1.VM }
	projectionMu             *locks.RWMutex
	services                 Services
	hostShares               HostShares
	logger                   *slog.Logger
	mu                       sync.Mutex
	observed                 map[string][]*hostv1.Container
	localResourceCache       map[string]localResourceSample
	workloadRuns             map[string]*Run
	workloadRunOrder         []string
	workloadRunIndex         map[string]int
	workloadRunsRecovered    bool
	stoppedWorkloadInstances map[string]string
	workloadPressureAt       time.Time
	workloadMemoryPressure   bool
	workloadOOMKills         uint64
	workloadOOMEvents        uint64
	workloadPressureHigh     int
	workloadPressureLow      int
	workloadSampleOffset     int
	workloadReclaim          *workloadReclaimCandidate
	imageOperationLocks      locks.Keyed[string]
	environmentLocks         map[string]*locks.RWMutex
	changed                  chan struct{}
}

func (controller *Owner) RLockImageOperation(ctx context.Context, key string) (func(), error) {
	return controller.imageOperationLocks.RLock(ctx, key)
}

func (controller *Owner) LockImageOperation(ctx context.Context, key string) (func(), error) {
	return controller.imageOperationLocks.Lock(ctx, key)
}

func (controller *Owner) LockEnvironment(ctx context.Context, environmentID string) (func(), error) {
	controller.mu.Lock()
	if controller.environmentLocks == nil {
		controller.environmentLocks = make(map[string]*locks.RWMutex)
	}
	lock := controller.environmentLocks[environmentID]
	if lock == nil {
		lock = &locks.RWMutex{}
		controller.environmentLocks[environmentID] = lock
	}
	controller.mu.Unlock()
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	return lock.Unlock, nil
}

func (controller *Owner) RememberContainers(environmentID string, containers []*hostv1.Container) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.observed[environmentID] = cloneContainers(containers, false)
}

func (controller *Owner) UnknownContainers(environmentID string) []*hostv1.Container {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return cloneContainers(controller.observed[environmentID], true)
}

func (controller *Owner) ForgetContainers(environmentID string) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	delete(controller.observed, environmentID)
	delete(controller.localResourceCache, environmentID)
	prefix := environmentID + "\x00"
	for key := range controller.stoppedWorkloadInstances {
		if strings.HasPrefix(key, prefix) {
			delete(controller.stoppedWorkloadInstances, key)
		}
	}
}

func cloneContainers(containers []*hostv1.Container, unknown bool) []*hostv1.Container {
	result := make([]*hostv1.Container, 0, len(containers))
	for _, container := range containers {
		copy := proto.Clone(container).(*hostv1.Container)
		if unknown {
			copy.State = hostv1.ContainerState_CONTAINER_STATE_UNKNOWN
		}
		result = append(result, copy)
	}
	return result
}

const localResourceFreshness = 15 * time.Second

type localResourceSample struct {
	namespace, boot string
	generation      uint64
	disk            *uint64
	sampledAt       time.Time
	observedAt      time.Time
	active          bool
}

// Cache one scalar observation per environment from its existing owner read.
// This does not add a sampler, timers, or Guest probes to the overview path.
func (controller *Owner) RememberLocalResourceSample(environment store.Environment, snapshot *hostv1.AgentEnvironment) {
	usage := snapshot.GetUsage()
	if usage == nil || usage.GetStale() || usage.GetUnavailableReason() != "" || usage.GetSampledAt() == nil {
		return
	}
	bytes := usage.GetDiskBytes()
	sample := localResourceSample{namespace: environment.NamespaceID, boot: controller.manager.Snapshot().GetBootId(), generation: environment.AllocationGeneration, disk: &bytes, sampledAt: usage.GetSampledAt().AsTime(), observedAt: snapshot.GetInventoryObservedAt().AsTime()}
	// Partial positive evidence may prove activity. It cannot prove that other work is stopped.
	for _, container := range snapshot.GetContainers() {
		if container.GetState() == hostv1.ContainerState_CONTAINER_STATE_RUNNING {
			sample.active = true
			break
		}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.localResourceCache == nil {
		controller.localResourceCache = make(map[string]localResourceSample)
	}
	controller.localResourceCache[environment.ID] = sample
}

func (controller *Owner) ResetWorkloadRecovery() {
	controller.mu.Lock()
	controller.workloadRunsRecovered = false
	controller.notifyWorkloadChangedLocked()
	controller.mu.Unlock()
}
func (controller *Owner) notifyWorkloadChangedLocked() {
	if controller.changed != nil {
		close(controller.changed)
		controller.changed = nil
	}
}
func (controller *Owner) WaitRegistrationQuiesced(ctx context.Context, registrationID string) error {
	for {
		controller.mu.Lock()
		settled := controller.workloadRunsRecovered
		for _, run := range controller.workloadRuns {
			if run.registrationID == registrationID && (run.starting || run.stopping || run.inFlight || run.quiescing || !run.uncertainStartUntil.IsZero() || len(run.executions) > 0) {
				settled = false
				break
			}
		}
		if settled {
			controller.mu.Unlock()
			return nil
		}
		if controller.changed == nil {
			controller.changed = make(chan struct{})
		}
		changed := controller.changed
		controller.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (controller *Owner) unlockWorkloadChanged() {
	controller.notifyWorkloadChangedLocked()
	controller.mu.Unlock()
}

type Services interface {
	ProjectEnvironmentFromGuest(context.Context, vmmdv1.VMMDServiceClient, store.Environment) error
	ProjectServiceContainerLocked(context.Context, vmmdv1.VMMDServiceClient, string, string, string, string) error
	RestoreContainerServiceListeners(context.Context, string, string) error
	RestoreManagedServiceContainers(context.Context, vmmdv1.VMMDServiceClient) error
	RestoreServiceProjectionsLocked(context.Context, vmmdv1.VMMDServiceClient) error
}
type HostShares interface {
	AuthorizeHostShareMounts(context.Context, string, []*vmmdv1.VolumeMount) error
	ReconcileHostShares(context.Context, vmmdv1.VMMDServiceClient) error
}
type Config struct {
	Store      *store.Store
	Guest      *guestconn.Connection
	Manager    interface{ Snapshot() *hostv1.VM }
	Projection *locks.RWMutex
	Services   Services
	HostShares HostShares
	Logger     *slog.Logger
}

func New(config Config) *Owner {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Owner{store: config.Store, guestConnection: config.Guest, manager: config.Manager, projectionMu: config.Projection, services: config.Services, hostShares: config.HostShares, logger: logger.With("component", "host.workload"), observed: make(map[string][]*hostv1.Container), workloadRuns: make(map[string]*Run), workloadRunIndex: make(map[string]int)}
}

type Instance struct {
	InstanceID                   string
	Starting, Stopping, InFlight bool
}

func (controller *Owner) Instance(environmentID, containerID string) (Instance, bool) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	run := controller.workloadRuns[workloadRunKey(environmentID, containerID)]
	if run == nil {
		return Instance{}, false
	}
	return Instance{InstanceID: run.instanceID, Starting: run.starting, Stopping: run.stopping, InFlight: run.inFlight}, true
}
func (controller *Owner) ReclaimCandidate(registrationID string) *connectorv1.WorkloadReclaimCandidate {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	candidate := controller.workloadReclaim
	if candidate == nil {
		return nil
	}
	run := candidate.run
	now := time.Now()
	if now.UnixMilli() >= candidate.value.GetExpiresUnixMillis() || now.Sub(controller.workloadPressureAt) > 2*workloadSampleInterval || len(run.executions) != 0 || run.stopping || run.instanceID != candidate.value.GetContainerInstanceId() {
		controller.clearWorkloadReclaimLocked("deadline_expired")
		controller.notifyWorkloadChangedLocked()
		return nil
	}
	if candidate.value.GetRegistrationId() != registrationID {
		return nil
	}
	return proto.Clone(candidate.value).(*connectorv1.WorkloadReclaimCandidate)
}
func (controller *Owner) ApplyLocalResourceSample(item *hostv1.LocalEnvironmentSummary, row store.Environment, vm *hostv1.VM, now time.Time) {
	controller.mu.Lock()
	sample, ok := controller.localResourceCache[row.ID]
	controller.mu.Unlock()
	if ok && sample.boot == vm.GetBootId() && sample.namespace == row.NamespaceID && sample.generation == row.AllocationGeneration && now.Sub(sample.sampledAt) >= 0 && now.Sub(sample.sampledAt) <= localResourceFreshness && vm.GetState() == hostv1.VMState_VM_STATE_RUNNING {
		item.PrivateDiskBytes = sample.disk
		item.SampledAt = timestamppb.New(sample.sampledAt)
		if sample.active && item.State == "unknown" {
			item.State = "observed_active"
			item.StateObservedAt = timestamppb.New(sample.observedAt)
		}
	}
}
