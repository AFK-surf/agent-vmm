package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
)

const referenceLeaseSeconds = 300

type referenceAllocation struct {
	id          string
	label       string
	workloadRef string
	resources   *hostv1.WorkloadResourcesV2
}

func referenceAllocations() []referenceAllocation {
	return []referenceAllocation{
		{id: "durable-service", label: "Reference durable service", workloadRef: "durable-service-environment", resources: referenceResources(100, 128<<20, 64, 256<<20)},
		{id: "ephemeral-task", label: "Reference ephemeral task", workloadRef: "ephemeral-task-allocation", resources: referenceResources(100, 128<<20, 64, 256<<20)},
	}
}

func referenceResources(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.WorkloadResourcesV2 {
	return &hostv1.WorkloadResourcesV2{CpuMaxMillis: cpu,
		MemoryMaxBytes: memory, PidMax: pids, WritableQuotaBytes: disk}
}

type referenceCommandCursor struct {
	session      *controllerremote.ControlSession
	nextSequence uint64
}

func newReferenceCommandCursor(session *controllerremote.ControlSession) *referenceCommandCursor {
	return &referenceCommandCursor{session: session}
}

func reconcileReferenceAllocations(ctx context.Context, cursor *referenceCommandCursor) (map[string]*remotev1.Lease, error) {
	hello := cursor.session.Hello()
	inventory := make(map[string]*remotev1.Allocation, len(hello.GetInventory()))
	leases := make(map[string]*remotev1.Lease, len(referenceAllocations()))
	for _, allocation := range hello.GetInventory() {
		inventory[allocation.GetAllocationId()] = allocation
	}
	for _, desired := range referenceAllocations() {
		result, err := dispatchReferenceCommand(ctx, cursor, "ensure-"+desired.id, &remotev1.ControllerCommand{TargetRevision: hello.GetPoolRevision(), Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{
			AllocationId: desired.id, Generation: 1, DisplayLabel: desired.label, ResourcesV2: desired.resources,
			EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: hello.GetPoolRevision(),
		}}})
		if err != nil {
			return nil, err
		}
		allocation := result.GetAllocation()
		if existing := inventory[desired.id]; allocation.GetLease() == nil && existing.GetLease() != nil {
			allocation.Lease = existing.GetLease()
		}
		if allocation == nil || allocation.GetRevision() == 0 {
			return nil, fmt.Errorf("reference allocation %q is unavailable", desired.id)
		}
		lease := allocation.GetLease()
		if lease == nil || lease.GetState() != hostv1.LeaseState_LEASE_STATE_ACTIVE {
			result, err := dispatchReferenceCommand(ctx, cursor, "acquire-"+desired.id, &remotev1.ControllerCommand{TargetRevision: allocation.GetRevision(), Command: &remotev1.ControllerCommand_AcquireLease{AcquireLease: &remotev1.AcquireLease{
				AllocationId: desired.id, DurationSeconds: referenceLeaseSeconds, WorkloadRef: desired.workloadRef,
			}}})
			if err != nil {
				return nil, err
			}
			lease = result.GetLease()
		}
		if lease == nil || lease.GetAllocationId() != desired.id || lease.GetGeneration() == 0 {
			return nil, fmt.Errorf("reference lease for %q is unavailable", desired.id)
		}
		if _, err := dispatchReferenceCommand(ctx, cursor, "open-"+desired.id, &remotev1.ControllerCommand{LeaseGeneration: lease.GetGeneration(), Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{
			AllocationId: desired.id, LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration(), ExecutionOwnerId: "appliance:" + desired.id,
		}}}); err != nil {
			return nil, err
		}
		leases[desired.id] = lease
	}
	return leases, nil
}

func maintainReferenceLeases(ctx context.Context, cursor *referenceCommandCursor, leases map[string]*remotev1.Lease) error {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for cycle := uint64(1); ; cycle++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		for allocationID, lease := range leases {
			result, err := dispatchReferenceCommand(ctx, cursor, fmt.Sprintf("renew-%d-%s", cycle, allocationID), &remotev1.ControllerCommand{LeaseGeneration: lease.GetGeneration(), Command: &remotev1.ControllerCommand_RenewLease{RenewLease: &remotev1.RenewLease{
				LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration(), DurationSeconds: referenceLeaseSeconds,
			}}})
			if err != nil {
				return err
			}
			leases[allocationID] = result.GetLease()
		}
	}
}

func dispatchReferenceCommand(ctx context.Context, cursor *referenceCommandCursor, name string, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	if command == nil || command.GetCommand() == nil {
		return nil, errors.New("unsupported reference command")
	}
	cursor.nextSequence++
	command.CommandId = fmt.Sprintf("reference-%d-%s", cursor.session.Hello().GetConnectionEpoch(), name)
	command.Sequence = cursor.nextSequence
	command.ConnectionEpoch = cursor.session.Hello().GetConnectionEpoch()
	commandContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transcript, err := cursor.session.Dispatch(commandContext, command)
	if err != nil {
		return nil, err
	}
	if transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return nil, fmt.Errorf("reference command %q failed: %s: %s", name, transcript.Result.GetReason(), transcript.Result.GetMessage())
	}
	return transcript.Result, nil
}

type referenceSessionBroker struct {
	epoch  atomic.Uint64
	mu     sync.Mutex
	active map[string]net.Conn
	ready  map[string]chan struct{}
}

func newReferenceSessionBroker() *referenceSessionBroker {
	return &referenceSessionBroker{active: make(map[string]net.Conn), ready: make(map[string]chan struct{})}
}

func (broker *referenceSessionBroker) setEpoch(value uint64) { broker.epoch.Store(value) }
func (broker *referenceSessionBroker) epochValue() uint64    { return broker.epoch.Load() }

func (broker *referenceSessionBroker) deliver(ctx context.Context, allocationID string, connection net.Conn) error {
	if allocationID == "" || connection == nil {
		return errors.New("complete reference runtime session is required")
	}
	broker.mu.Lock()
	previous := broker.active[allocationID]
	broker.active[allocationID] = connection
	ready := broker.ready[allocationID]
	if ready == nil {
		ready = make(chan struct{}, 1)
		broker.ready[allocationID] = ready
	}
	broker.mu.Unlock()
	select {
	case ready <- struct{}{}:
	default:
	}
	if previous != nil {
		_ = previous.Close()
	}
	go func() {
		<-ctx.Done()
		broker.mu.Lock()
		if broker.active[allocationID] == connection {
			delete(broker.active, allocationID)
		}
		broker.mu.Unlock()
		_ = connection.Close()
	}()
	return nil
}

func (broker *referenceSessionBroker) take(ctx context.Context, allocationID string) (net.Conn, error) {
	for {
		broker.mu.Lock()
		connection := broker.active[allocationID]
		ready := broker.ready[allocationID]
		if ready == nil {
			ready = make(chan struct{}, 1)
			broker.ready[allocationID] = ready
		}
		if connection != nil {
			delete(broker.active, allocationID)
			broker.mu.Unlock()
			return connection, nil
		}
		broker.mu.Unlock()
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
