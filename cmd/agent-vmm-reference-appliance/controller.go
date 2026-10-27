package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"google.golang.org/protobuf/proto"
)

type referenceAllocation struct {
	id        string
	label     string
	resources *remotev1.ResourceLimits
}

func referenceAllocations() []referenceAllocation {
	return []referenceAllocation{
		{id: "durable-service", label: "Reference durable service", resources: referenceResources(512, 256<<20)},
		{id: "ephemeral-task", label: "Reference ephemeral task", resources: referenceResources(64, 256<<20)},
	}
}

func referenceResources(pids uint32, disk uint64) *remotev1.ResourceLimits {
	return &remotev1.ResourceLimits{
		Pids: proto.Uint32(pids), DiskBytes: proto.Uint64(disk)}
}

type referenceCommandCursor struct {
	session      *controllerremote.ControlSession
	nextSequence uint64
}

func newReferenceCommandCursor(session *controllerremote.ControlSession) *referenceCommandCursor {
	return &referenceCommandCursor{session: session}
}

func reconcileReferenceAllocations(ctx context.Context, cursor *referenceCommandCursor) (map[string]*remotev1.Allocation, error) {
	hello := cursor.session.Hello()
	allocations := make(map[string]*remotev1.Allocation, len(referenceAllocations()))
	for _, desired := range referenceAllocations() {
		result, err := dispatchReferenceCommand(ctx, cursor, "ensure-"+desired.id, &remotev1.ControllerCommand{TargetRevision: hello.GetPoolRevision(), Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{
			AllocationId: desired.id, Generation: 1, DisplayLabel: desired.label, Limits: desired.resources,
			EgressMode: remotev1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: hello.GetPoolRevision(),
		}}})
		if err != nil {
			return nil, err
		}
		allocation := result.GetAllocation()
		if allocation == nil || allocation.GetRevision() == 0 || allocation.GetGeneration() == 0 {
			return nil, fmt.Errorf("reference allocation %q is unavailable", desired.id)
		}
		if _, err := dispatchReferenceCommand(ctx, cursor, "open-"+desired.id, &remotev1.ControllerCommand{Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{
			AllocationId: desired.id, AllocationGeneration: allocation.GetGeneration(), ExecutionOwnerId: "appliance:" + desired.id,
		}}}); err != nil {
			return nil, err
		}
		allocations[desired.id] = allocation
	}
	return allocations, nil
}

func maintainReferenceAllocations(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
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
