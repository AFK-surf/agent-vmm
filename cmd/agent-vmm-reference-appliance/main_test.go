package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
)

type referenceControlStream struct {
	ctx       context.Context
	hello     *remotev1.HostHello
	once      sync.Once
	responses chan *remotev1.HostControlMessage
	mu        sync.Mutex
	commands  []*remotev1.ControllerCommand
	ready     *remotev1.ControllerReady
}

func newReferenceControlStream(ctx context.Context, inventory []*remotev1.Allocation) *referenceControlStream {
	return &referenceControlStream{ctx: ctx, responses: make(chan *remotev1.HostControlMessage, 32), hello: &remotev1.HostHello{
		ProtocolVersion: controllerremote.ProtocolVersion, HostApiVersion: "host.v1", RegistrationId: "registration", ConnectionEpoch: 7,
		InventoryWatermark: 3, PoolRevision: 2, Inventory: inventory,
		SupportedFeatures: append([]string(nil), controllerremote.RequiredFeatures...), RequiredFeatures: append([]string(nil), controllerremote.RequiredFeatures...),
	}}
}

func (stream *referenceControlStream) Context() context.Context { return stream.ctx }

func (stream *referenceControlStream) Recv() (*remotev1.HostControlMessage, error) {
	var first *remotev1.HostControlMessage
	stream.once.Do(func() {
		first = &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: stream.hello}}
	})
	if first != nil {
		return first, nil
	}
	select {
	case response := <-stream.responses:
		return response, nil
	case <-stream.ctx.Done():
		return nil, stream.ctx.Err()
	}
}

func (stream *referenceControlStream) Send(message *remotev1.ControllerControlMessage) error {
	if ready := message.GetControllerReady(); ready != nil {
		stream.ready = ready
		return nil
	}
	command := message.GetCommand()
	stream.mu.Lock()
	stream.commands = append(stream.commands, command)
	stream.mu.Unlock()
	stream.responses <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Ack{Ack: &remotev1.CommandAck{
		CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Disposition: remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED,
	}}}
	result := &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: 4}
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_EnsureAllocation:
		allocation := &remotev1.Allocation{AllocationId: value.EnsureAllocation.GetAllocationId(), Generation: value.EnsureAllocation.GetGeneration(), ResourcesV2: value.EnsureAllocation.GetResourcesV2(), EgressMode: value.EnsureAllocation.GetEgressMode(), Revision: 5, State: hostv1.AllocationState_ALLOCATION_STATE_READY}
		for _, existing := range stream.hello.GetInventory() {
			if existing.GetAllocationId() == allocation.GetAllocationId() {
				allocation.Generation = existing.GetGeneration()
			}
		}
		result.Result = &remotev1.CommandResult_Allocation{Allocation: allocation}
	case *remotev1.ControllerCommand_OpenSession:
		result.Result = &remotev1.CommandResult_SessionReady{SessionReady: &remotev1.SessionReady{AllocationId: value.OpenSession.GetAllocationId(), AllocationGeneration: value.OpenSession.GetAllocationGeneration(), TunnelNonce: "nonce"}}
	}
	stream.responses <- &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Result{Result: result}}
	return nil
}

func TestReferenceControllerReconcilesEmptyAndRestartedInventory(t *testing.T) {
	tests := []struct {
		name      string
		inventory []*remotev1.Allocation
		commands  int
	}{
		{name: "empty inventory", commands: 4},
		{name: "retained inventory", inventory: []*remotev1.Allocation{{AllocationId: "durable-service", Generation: 2, Revision: 4, State: hostv1.AllocationState_ALLOCATION_STATE_RETAINED}, {AllocationId: "ephemeral-task", Generation: 3, Revision: 4, State: hostv1.AllocationState_ALLOCATION_STATE_RETAINED}}, commands: 4},
		{name: "active inventory", inventory: []*remotev1.Allocation{{AllocationId: "durable-service", Generation: 2, Revision: 5, State: hostv1.AllocationState_ALLOCATION_STATE_READY}, {AllocationId: "ephemeral-task", Generation: 3, Revision: 5, State: hostv1.AllocationState_ALLOCATION_STATE_READY}}, commands: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream := newReferenceControlStream(ctx, test.inventory)
			session, err := controllerremote.AcceptControl(stream, "registration")
			if err != nil {
				t.Fatal(err)
			}
			if err := session.Ready(); err != nil {
				t.Fatal(err)
			}
			allocations, err := reconcileReferenceAllocations(ctx, newReferenceCommandCursor(session))
			if err != nil {
				t.Fatal(err)
			}
			if len(allocations) != 2 {
				t.Fatalf("allocations=%+v", allocations)
			}
			if stream.ready.GetAcknowledgedInventoryWatermark() != 3 || len(stream.commands) != test.commands {
				t.Fatalf("ready=%+v commands=%d", stream.ready, len(stream.commands))
			}
			for index, command := range stream.commands {
				if command.GetSequence() != uint64(index+1) || command.GetConnectionEpoch() != 7 || command.GetCommandId() == "" {
					t.Fatalf("command[%d]=%+v", index, command)
				}
			}
		})
	}
}

func TestScanSecretLocationsCoversProcessAndFilesystemSurfaces(t *testing.T) {
	root := t.TempDir()
	process := filepath.Join(root, "environ")
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(process, []byte("SAFE=value"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret := []byte("bootstrap-secret")
	if err := scanSecretLocations(secret, []string{process}, []string{state}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "appliance.log"), append([]byte("prefix:"), secret...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := scanSecretLocations(secret, []string{process}, []string{state}); err == nil {
		t.Fatal("filesystem secret was not detected")
	}
	if err := os.Remove(filepath.Join(state, "appliance.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(process, append([]byte("KEY="), secret...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := scanSecretLocations(secret, []string{process}, []string{state}); err == nil {
		t.Fatal("process metadata secret was not detected")
	}
}

func TestScanSecretLocationsSkipsFilesystemInaccessibleToPayload(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can traverse the inaccessible fixture")
	}
	root := t.TempDir()
	inaccessible := filepath.Join(root, "lost+found")
	if err := os.Mkdir(inaccessible, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(inaccessible, 0o700) })
	if err := scanSecretLocations([]byte("bootstrap-secret"), nil, []string{root}); err != nil {
		t.Fatal(err)
	}
}

func TestRequireCoreDumpsDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limits")
	if err := os.WriteFile(path, []byte("Max core file size        0                    0                    bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireCoreDumpsDisabled([]string{path}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("Max core file size        unlimited            unlimited            bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireCoreDumpsDisabled([]string{path}); err == nil {
		t.Fatal("enabled core dumps were accepted")
	}
}
