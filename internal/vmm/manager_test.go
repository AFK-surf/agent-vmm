//go:build darwin || linux

package vmm

import (
	"context"
	"errors"
	"strings"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type lifecycleMachine struct {
	machineBackend
	state  hostv1.VMState
	states chan hostv1.VMState
}

func (machine *lifecycleMachine) State() hostv1.VMState { return machine.state }

func (machine *lifecycleMachine) StateChangedNotify() <-chan hostv1.VMState { return machine.states }

func TestUnexpectedStopFencesAdmissionAndHandsEveryStopToRecovery(t *testing.T) {
	machine := new(lifecycleMachine)
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "boot-one", machine: machine}
	fences := 0
	manager.admissionFence = func() {
		fences++
		if manager.state != hostv1.VMState_VM_STATE_RUNNING || manager.bootID != "boot-one" {
			t.Fatal("admission fence ran after terminal state was published")
		}
	}
	manager.invalidate = func() {}
	var recovered []string
	manager.recover = func(previousBootID string) { recovered = append(recovered, previousBootID) }
	_, recover, bootID := manager.terminalTransition(machine, hostv1.VMState_VM_STATE_STOPPED, true)
	if manager.state != hostv1.VMState_VM_STATE_STOPPED || manager.bootID != "" || recover == nil || fences != 1 {
		t.Fatalf("first transition = state %s boot %q fences %d", manager.state, manager.bootID, fences)
	}
	recover(bootID)
	manager.state, manager.bootID = hostv1.VMState_VM_STATE_RUNNING, "boot-two"
	machine = new(lifecycleMachine)
	manager.machine = machine
	manager.admissionFence = nil
	_, recover, bootID = manager.terminalTransition(machine, hostv1.VMState_VM_STATE_FAILED, true)
	if recover == nil || manager.state != hostv1.VMState_VM_STATE_FAILED || manager.bootID != "" {
		t.Fatalf("second transition = %+v", manager.snapshotLocked())
	}
	recover(bootID)
	if len(recovered) != 2 || recovered[0] != "boot-one" || recovered[1] != "boot-two" {
		t.Fatalf("recovered boots = %v", recovered)
	}
}

func TestExpectedStopDoesNotRecover(t *testing.T) {
	machine := new(lifecycleMachine)
	manager := &Manager{state: hostv1.VMState_VM_STATE_STOPPING, bootID: "boot", machine: machine, recover: func(string) {}}
	_, recover, _ := manager.terminalTransition(machine, hostv1.VMState_VM_STATE_STOPPED, true)
	if recover != nil || manager.state != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("expected stop = state %s recover %t", manager.state, recover != nil)
	}
}

func TestFailedVMWithMachineIsStillAttached(t *testing.T) {
	manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, machine: new(lifecycleMachine)}
	if !manager.HasMachineAttachment() {
		t.Fatal("FAILED VM attachment was treated as terminal")
	}
	manager.machine = nil
	if manager.HasMachineAttachment() {
		t.Fatal("released VM remained attached")
	}
}

func TestStaleMachineNotificationCannotInvalidateCurrentRuntime(t *testing.T) {
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "current", machine: new(lifecycleMachine)}
	invalidate, recover, _ := manager.terminalTransition(new(lifecycleMachine), hostv1.VMState_VM_STATE_STOPPED, true)
	if invalidate != nil || recover != nil || manager.state != hostv1.VMState_VM_STATE_RUNNING || manager.bootID != "current" {
		t.Fatalf("stale notification changed current runtime: %+v", manager.snapshotLocked())
	}
}

func TestRecoveryFailureRequiresOperatorAction(t *testing.T) {
	manager := &Manager{state: hostv1.VMState_VM_STATE_STOPPED}
	manager.FailRecovery(errors.New("policy replay failed"))
	if manager.state != hostv1.VMState_VM_STATE_FAILED || !strings.Contains(manager.lastErr, "operator action required") || !strings.Contains(manager.lastErr, "policy replay failed") {
		t.Fatalf("recovery failure = %+v", manager.snapshotLocked())
	}
}

func TestClosedStateNotificationFailsClosed(t *testing.T) {
	machine := new(lifecycleMachine)
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "boot", machine: machine}
	invalidated := false
	manager.invalidate = func() { invalidated = true }
	states := make(chan hostv1.VMState)
	close(states)
	manager.observeStateChanges(machine, states)
	if !invalidated || manager.state != hostv1.VMState_VM_STATE_FAILED || manager.bootID != "" {
		t.Fatalf("closed notification channel did not fail closed: %+v invalidated=%t", manager.snapshotLocked(), invalidated)
	}
}

func TestStopFailedVMUsesExactMachineFact(t *testing.T) {
	for _, force := range []bool{false, true} {
		manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, lastErr: "guest startup failed", bootID: "stale-boot"}
		vm, err := manager.Stop(context.Background(), force)
		if err != nil || vm.GetState() != hostv1.VMState_VM_STATE_STOPPED || vm.GetBootId() != "" || vm.GetError() != "guest startup failed" {
			t.Fatalf("force=%t vm=%v err=%v", force, vm, err)
		}
	}
	machine := &lifecycleMachine{state: hostv1.VMState_VM_STATE_RUNNING, states: make(chan hostv1.VMState, 1)}
	manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, machine: machine, lastErr: "failed with retained instance"}
	if _, err := manager.Stop(context.Background(), false); err == nil || manager.machine != machine || manager.state != hostv1.VMState_VM_STATE_FAILED || manager.lastErr != "failed with retained instance" {
		t.Fatal("retained failed instance was treated as stopped")
	}
	machine.state = hostv1.VMState_VM_STATE_FAILED
	machine.states <- hostv1.VMState_VM_STATE_FAILED
	manager.observeStateChanges(machine, machine.states)
	if manager.HasMachineAttachment() || manager.state != hostv1.VMState_VM_STATE_FAILED || manager.lastErr != "failed with retained instance" {
		t.Fatalf("ended retained instance = %+v attached=%t", manager.snapshotLocked(), manager.HasMachineAttachment())
	}
	if vm, err := manager.Stop(context.Background(), true); err != nil || vm.GetState() != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatalf("force stop after retained instance ended = %v, %v", vm, err)
	}
}

// A start that fails before it claims the retained VMM keeps that VMM reachable
// by a forced Stop; the retained session ending during a start does not decide it.
func TestStartKeepsRetainedMachineUntilItsSessionEnds(t *testing.T) {
	retained := &lifecycleMachine{state: hostv1.VMState_VM_STATE_RUNNING, states: make(chan hostv1.VMState, 1)}
	manager := &Manager{state: hostv1.VMState_VM_STATE_FAILED, machine: retained, lastErr: "failed with retained instance"}
	fenced, invalidated := false, false
	manager.admissionFence = func() { fenced = true }
	manager.invalidate = func() { invalidated = true }
	if _, err := manager.Start(context.Background()); err == nil {
		t.Fatal("start without a bundle succeeded")
	}
	if !manager.HasMachineAttachment() {
		t.Fatal("early start failure dropped the retained VMM")
	}
	manager.state = hostv1.VMState_VM_STATE_STARTING
	retained.states <- hostv1.VMState_VM_STATE_FAILED
	manager.observeStateChanges(retained, retained.states)
	if fenced || invalidated || manager.HasMachineAttachment() || manager.state != hostv1.VMState_VM_STATE_STARTING {
		t.Fatalf("retained machine acted on a new start: fenced=%t invalidated=%t vm=%+v", fenced, invalidated, manager.snapshotLocked())
	}
}

func TestWaitStateClosedNotificationUsesCurrentMachineState(t *testing.T) {
	for _, state := range []hostv1.VMState{hostv1.VMState_VM_STATE_STOPPED, hostv1.VMState_VM_STATE_RUNNING} {
		states := make(chan hostv1.VMState)
		close(states)
		machine := &lifecycleMachine{state: state, states: states}
		err := waitState(context.Background(), machine, hostv1.VMState_VM_STATE_STOPPED)
		if (err == nil) != (state == hostv1.VMState_VM_STATE_STOPPED) {
			t.Fatalf("state %s: %v", state, err)
		}
	}
}
