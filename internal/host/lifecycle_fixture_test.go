//go:build darwin || linux

package host

import (
	"context"
	"sync"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type lifecycleFixtureManager struct {
	*vmm.Manager
	snapshot func() *hostv1.VM
}

func (manager *lifecycleFixtureManager) Snapshot() *hostv1.VM {
	if manager.snapshot != nil {
		return manager.snapshot()
	}
	return manager.Manager.Snapshot()
}
func (manager *lifecycleFixtureManager) DialBoot(string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
}

var lifecycleFixtureManagers sync.Map

type lifecycleFixtureRuntime struct{}

func (lifecycleFixtureRuntime) RecoverWorkloadRuns(context.Context) error         { return nil }
func (lifecycleFixtureRuntime) ResetWorkloadRecovery()                            {}
func (lifecycleFixtureRuntime) StartEgress() error                                { return nil }
func (lifecycleFixtureRuntime) StopEgress()                                       {}
func (lifecycleFixtureRuntime) ReconcilePolicies(context.Context) error           { return nil }
func (lifecycleFixtureRuntime) ReconcileExternalConnectors(context.Context) error { return nil }
func (lifecycleFixtureRuntime) StopAllExternalConnectors()                        {}
func (lifecycleFixtureRuntime) RevokeAllSessions(context.Context)                 {}
func (lifecycleFixtureRuntime) InvalidateAppliances()                             {}
func (lifecycleFixtureRuntime) PrepareHostShares(context.Context) error           { return nil }
func newVMLifecycleOwner(controller *Controller, manager *vmm.Manager) *hostlifecycle.Owner {
	if manager == nil {
		manager = &vmm.Manager{}
	}
	wrapper := &lifecycleFixtureManager{Manager: manager}
	runtime := lifecycleFixtureRuntime{}
	owner := hostlifecycle.New(hostlifecycle.Config{Manager: wrapper, Store: controller.store, Guest: &guestconn.Connection{}, Inventory: &controller.inventory, Workloads: runtime, Services: runtime, Remote: runtime, Sessions: runtime, Appliances: runtime, HostShares: runtime})
	lifecycleFixtureManagers.Store(owner, wrapper)
	controller.remote.BindLifecycle(owner)
	controller.appliances.BindLifecycle(owner)
	return owner
}
func setTestLifecycleSnapshot(controller *Controller, snapshot func() *hostv1.VM) {
	value, ok := lifecycleFixtureManagers.Load(controller.vmLifecycle)
	if !ok {
		controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
		value, _ = lifecycleFixtureManagers.Load(controller.vmLifecycle)
	}
	value.(*lifecycleFixtureManager).snapshot = snapshot
	controller.remote.BindLifecycle(controller.vmLifecycle)
	controller.appliances.BindLifecycle(controller.vmLifecycle)
}
func admitTestLifecycle(t *testing.T, controller *Controller) {
	t.Helper()
	setTestLifecycleSnapshot(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "test-boot"} })
	if err := controller.vmLifecycle.Run(t.Context(), hostlifecycle.Start, nil, controller.vmLifecycle.StartOwned); err != nil {
		t.Fatal(err)
	}
}
func closedTestLifecycle(t *testing.T, controller *Controller) *hostlifecycle.Owner {
	t.Helper()
	owner := newVMLifecycleOwner(controller, &vmm.Manager{})
	if err := owner.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	return owner
}
func holdTestLifecycle(t *testing.T, owner *hostlifecycle.Owner) func() {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- owner.Run(context.Background(), hostlifecycle.Appliance, nil, func(context.Context) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("hold lifecycle: %v", err)
	}
	return func() {
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("release lifecycle: %v", err)
		}
	}
}

func holdTestInventory(t *testing.T, controller *Controller) func() {
	t.Helper()
	release, err := controller.inventory.Transition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return release
}
func enterTestInventory(t *testing.T, controller *Controller) func() {
	t.Helper()
	_, release, err := controller.inventory.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return release
}
