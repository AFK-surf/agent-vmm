//go:build darwin || linux

package host

import (
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func withTestRemoteCapacity(controller *Controller, capacity store.Environment) *Controller {
	controller.remote = assembleRemote(controller, capacity.Resources())
	controller.remote.BindCleanup(controller.cleanup)
	controller.appliances.BindRemote(controller.remote)
	if controller.vmLifecycle != nil {
		controller.remote.BindLifecycle(controller.vmLifecycle)
	}
	controller.cleanup.BindRemote(controller.remote)
	return controller
}
func assertTestRemoteAdmissionAvailable(t *testing.T, controller *Controller) {
	t.Helper()
	released := make(chan struct{})
	go func() { release := controller.remote.LockAdmission(); release(); close(released) }()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("rejected control kept admission locked for other registrations")
	}
}
