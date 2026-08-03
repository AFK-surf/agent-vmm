package control

import vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"

func componentStatus(name string, err error, observedAt int64) *vmmdv1.ComponentStatus {
	status := &vmmdv1.ComponentStatus{Component: name, State: vmmdv1.ComponentState_COMPONENT_STATE_HEALTHY, ObservedAtUnixNano: observedAt}
	if err != nil {
		status.State = vmmdv1.ComponentState_COMPONENT_STATE_UNAVAILABLE
		status.Reason = err.Error()
	}
	return status
}
