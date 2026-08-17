//go:build darwin

package host

import (
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type controllerDomainMetricState struct {
	bytesToController, bytesToRuntime, sendStalls, windowBlockedMillis uint64
	connectorRestarts, reconciliations, lastStopLatencyMillis          uint64
	connectionInstance                                                 string
	lastBytesToController, lastBytesToRuntime                          uint64
	lastSendStalls, lastWindowBlockedMillis                            uint64
	sampledAt                                                          time.Time
}

func (controller *Controller) domainMetricState(registrationID string) *controllerDomainMetricState {
	if controller.domainMetrics == nil {
		controller.domainMetrics = make(map[string]*controllerDomainMetricState)
	}
	state := controller.domainMetrics[registrationID]
	if state == nil {
		state = new(controllerDomainMetricState)
		controller.domainMetrics[registrationID] = state
	}
	return state
}

func (controller *Controller) recordDomainReconcile(registrationID, connectionInstance string, restarted bool) {
	controller.domainMetricsMu.Lock()
	defer controller.domainMetricsMu.Unlock()
	state := controller.domainMetricState(registrationID)
	state.reconciliations++
	if restarted {
		state.connectorRestarts++
	}
	state.connectionInstance = connectionInstance
	state.lastBytesToController, state.lastBytesToRuntime = 0, 0
	state.lastSendStalls, state.lastWindowBlockedMillis = 0, 0
	state.sampledAt = time.Now()
}

func (controller *Controller) recordDomainTransport(registrationID, connectionInstance string, metrics *hostv1.ControllerDomainMetrics) {
	if metrics == nil {
		return
	}
	controller.domainMetricsMu.Lock()
	defer controller.domainMetricsMu.Unlock()
	state := controller.domainMetricState(registrationID)
	if state.connectionInstance != connectionInstance {
		state.connectionInstance = connectionInstance
		state.lastBytesToController, state.lastBytesToRuntime = 0, 0
		state.lastSendStalls, state.lastWindowBlockedMillis = 0, 0
	}
	state.bytesToController += counterDelta(metrics.GetBytesToController(), state.lastBytesToController)
	state.bytesToRuntime += counterDelta(metrics.GetBytesToRuntime(), state.lastBytesToRuntime)
	state.sendStalls += counterDelta(metrics.GetSendStalls(), state.lastSendStalls)
	state.windowBlockedMillis += counterDelta(metrics.GetWindowBlockedMillis(), state.lastWindowBlockedMillis)
	state.lastBytesToController, state.lastBytesToRuntime = metrics.GetBytesToController(), metrics.GetBytesToRuntime()
	state.lastSendStalls, state.lastWindowBlockedMillis = metrics.GetSendStalls(), metrics.GetWindowBlockedMillis()
	state.sampledAt = time.Now()
}

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

func (controller *Controller) recordDomainStopLatency(registrationID string, elapsed time.Duration) {
	controller.domainMetricsMu.Lock()
	defer controller.domainMetricsMu.Unlock()
	state := controller.domainMetricState(registrationID)
	state.lastStopLatencyMillis = uint64(max(elapsed/time.Millisecond, 0))
	state.sampledAt = time.Now()
}

func (controller *Controller) controllerDomainMetrics(registrationID string) *hostv1.ControllerDomainMetrics {
	controller.domainMetricsMu.Lock()
	state := controller.domainMetricState(registrationID)
	result := &hostv1.ControllerDomainMetrics{
		BytesToController: state.bytesToController, BytesToRuntime: state.bytesToRuntime,
		SendStalls: state.sendStalls, WindowBlockedMillis: state.windowBlockedMillis,
		ConnectorRestarts: state.connectorRestarts, Reconciliations: state.reconciliations,
		LastStopLatencyMillis: state.lastStopLatencyMillis,
	}
	if !state.sampledAt.IsZero() {
		result.SampledAt = timestamppb.New(state.sampledAt)
	}
	controller.domainMetricsMu.Unlock()

	controller.mu.Lock()
	for _, lease := range controller.remoteLeases {
		if lease.registrationID != registrationID || lease.state != hostv1.LeaseState_LEASE_STATE_ACTIVE {
			continue
		}
		result.ActiveLeases++
		if lease.sessionID != "" {
			result.ActiveSessions++
		}
	}
	controller.mu.Unlock()
	return result
}
