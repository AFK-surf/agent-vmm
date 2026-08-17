//go:build darwin

package host

import (
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestControllerDomainMetricsAreRegistrationScopedAndAccumulateAcrossReconnect(t *testing.T) {
	controller := &Controller{remoteLeases: make(map[string]*remoteLease), domainMetrics: make(map[string]*controllerDomainMetricState)}
	controller.recordDomainReconcile("registration-a", "connection-a", false)
	controller.recordDomainTransport("registration-a", "connection-a", &hostv1.ControllerDomainMetrics{BytesToController: 10, BytesToRuntime: 20, SendStalls: 2, WindowBlockedMillis: 3})
	controller.recordDomainReconcile("registration-a", "connection-b", true)
	controller.recordDomainTransport("registration-a", "connection-b", &hostv1.ControllerDomainMetrics{BytesToController: 4, BytesToRuntime: 5, SendStalls: 1, WindowBlockedMillis: 2})
	controller.recordDomainStopLatency("registration-a", 7*time.Millisecond)
	controller.remoteLeases["lease-a"] = &remoteLease{registrationID: "registration-a", state: hostv1.LeaseState_LEASE_STATE_ACTIVE, sessionID: "session-a"}
	controller.remoteLeases["lease-b"] = &remoteLease{registrationID: "registration-b", state: hostv1.LeaseState_LEASE_STATE_ACTIVE, sessionID: "session-b"}

	metrics := controller.controllerDomainMetrics("registration-a")
	if metrics.GetBytesToController() != 14 || metrics.GetBytesToRuntime() != 25 || metrics.GetSendStalls() != 3 || metrics.GetWindowBlockedMillis() != 5 ||
		metrics.GetConnectorRestarts() != 1 || metrics.GetReconciliations() != 2 || metrics.GetLastStopLatencyMillis() != 7 ||
		metrics.GetActiveLeases() != 1 || metrics.GetActiveSessions() != 1 || metrics.GetSampledAt() == nil {
		t.Fatalf("domain metrics=%+v", metrics)
	}
	other := controller.controllerDomainMetrics("registration-b")
	if other.GetBytesToController() != 0 || other.GetActiveLeases() != 1 || other.GetActiveSessions() != 1 {
		t.Fatalf("other domain metrics=%+v", other)
	}
}
