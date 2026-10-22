//go:build darwin || linux

package host

import (
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestControllerDomainMetricsAreRegistrationScopedAndAccumulateAcrossReconnect(t *testing.T) {
	controller := &Controller{sessions: make(map[string]*sessionServer), domainMetrics: make(map[string]*controllerDomainMetricState)}
	controller.recordDomainReconcile("registration-a", "connection-a", false)
	controller.recordDomainTransport("registration-a", "connection-a", &hostv1.ControllerDomainMetrics{BytesToController: 10, BytesToRuntime: 20, SendStalls: 2, WindowBlockedMillis: 3})
	controller.recordDomainReconcile("registration-a", "connection-b", true)
	controller.recordDomainTransport("registration-a", "connection-b", &hostv1.ControllerDomainMetrics{BytesToController: 4, BytesToRuntime: 5, SendStalls: 1, WindowBlockedMillis: 2})
	controller.recordDomainStopLatency("registration-a", 7*time.Millisecond)
	controller.sessions["session-a"] = &sessionServer{registrationID: "registration-a"}
	controller.sessions["session-b"] = &sessionServer{registrationID: "registration-b"}

	metrics := controller.controllerDomainMetrics("registration-a")
	if metrics.GetBytesToController() != 14 || metrics.GetBytesToRuntime() != 25 || metrics.GetSendStalls() != 3 || metrics.GetWindowBlockedMillis() != 5 ||
		metrics.GetConnectorRestarts() != 1 || metrics.GetReconciliations() != 2 || metrics.GetLastStopLatencyMillis() != 7 ||
		metrics.GetActiveSessions() != 1 || metrics.GetSampledAt() == nil {
		t.Fatalf("domain metrics=%+v", metrics)
	}
	other := controller.controllerDomainMetrics("registration-b")
	if other.GetBytesToController() != 0 || other.GetActiveSessions() != 1 {
		t.Fatalf("other domain metrics=%+v", other)
	}
}
