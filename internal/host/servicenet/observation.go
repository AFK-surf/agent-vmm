//go:build darwin || linux

package servicenet

import (
	"fmt"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (controller *Owner) HostEgressHealth(vm *hostv1.VM, observedAt *timestamppb.Timestamp) *hostv1.ComponentHealth {
	active, bootID, revision, sources := controller.egress.Status()
	metrics := controller.egress.Metrics()
	state := hostv1.HealthState_HEALTH_STATE_HEALTHY
	reason := fmt.Sprintf("policy_revision=%d sources=%d packets_rx=%d packets_tx=%d drops=%d rejected=%d dial_failures=%d active_tcp=%d active_udp=%d active_dns=%d last_error=%q last_dial_error=%q", revision, sources, metrics.PacketsReceived, metrics.PacketsSent, metrics.PacketDrops, metrics.RejectedFlows, metrics.DialFailures, metrics.ActiveTCP, metrics.ActiveUDP, metrics.ActiveDNS, metrics.LastError, metrics.LastDialError)
	if !active || bootID != vm.GetBootId() || revision == 0 {
		state = hostv1.HealthState_HEALTH_STATE_DEGRADED
		reason = "egress session or source policy is not ready"
	}
	return &hostv1.ComponentHealth{Component: "host-egress", State: state, Reason: reason, ObservedAt: observedAt}
}
