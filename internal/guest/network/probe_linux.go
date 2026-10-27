//go:build linux

package network

import (
	"context"
	"net"
	"net/netip"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

type ProbeResult struct {
	PolicyEnforced bool
	DNSOK          bool
	TCPOK          bool
	PrivateDenied  bool
	FailureLayer   string
	Reason         string
}

func (manager *Manager) ProbeEgress(ctx context.Context, namespaceID string) ProbeResult {
	manager.mu.RLock()
	namespace, ok := manager.namespaces[namespaceID]
	manager.mu.RUnlock()
	if !ok {
		return ProbeResult{FailureLayer: "source_policy", Reason: "namespace is not projected"}
	}
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: 3 * time.Second, LocalAddr: &net.UDPAddr{IP: net.IP(namespace.Gateway.AsSlice())}}
		return dialer.DialContext(ctx, "udp4", publicResolver)
	}}
	addresses, dnsErr := resolver.LookupNetIP(probeCtx, "ip4", "example.com")
	if namespace.EgressMode == vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL {
		if dnsErr == nil && len(addresses) != 0 {
			return ProbeResult{FailureLayer: "guest_policy_dns", Reason: "deny-all namespace resolved a public name"}
		}
		return ProbeResult{PolicyEnforced: true, PrivateDenied: true}
	}
	if dnsErr != nil || len(addresses) == 0 {
		return ProbeResult{FailureLayer: "guest_policy_dns", Reason: "public DNS probe failed"}
	}
	result := ProbeResult{DNSOK: true}
	var target netip.Addr
	for _, address := range addresses {
		if address.Unmap().Is4() {
			target = address.Unmap()
			break
		}
	}
	if !target.IsValid() {
		result.FailureLayer, result.Reason = "host_dns", "public DNS returned no IPv4 address"
		return result
	}
	dialer := net.Dialer{Timeout: 3 * time.Second, LocalAddr: &net.TCPAddr{IP: net.IP(namespace.Gateway.AsSlice())}}
	connection, err := dialer.DialContext(probeCtx, "tcp4", net.JoinHostPort(target.String(), "443"))
	if err != nil {
		result.FailureLayer, result.Reason = "host_dial", "public TCP probe failed"
		return result
	}
	_ = connection.Close()
	result.TCPOK = true
	privateCtx, privateCancel := context.WithTimeout(probeCtx, 500*time.Millisecond)
	defer privateCancel()
	private, err := dialer.DialContext(privateCtx, "tcp4", "192.168.1.1:443")
	if err == nil {
		_ = private.Close()
		result.FailureLayer, result.Reason = "source_policy", "private destination probe was not denied"
		return result
	}
	result.PolicyEnforced = true
	result.PrivateDenied = true
	return result
}
