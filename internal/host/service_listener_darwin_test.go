//go:build darwin

package host

import (
	"context"
	"net"
	"net/netip"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestListenServiceAddressEnforcesExposureBoundary(t *testing.T) {
	ctx := context.Background()
	loopback, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer loopback.Close()
	loopbackAddress := loopback.Addr().(*net.TCPAddr)
	if !loopbackAddress.AddrPort().Addr().IsLoopback() {
		t.Fatalf("device listener bound %s", loopbackAddress)
	}
	if _, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, "0.0.0.0", 0); err == nil {
		t.Fatal("device listener accepted wildcard exposure")
	}
	if _, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, "127.0.0.1", 0); err == nil {
		t.Fatal("LAN listener accepted loopback exposure")
	}
	if _, err := listenServiceAddress(ctx, hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, "0.0.0.0", 0); err == nil {
		t.Fatal("LAN listener accepted wildcard exposure")
	}
}

func TestListenServiceAddressAcceptsExplicitLocalPrivateInterface(t *testing.T) {
	address, ok := firstLocalPrivateAddress()
	if !ok {
		t.Skip("no local private interface is available")
	}
	listener, err := listenServiceAddress(context.Background(), hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN, address.String(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if actual := listener.Addr().(*net.TCPAddr).AddrPort().Addr().Unmap(); actual != address.Unmap() {
		t.Fatalf("listener bound %s, want %s", actual, address)
	}
}

func firstLocalPrivateAddress() (netip.Addr, bool) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return netip.Addr{}, false
	}
	for _, current := range interfaces {
		addresses, _ := current.Addrs()
		for _, address := range addresses {
			prefix, err := netip.ParsePrefix(address.String())
			if err == nil && prefix.Addr().IsPrivate() && !prefix.Addr().IsLoopback() {
				return prefix.Addr().Unmap(), true
			}
		}
	}
	return netip.Addr{}, false
}
