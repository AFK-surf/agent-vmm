//go:build linux

package network

import (
	"fmt"
	"net"
	"net/netip"
	"testing"
)

func TestRetainedEndpointUsesExistingInterfaceAndOwner(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil || len(interfaces) == 0 {
		t.Fatalf("interfaces: %v", err)
	}
	host := interfaces[0]
	namespace := Namespace{ID: "owner", Subnet: netip.MustParsePrefix("10.40.0.0/24")}
	for _, address := range []string{"10.40.0.2", "10.50.0.2"} {
		data := []byte(fmt.Sprintf(`[{"link_index":%d,"addr_info":[{"family":"inet","local":%q}]}]`, host.Index, address))
		endpoint, err := retainedEndpoint(namespace, "payload", "original-instance", "/run/netns/original", data)
		if address == "10.50.0.2" {
			if err == nil {
				t.Fatal("endpoint outside owner subnet accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if endpoint.NamespaceID != namespace.ID || endpoint.ContainerID != "payload" || endpoint.InstanceID != "original-instance" || endpoint.HostInterface != host.Name || endpoint.NetNSPath != "/run/netns/original" || endpoint.Address.String() != address {
			t.Fatalf("retained endpoint changed: %+v", endpoint)
		}
	}
}
