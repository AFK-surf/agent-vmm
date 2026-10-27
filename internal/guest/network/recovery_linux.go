//go:build linux

package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"path/filepath"
)

func (m *Manager) RestoreEndpoint(ctx context.Context, namespaceID, containerID, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	namespace := m.namespaces[namespaceID]
	path := endpointNetNS(namespaceID, containerID)
	output, err := exec.CommandContext(ctx, "ip", "-n", filepath.Base(path), "-j", "address", "show", "dev", "eth0").Output()
	if err != nil {
		return fmt.Errorf("read retained endpoint %s: %w", path, err)
	}
	endpoint, err := retainedEndpoint(namespace, containerID, instanceID, path, output)
	if err != nil {
		return err
	}
	if err := m.startDNSLocked(namespaceID); err != nil {
		return err
	}
	return m.mutateLocked(func(next *state) error { next.endpoints[namespaceID][containerID] = endpoint; return nil })
}

func retainedEndpoint(namespace Namespace, containerID, instanceID, path string, data []byte) (Endpoint, error) {
	var interfaces []struct {
		LinkIndex int `json:"link_index"`
		Addresses []struct {
			Family string `json:"family"`
			Local  string `json:"local"`
		} `json:"addr_info"`
	}
	if err := json.Unmarshal(data, &interfaces); err != nil {
		return Endpoint{}, err
	}
	for _, link := range interfaces {
		host, err := net.InterfaceByIndex(link.LinkIndex)
		if err != nil {
			return Endpoint{}, err
		}
		for _, address := range link.Addresses {
			ip, err := netip.ParseAddr(address.Local)
			if address.Family == "inet" && err == nil && namespace.Subnet.Contains(ip) {
				return Endpoint{NamespaceID: namespace.ID, ContainerID: containerID, InstanceID: instanceID, HostInterface: host.Name, Address: ip, NetNSPath: path}, nil
			}
		}
	}
	return Endpoint{}, fmt.Errorf("retained endpoint has no address in namespace %s", namespace.ID)
}
