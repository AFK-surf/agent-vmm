//go:build linux

package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const cniPath = "/usr/libexec/cni"

type cniResult struct {
	Interfaces []struct {
		Name    string `json:"name"`
		Sandbox string `json:"sandbox"`
	} `json:"interfaces"`
	IPs []struct {
		Address string `json:"address"`
	} `json:"ips"`
}

func bridgeName(namespaceID string) string {
	hash := sha256.Sum256([]byte(namespaceID))
	return fmt.Sprintf("avm%08x", hash[:4])
}

func endpointNetNS(namespaceID, containerID string) string {
	hash := sha256.Sum256([]byte(namespaceID + "\x00" + containerID))
	return filepath.Join(netnsRoot, fmt.Sprintf("avm-%x", hash[:8]))
}

func cniContainerID(namespaceID, containerID string) string {
	hash := sha256.Sum256([]byte(namespaceID + "\x00" + containerID))
	return fmt.Sprintf("agent-vmm-%x", hash[:16])
}

func (m *Manager) Attach(ctx context.Context, namespaceID, containerID, instanceID string) (Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	namespace, ok := m.namespaces[namespaceID]
	if !ok {
		return Endpoint{}, errors.New("network namespace does not exist")
	}
	if endpoint, ok := m.endpoints[namespaceID][containerID]; ok && endpoint.InstanceID == instanceID {
		return endpoint, nil
	} else if ok {
		return Endpoint{}, errors.New("container network endpoint belongs to another instance")
	}
	path := endpointNetNS(namespaceID, containerID)
	if err := createNetNS(path); err != nil {
		return Endpoint{}, err
	}
	if err := runCNI(ctx, "ADD", "loopback", namespace, containerID, path); err != nil {
		_ = deleteNetNS(path)
		return Endpoint{}, err
	}
	output, err := runCNIOutput(ctx, "ADD", "bridge", namespace, containerID, path)
	if err != nil {
		_ = runCNI(context.Background(), "DEL", "loopback", namespace, containerID, path)
		_ = deleteNetNS(path)
		return Endpoint{}, err
	}
	var result cniResult
	if err := json.Unmarshal(output, &result); err != nil {
		m.detachCNI(ctx, namespace, containerID, path)
		return Endpoint{}, fmt.Errorf("decode CNI result: %w", err)
	}
	var address netip.Addr
	for _, item := range result.IPs {
		prefix, err := netip.ParsePrefix(item.Address)
		if err == nil && prefix.Addr().Is4() {
			address = prefix.Addr()
			break
		}
	}
	if !address.IsValid() || !namespace.Subnet.Contains(address) {
		m.detachCNI(ctx, namespace, containerID, path)
		return Endpoint{}, errors.New("CNI returned no address in namespace subnet")
	}
	var hostInterface string
	for _, item := range result.Interfaces {
		if item.Sandbox == "" && item.Name != namespace.Bridge {
			hostInterface = item.Name
			break
		}
	}
	if hostInterface == "" {
		m.detachCNI(ctx, namespace, containerID, path)
		return Endpoint{}, errors.New("CNI returned no host interface")
	}
	endpoint := Endpoint{NamespaceID: namespaceID, ContainerID: containerID, InstanceID: instanceID, HostInterface: hostInterface, Address: address, NetNSPath: path}
	hadDNS := m.dns[namespaceID] != nil
	if err := m.startDNSLocked(namespaceID); err != nil {
		m.detachCNI(ctx, namespace, containerID, path)
		return Endpoint{}, err
	}
	if err := m.mutateLocked(func(next *state) error { next.endpoints[namespaceID][containerID] = endpoint; return nil }); err != nil {
		if m.endpoints[namespaceID][containerID].InstanceID == instanceID {
			return endpoint, err
		}
		if !hadDNS {
			m.dns[namespaceID].close(namespaceID)
			delete(m.dns, namespaceID)
		}
		m.detachCNI(ctx, namespace, containerID, path)
		return Endpoint{}, err
	}
	return endpoint, nil
}

func (m *Manager) Detach(ctx context.Context, namespaceID, containerID, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	endpoint, ok := m.endpoints[namespaceID][containerID]
	if !ok || endpoint.InstanceID != instanceID {
		return nil
	}
	namespace := m.namespaces[namespaceID]
	ruleErr := m.mutateLocked(func(next *state) error { delete(next.endpoints[namespaceID], containerID); return nil })
	if _, retained := m.endpoints[namespaceID][containerID]; retained {
		return ruleErr
	}
	if counters, err := interfaceCounters(endpoint.HostInterface, true); err == nil {
		total := m.settled[namespaceID]
		total.RXBytes += counters.RXBytes
		total.TXBytes += counters.TXBytes
		m.settled[namespaceID] = total
	} else {
		m.settledOK[namespaceID] = false
	}
	m.closeForwardsLocked(namespaceID + "/" + containerID)
	cleanupContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return errors.Join(ruleErr, m.detachCNI(cleanupContext, namespace, containerID, endpoint.NetNSPath))
}

func (m *Manager) closeForwardsLocked(key string) {
	for connection := range m.forwards[key] {
		if tracked, ok := connection.(*trackedConn); ok {
			_ = tracked.Conn.Close()
		} else {
			_ = connection.Close()
		}
	}
	delete(m.forwards, key)
}

func (m *Manager) AttachLoopback(ctx context.Context, namespaceID, id string) (string, error) {
	path := endpointNetNS(namespaceID, "loopback-"+id)
	if err := createNetNS(path); err != nil {
		return "", err
	}
	namespace := Namespace{ID: namespaceID + "-loopback"}
	if err := runCNI(ctx, "ADD", "loopback", namespace, id, path); err != nil {
		_ = deleteNetNS(path)
		return "", err
	}
	return path, nil
}

func (m *Manager) DetachLoopback(ctx context.Context, namespaceID, id, netNSPath string) error {
	namespace := Namespace{ID: namespaceID + "-loopback"}
	err := runCNI(ctx, "DEL", "loopback", namespace, id, netNSPath)
	removeErr := deleteNetNS(netNSPath)
	if err != nil {
		return err
	}
	return removeErr
}

func (m *Manager) detachCNI(ctx context.Context, namespace Namespace, containerID, path string) error {
	var result error
	if err := runCNI(ctx, "DEL", "bridge", namespace, containerID, path); err != nil {
		result = err
	}
	if err := runCNI(ctx, "DEL", "loopback", namespace, containerID, path); err != nil && result == nil {
		result = err
	}
	if err := deleteNetNS(path); err != nil && result == nil {
		result = err
	}
	return result
}

func runCNI(ctx context.Context, command, plugin string, namespace Namespace, containerID, path string) error {
	_, err := runCNIOutput(ctx, command, plugin, namespace, containerID, path)
	return err
}

func runCNIOutput(ctx context.Context, command, plugin string, namespace Namespace, containerID, path string) ([]byte, error) {
	config := map[string]any{"cniVersion": "1.0.0", "name": namespace.ID, "type": plugin}
	if plugin == "bridge" {
		config["bridge"] = namespace.Bridge
		config["isGateway"] = true
		config["ipMasq"] = false
		config["ipam"] = map[string]any{
			"type": "host-local", "ranges": [][]map[string]string{{{"subnet": namespace.Subnet.String(), "gateway": namespace.Gateway.String()}}},
			"routes": []map[string]string{{"dst": "0.0.0.0/0"}},
		}
	}
	input, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	process := exec.CommandContext(ctx, filepath.Join(cniPath, plugin))
	process.Env = []string{
		"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "CNI_COMMAND=" + command,
		"CNI_CONTAINERID=" + cniContainerID(namespace.ID, containerID), "CNI_NETNS=" + path,
		"CNI_IFNAME=eth0", "CNI_PATH=" + cniPath,
	}
	process.Stdin = bytes.NewReader(input)
	output, err := process.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("CNI %s %s: %w: %s", plugin, command, err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func (m *Manager) CleanupStaleEndpoint(namespaceID, containerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	namespace, ok := m.namespaces[namespaceID]
	if !ok {
		return errors.New("network namespace does not exist")
	}
	path := endpointNetNS(namespaceID, containerID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runCNI(ctx, "DEL", "bridge", namespace, containerID, path)
	loopbackErr := runCNI(ctx, "DEL", "loopback", namespace, containerID, path)
	netnsErr := deleteNetNS(path)
	if err != nil {
		return err
	}
	if loopbackErr != nil {
		return loopbackErr
	}
	return netnsErr
}

func createNetNS(path string) (result error) {
	if err := os.MkdirAll(netnsRoot, 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	file.Close()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := os.Open(fmt.Sprintf("/proc/self/task/%d/ns/net", unix.Gettid()))
	if err != nil {
		os.Remove(path)
		return err
	}
	defer original.Close()
	defer func() {
		if err := unix.Setns(int(original.Fd()), unix.CLONE_NEWNET); err != nil && result == nil {
			result = err
		}
		if result != nil {
			_ = unix.Unmount(path, unix.MNT_DETACH)
			_ = os.Remove(path)
		}
	}()
	if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
		return err
	}
	source := fmt.Sprintf("/proc/self/task/%d/ns/net", unix.Gettid())
	return unix.Mount(source, path, "", unix.MS_BIND, "")
}

func deleteNetNS(path string) error {
	err := unix.Unmount(path, unix.MNT_DETACH)
	if err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		return err
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
