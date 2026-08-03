//go:build linux

package control

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
)

func assignProject(path string, projectID uint32) error {
	return guestquota.Assign(path, projectID)
}

func setProjectQuota(device string, projectID uint32, bytes uint64) error {
	return guestquota.SetLimit(device, projectID, bytes)
}

func projectUsage(device string, projectID uint32) (uint64, error) {
	return guestquota.Usage(device, projectID)
}

func applyNamespaceCgroup(namespaceID string, limits *vmmdv1.NamespaceLimits) error {
	root := "/sys/fs/cgroup/agent-vmm"
	if err := enableControllers("/sys/fs/cgroup"); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := enableControllers(root); err != nil {
		return err
	}
	path := filepath.Join(root, namespaceID)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	values := map[string]string{
		"cpu.max":    strconv.FormatUint(uint64(limits.GetCpuMillis())*100, 10) + " 100000",
		"memory.max": strconv.FormatUint(limits.GetMemoryBytes(), 10),
		"pids.max":   strconv.FormatUint(uint64(limits.GetPids()), 10),
	}
	for name, value := range values {
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0o644); err != nil {
			return fmt.Errorf("write cgroup %s: %w", name, err)
		}
	}
	return nil
}

func enableControllers(path string) error {
	return os.WriteFile(filepath.Join(path, "cgroup.subtree_control"), []byte("+cpu +memory +pids"), 0o644)
}
