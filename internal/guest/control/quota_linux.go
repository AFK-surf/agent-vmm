//go:build linux

package control

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"runtime"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func assignProject(path string, projectID uint32) error {
	return guestquota.Assign(path, projectID)
}

func projectID(path string) (uint32, error) { return guestquota.ProjectID(path) }

func setProjectQuota(device string, projectID uint32, bytes uint64) error {
	return guestquota.SetLimit(device, projectID, bytes)
}

func projectUsage(device string, projectID uint32) (uint64, error) {
	return guestquota.Usage(device, projectID)
}

func applyNamespaceCgroup(namespaceID string, limits *vmmdv1.NamespaceLimits) error {
	return applyResourceCgroup(namespaceID, resourcev2.Contract{CPUMaxMillis: uint64(limits.GetCpuMillis()),
		MemoryMaxBytes: limits.GetMemoryBytes(), PIDMax: uint64(limits.GetPids())})
}

func applyResourceCgroup(namespaceID string, contract resourcev2.Contract) error {
	return applyResourceCgroupAt("/sys/fs/cgroup", namespaceID, contract)
}

func applyResourceCgroupAt(parent, namespaceID string, contract resourcev2.Contract) error {
	root := filepath.Join(parent, "agent-vmm")
	if err := enableControllers(parent); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := enableControllers(root); err != nil {
		return err
	}
	group := filepath.Dir(filepath.Join(parent, resourcev2.NamespaceCgroup(namespaceID)))
	if err := os.MkdirAll(group, 0o755); err != nil {
		return err
	}
	if err := enableControllers(group); err != nil {
		return err
	}
	path := filepath.Join(group, namespaceID)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	for name, value := range contract.CgroupValues() {
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0o644); err != nil {
			return fmt.Errorf("write cgroup %s: %w", name, err)
		}
	}
	return enableControllers(path)
}

func enableControllers(path string) error {
	return os.WriteFile(filepath.Join(path, "cgroup.subtree_control"), []byte("+cpu +memory +pids"), 0o644)
}

func initializeResourcePools() error {
	var memory unix.Sysinfo_t
	if err := unix.Sysinfo(&memory); err != nil {
		return err
	}
	return initializeResourcePoolsAt("/sys/fs/cgroup", uint64(runtime.NumCPU())*1000, memory.Totalram*uint64(memory.Unit))
}

func initializeResourcePoolsAt(parent string, cpuMillis, memoryBytes uint64) error {
	root := filepath.Join(parent, "agent-vmm")
	if err := enableControllers(parent); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := enableControllers(root); err != nil {
		return err
	}
	for _, group := range []string{"system", "workload"} {
		path := filepath.Join(root, group)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
		if err := enableControllers(path); err != nil {
			return err
		}
	}
	workload := resourcev2.Contract{CPUMaxMillis: max(uint64(500), cpuMillis-min(cpuMillis, uint64(1000))), MemoryMaxBytes: memoryBytes / 4 * 3, PIDMax: 2048}
	system := resourcev2.Contract{CPUMaxMillis: cpuMillis - workload.CPUMaxMillis, MemoryMaxBytes: memoryBytes - workload.MemoryMaxBytes, PIDMax: 2048}
	for pool, contract := range map[string]resourcev2.Contract{"system": system, "workload": workload} {
		for name, value := range contract.CgroupValues() {
			if err := os.WriteFile(filepath.Join(root, pool, name), []byte(value), 0o644); err != nil {
				return fmt.Errorf("write %s pool %s: %w", pool, name, err)
			}
		}
	}
	return nil
}
