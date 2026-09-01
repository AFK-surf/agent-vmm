//go:build linux

package control

import (
	"fmt"
	"os"
	"path/filepath"

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
	return applyResourceCgroup(namespaceID, resourcev2.Contract{CPUMaxMillis: uint64(limits.GetCpuMillis()), CPUWeight: 100,
		MemoryHighBytes: limits.GetMemoryBytes(), MemoryMaxBytes: limits.GetMemoryBytes(), PIDMax: uint64(limits.GetPids())})
}

func applyWorkloadCgroup(namespaceID string, resources *vmmdv1.WorkloadResourcesV2) error {
	return applyResourceCgroup(namespaceID, resourcev2.Contract{CPUGuaranteeMillis: uint64(resources.GetCpuGuaranteeMillis()),
		CPUMaxMillis: uint64(resources.GetCpuMaxMillis()), CPUWeight: uint64(resources.GetCpuWeight()),
		MemoryHighBytes: resources.GetMemoryHighBytes(), MemoryMaxBytes: resources.GetMemoryMaxBytes(), PIDMax: uint64(resources.GetPidMax()),
		WritableQuotaBytes: resources.GetWritableQuotaBytes()})
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
	path := filepath.Join(root, namespaceID)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	for name, value := range contract.CgroupValues() {
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0o644); err != nil {
			return fmt.Errorf("write cgroup %s: %w", name, err)
		}
	}
	return nil
}

func enableControllers(path string) error {
	return os.WriteFile(filepath.Join(path, "cgroup.subtree_control"), []byte("+cpu +memory +pids"), 0o644)
}
