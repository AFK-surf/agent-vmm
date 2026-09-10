//go:build linux

package control

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func TestCgroupContractWritesPlatformSharingAndHardIsolationFiles(t *testing.T) {
	root := t.TempDir()
	contract := resourcev2.Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512}
	if err := applyResourceCgroupAt(root, "workload", contract); err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]string{
		"cpu.max": "200000 100000", "cpu.weight": "100",
		"memory.high": "max", "memory.max": "2147483648", "pids.max": "512",
	} {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", "workload", "workload", name))
		if err != nil || string(data) != expected {
			t.Fatalf("%s=%q want=%q err=%v", name, data, expected, err)
		}
	}
}

func TestResourcePoolsBoundSystemAndWorkloadNamespaceChildren(t *testing.T) {
	root := t.TempDir()
	if err := initializeResourcePoolsAt(root, 8000, 12<<30); err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"workload-a", "system.appliance-a"} {
		if err := applyResourceCgroupAt(root, namespace, resourcev2.Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512}); err != nil {
			t.Fatal(err)
		}
	}
	for name, expected := range map[string]string{"cpu.max": "700000 100000", "memory.max": "9663676416", "pids.max": "2048", "cgroup.subtree_control": "+cpu +memory +pids"} {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", "workload", name))
		if err != nil || string(data) != expected {
			t.Fatalf("pool %s = %q err=%v", name, data, err)
		}
	}
	for name, expected := range map[string]string{"cpu.max": "100000 100000", "memory.max": "3221225472", "pids.max": "2048", "cgroup.subtree_control": "+cpu +memory +pids"} {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", "system", name))
		if err != nil || string(data) != expected {
			t.Fatalf("system pool %s = %q err=%v", name, data, err)
		}
	}
	for _, path := range []string{"workload/workload-a", "system/system.appliance-a"} {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", path, "cgroup.subtree_control"))
		if err != nil || string(data) != "+cpu +memory +pids" {
			t.Fatalf("namespace controllers %s = %q err=%v", path, data, err)
		}
	}
}
