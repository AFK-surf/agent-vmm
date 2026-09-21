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
		"cpu.max": "max 100000", "cpu.weight": "100",
		"memory.high": "max", "memory.max": "max", "pids.max": "512",
	} {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", "workload", "workload", name))
		if err != nil || string(data) != expected {
			t.Fatalf("%s=%q want=%q err=%v", name, data, expected, err)
		}
	}
}

func TestResourcePoolsShareVMComputeAndKeepChildPIDLimits(t *testing.T) {
	root := t.TempDir()
	if err := initializeResourcePoolsAt(root, 8000, 12<<30); err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"workload-a", "system.appliance-a"} {
		if err := applyResourceCgroupAt(root, namespace, resourcev2.Contract{CPUMaxMillis: 2000, MemoryMaxBytes: 2 << 30, PIDMax: 512}); err != nil {
			t.Fatal(err)
		}
	}
	for name, expected := range map[string]string{"cpu.max": "max 100000", "memory.max": "12884901888", "pids.max": "max", "cgroup.subtree_control": "+cpu +memory +pids"} {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", "workload", name))
		if err != nil || string(data) != expected {
			t.Fatalf("pool %s = %q err=%v", name, data, err)
		}
	}
	for name, expected := range map[string]string{"cpu.max": "max 100000", "memory.max": "12884901888", "pids.max": "max", "cgroup.subtree_control": "+cpu +memory +pids"} {
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

func TestWorkloadPoolPressureUsesLocalOOMAndFullChargedMemory(t *testing.T) {
	root := t.TempDir()
	for name, contents := range map[string]string{
		"cpu.stat": "usage_usec 1\n", "memory.current": "950", "memory.max": "1000",
		"memory.stat": "anon 100\nfile 850\nshmem 800\n", "pids.current": "3",
		"memory.events": "oom 17\noom_kill 16\n", "memory.events.local": "oom 0\noom_kill 0\n",
		"memory.pressure": "some avg10=20.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=11.00 avg60=0.00 avg300=0.00 total=0\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	usage, limit, pressure, kills, oom, err := workloadPoolUsage(root)
	if err != nil || usage.GetMemoryBytes() != 950 || limit != 1000 || pressure != 11 || kills != 0 || oom != 0 {
		t.Fatalf("child OOM or shmem/file misaccounted: usage=%v limit=%d psi=%f kills=%d oom=%d err=%v", usage, limit, pressure, kills, oom, err)
	}
	if err := os.WriteFile(filepath.Join(root, "memory.events.local"), []byte("oom 2\noom_kill 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, _, _, kills, oom, err = workloadPoolUsage(root)
	if err != nil || kills != 1 || oom != 2 {
		t.Fatalf("local OOM counters: kills=%d oom=%d err=%v", kills, oom, err)
	}
}
