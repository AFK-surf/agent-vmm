//go:build linux

package control

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadNetworkUsage(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "rx_bytes"), []byte("1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "tx_bytes"), []byte("5678\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	usage, err := readNetworkUsage(directory)
	if err != nil {
		t.Fatal(err)
	}
	if usage.GetNetworkRxBytes() != 1234 || usage.GetNetworkTxBytes() != 5678 || usage.GetSampledAtUnixNano() == 0 {
		t.Fatalf("readNetworkUsage() = %+v", usage)
	}
}

func TestReadSystemUsageIncludesControlFilesystem(t *testing.T) {
	directory := t.TempDir()
	for name, value := range map[string]string{
		"cpu.stat": "usage_usec 12\n", "memory.current": "4096\n", "pids.current": "3\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(directory, "rx_bytes"), []byte("1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "tx_bytes"), []byte("5678\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	usage, err := readSystemUsage(directory, directory, directory)
	if err != nil {
		t.Fatal(err)
	}
	if usage.GetNetworkRxBytes() != 1234 || usage.GetNetworkTxBytes() != 5678 || usage.GetDiskBytes() == 0 ||
		usage.GetMemoryBytes() != 4096 || usage.GetPids() != 3 || usage.GetCpuNanos() != 12_000 {
		t.Fatalf("system usage = %+v", usage)
	}
}

func TestUnavailableUsageDoesNotExposePaths(t *testing.T) {
	usage := unavailableUsage(&os.PathError{Op: "read", Path: "/sys/fs/cgroup/agent-secret", Err: os.ErrPermission})
	if usage.GetUnavailableReason() != "usage source is not readable" {
		t.Fatalf("reason = %q", usage.GetUnavailableReason())
	}
	usage = unavailableUsage(errors.New("namespace customer-secret quota failed"))
	if usage.GetUnavailableReason() != "usage could not be collected" {
		t.Fatalf("reason = %q", usage.GetUnavailableReason())
	}
}
