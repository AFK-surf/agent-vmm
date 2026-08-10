//go:build linux

package control

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
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

func TestVolumeUsageSamplingIsBoundedAndDeduplicated(t *testing.T) {
	runtime := &Runtime{
		volumeUsage:       make(map[string]volumeUsageEntry),
		volumeSampling:    make(map[string]bool),
		volumeGeneration:  make(map[string]uint64),
		volumeSampleSlots: make(chan struct{}, 2),
	}
	started := make(chan struct{}, 10)
	release := make(chan struct{}, 2)
	var active atomic.Int32
	var peak atomic.Int32
	runtime.volumeSampler = func(string) (uint64, error) {
		current := active.Add(1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return 1, nil
	}

	for _, volumeID := range []string{"a", "a", "b", "c", "d"} {
		runtime.cachedOrScheduleVolumeUsage("namespace", volumeID)
	}
	<-started
	<-started
	select {
	case <-started:
		t.Fatal("more than two volume samplers started")
	case <-time.After(50 * time.Millisecond):
	}
	if peak.Load() != 2 {
		t.Fatalf("peak sampling concurrency = %d", peak.Load())
	}
	runtime.volumeUsageMu.Lock()
	if len(runtime.volumeSampling) != 2 {
		t.Fatalf("sampling keys = %d", len(runtime.volumeSampling))
	}
	runtime.volumeUsageMu.Unlock()
	release <- struct{}{}
	release <- struct{}{}
	waitForVolumeSamplingIdle(t, runtime)
}

func TestVolumeUsageGenerationRejectsStaleSample(t *testing.T) {
	runtime := &Runtime{
		volumeUsage:       make(map[string]volumeUsageEntry),
		volumeSampling:    make(map[string]bool),
		volumeGeneration:  make(map[string]uint64),
		volumeSampleSlots: make(chan struct{}, 2),
	}
	started := make(chan int, 2)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	var calls atomic.Int32
	runtime.volumeSampler = func(string) (uint64, error) {
		call := int(calls.Add(1))
		started <- call
		if call == 1 {
			<-firstRelease
			return 10, nil
		}
		<-secondRelease
		return 20, nil
	}

	runtime.cachedOrScheduleVolumeUsage("namespace", "volume")
	<-started
	runtime.invalidateVolumeUsage("namespace", "volume")
	runtime.cachedOrScheduleVolumeUsage("namespace", "volume")
	<-started
	close(secondRelease)
	waitForVolumeUsage(t, runtime, "namespace/volume", 20)
	close(firstRelease)
	waitForVolumeSamplingIdle(t, runtime)
	waitForVolumeUsage(t, runtime, "namespace/volume", 20)
}

func waitForVolumeSamplingIdle(t *testing.T, runtime *Runtime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(runtime.volumeSampleSlots) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("volume sampling did not finish")
}

func waitForVolumeUsage(t *testing.T, runtime *Runtime, key string, expected uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.volumeUsageMu.Lock()
		entry, ok := runtime.volumeUsage[key]
		runtime.volumeUsageMu.Unlock()
		if ok && entry.bytes == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("volume usage %q did not become %d", key, expected)
}
