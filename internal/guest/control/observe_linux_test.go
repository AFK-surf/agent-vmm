//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
)

type namespaceUsageStore struct {
	namespaces.Store
	labelsCalls int
	listCalls   int
	labels      map[string]string
	err         error
}

func (store *namespaceUsageStore) Labels(context.Context, string) (map[string]string, error) {
	store.labelsCalls++
	return store.labels, store.err
}

func (store *namespaceUsageStore) List(context.Context) ([]string, error) {
	store.listCalls++
	return nil, errors.New("namespace inventory must not be listed")
}

func TestNamespaceUsageResolvesOneNamespaceWithoutInventoryScan(t *testing.T) {
	store := &namespaceUsageStore{labels: namespaceLabels(&vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 1 << 30, Pids: 128, DiskBytes: 4 << 30}, vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, 10001, "10.64.1.0/24")}
	namespace, err := namespaceForUsage(context.Background(), store, "environment-a")
	if err != nil {
		t.Fatal(err)
	}
	if store.labelsCalls != 1 || store.listCalls != 0 || namespace.GetProjectId() != 10001 {
		t.Fatalf("labels=%d list=%d namespace=%+v", store.labelsCalls, store.listCalls, namespace)
	}
}

func TestNamespaceUsageRejectsIncompletePointRead(t *testing.T) {
	store := &namespaceUsageStore{labels: map[string]string{labelPrefix + "project-id": "10001"}}
	if _, err := namespaceForUsage(context.Background(), store, "environment-a"); err == nil {
		t.Fatal("incomplete namespace labels were accepted")
	}
	if store.labelsCalls != 1 || store.listCalls != 0 {
		t.Fatalf("labels=%d list=%d", store.labelsCalls, store.listCalls)
	}
}

func TestMissingImageMaterializationIsSkippedByImageListing(t *testing.T) {
	if !unmaterializedImage(errdefs.ErrNotFound) {
		t.Fatal("containerd not-found errors must identify an unmaterialized image")
	}
	if unmaterializedImage(errors.New("content store is unavailable")) {
		t.Fatal("non-not-found image errors must still fail the list")
	}
}

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

func TestNamespaceStateDeviceSeparatesSystemUsage(t *testing.T) {
	runtime := &Runtime{stateDevice: "workload-device", controlDevice: "control-device"}
	if got := runtime.namespaceStateDevice("system.appliance"); got != "control-device" {
		t.Fatalf("system namespace device = %q", got)
	}
	if got := runtime.namespaceStateDevice("workload-a"); got != "workload-device" {
		t.Fatalf("workload namespace device = %q", got)
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

func TestBoundedDirectoryInventoryStopsAtRequestedCount(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := directoryEntriesBounded(root, 3)
	if err != nil || len(entries) != 3 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
}

func TestBoundedDirectoryInventoryAcceptsFewerEntriesThanRequested(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "only"), 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := directoryEntriesBounded(root, 3)
	if err != nil || len(entries) != 1 || entries[0].Name() != "only" {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestVolumeGenerationFencesSameNameRecreation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "volumes", "namespace-a", "volume-a")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := initializeVolumeDirectory(path, 0o700, nil, nil, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	first, err := volumeGenerationID(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(first, "legacy-") {
		t.Fatalf("new volume used legacy generation %q", first)
	}
	if err := removeVolumeDirectory(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(volumeGenerationMarkerPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted volume retained generation marker: %v", err)
	}
	if err := initializeVolumeDirectory(path, 0o700, nil, nil, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	second, err := volumeGenerationID(path)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("same-name recreation kept generation %q", first)
	}
	if err := requireVolumeGeneration(path, first); err == nil {
		t.Fatal("delete generation fence accepted the recreated volume")
	}
	if err := requireVolumeGeneration(path, second); err != nil {
		t.Fatalf("current volume generation rejected: %v", err)
	}
}

func TestExistingVolumeGenerationUsesStableLegacyFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "volumes", "namespace-a", "volume-a")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := volumeGenerationID(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := volumeGenerationID(path)
	if err != nil || first == "" || second != first {
		t.Fatalf("generation = %q then %q, err=%v", first, second, err)
	}
	if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
		t.Fatalf("generation metadata leaked into workload volume: entries=%v err=%v", entries, err)
	}
	if _, err := os.Stat(volumeGenerationMarkerPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy observation wrote generation metadata: %v", err)
	}
}
