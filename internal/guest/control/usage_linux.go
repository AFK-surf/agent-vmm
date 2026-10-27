//go:build linux

package control

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

func (r *Runtime) NamespaceUsage(ctx context.Context, namespaceID string) (*vmmdv1.Usage, error) {
	namespace, err := namespaceForUsage(ctx, r.client.NamespaceService(), namespaceID)
	if err != nil {
		return nil, err
	}
	usage, err := readCgroupUsage(filepath.Join("/sys/fs/cgroup", resourcev2.NamespaceCgroup(namespaceID)))
	if err != nil {
		return unavailableUsage(err), nil
	}
	disk, err := projectUsage(r.namespaceStateDevice(namespaceID), namespace.GetProjectId())
	if err != nil {
		return unavailableUsage(err), nil
	}
	usage.DiskBytes = disk
	counters, err := r.network.NamespaceCounters(namespaceID)
	if err != nil {
		return unavailableUsage(err), nil
	}
	usage.NetworkRxBytes = counters.RXBytes
	usage.NetworkTxBytes = counters.TXBytes
	return usage, nil
}

func namespaceForUsage(ctx context.Context, store namespaces.Store, namespaceID string) (*vmmdv1.Namespace, error) {
	if err := validateID("namespace_id", namespaceID); err != nil {
		return nil, err
	}
	labels, err := store.Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	return namespace, err
}

func (r *Runtime) ContainerUsage(ctx context.Context, namespaceID, containerID string) (*vmmdv1.Usage, error) {
	container, err := r.InspectContainer(ctx, namespaceID, containerID)
	if err != nil {
		return nil, err
	}
	usage, err := readCgroupUsage(filepath.Join("/sys/fs/cgroup", resourcev2.NamespaceCgroup(namespaceID), containerID))
	if err != nil {
		return unavailableUsage(err), nil
	}
	counters, err := r.network.ContainerCounters(namespaceID, containerID)
	if err == nil {
		usage.NetworkRxBytes = counters.RXBytes
		usage.NetworkTxBytes = counters.TXBytes
	} else if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return unavailableUsage(err), nil
	}
	return usage, nil
}

func (r *Runtime) SystemUsage() (*vmmdv1.Usage, error) {
	return readSystemUsage(filepath.Join("/sys/class/net", egresslink.TunName, "statistics"), controlRoot, "/sys/fs/cgroup")
}

func readSystemUsage(networkPath, controlPath, cgroupPath string) (*vmmdv1.Usage, error) {
	usage, err := readCgroupUsage(cgroupPath)
	if err != nil {
		return nil, err
	}
	network, err := readNetworkUsage(networkPath)
	if err != nil {
		return nil, err
	}
	usage.NetworkRxBytes = network.NetworkRxBytes
	usage.NetworkTxBytes = network.NetworkTxBytes
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(controlPath, &filesystem); err != nil {
		return nil, err
	}
	if filesystem.Blocks < filesystem.Bfree || filesystem.Bsize <= 0 {
		return nil, errors.New("control filesystem usage is invalid")
	}
	usage.DiskBytes = (filesystem.Blocks - filesystem.Bfree) * uint64(filesystem.Bsize)
	return usage, nil
}

func readNetworkUsage(path string) (*vmmdv1.Usage, error) {
	rxBytes, err := uintFile(filepath.Join(path, "rx_bytes"))
	if err != nil {
		return nil, err
	}
	txBytes, err := uintFile(filepath.Join(path, "tx_bytes"))
	if err != nil {
		return nil, err
	}
	return &vmmdv1.Usage{NetworkRxBytes: rxBytes, NetworkTxBytes: txBytes, SampledAtUnixNano: time.Now().UnixNano()}, nil
}

func readCgroupUsage(path string) (*vmmdv1.Usage, error) {
	usage := &vmmdv1.Usage{SampledAtUnixNano: time.Now().UnixNano()}
	cpu, err := keyValueFile(filepath.Join(path, "cpu.stat"))
	if err != nil {
		return nil, err
	}
	usage.CpuNanos = cpu["usage_usec"] * 1000
	if usage.MemoryBytes, err = uintFile(filepath.Join(path, "memory.current")); err != nil {
		return nil, err
	}
	pids, err := uintFile(filepath.Join(path, "pids.current"))
	if err != nil {
		return nil, err
	}
	usage.Pids = uint32(pids)
	ioValues, err := keyValueFile(filepath.Join(path, "io.stat"))
	if err == nil {
		usage.ReadBytes = ioValues["rbytes"]
		usage.WriteBytes = ioValues["wbytes"]
	}
	return usage, nil
}

func uintFile(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

func keyValueFile(path string) (map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := make(map[string]uint64)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && !strings.Contains(fields[0], "=") && !strings.Contains(fields[1], "=") {
			value, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr == nil {
				values[fields[0]] += value
			}
			continue
		}
		for _, field := range fields {
			key, raw, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			value, parseErr := strconv.ParseUint(raw, 10, 64)
			if parseErr == nil {
				values[key] += value
			}
		}
	}
	return values, scanner.Err()
}

func unavailableUsage(err error) *vmmdv1.Usage {
	return &vmmdv1.Usage{SampledAtUnixNano: time.Now().UnixNano(), Unavailable: true, UnavailableReason: usageFailureReason(err)}
}

func usageFailureReason(err error) string {
	for _, reason := range []string{"workload pool usage", "workload pool memory limit", "workload pool memory events", "workload pool memory pressure"} {
		if strings.Contains(err.Error(), reason) {
			return reason + " is unavailable"
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return "usage source is not available"
	}
	if errors.Is(err, os.ErrPermission) {
		return "usage source is not readable"
	}
	return "usage could not be collected"
}

func workloadPoolUsage(path string) (*vmmdv1.Usage, uint64, float64, uint64, uint64, error) {
	usage, err := readCgroupUsage(path)
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("workload pool usage: %w", err)
	}
	limit, err := uintFile(filepath.Join(path, "memory.max"))
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("workload pool memory limit: %w", err)
	}
	// memory.current includes runsc's shmem/file charges. The breakdown is
	// diagnostic only: subtracting file memory would hide sandbox pressure.
	stats, statErr := keyValueFile(filepath.Join(path, "memory.stat"))
	if limit > 0 && float64(usage.MemoryBytes)/float64(limit) >= .9 {
		slog.Default().Warn("workload memory pressure", "current", usage.MemoryBytes, "limit", limit, "anon", stats["anon"], "file", stats["file"], "shmem", stats["shmem"], "error", statErr)
	}
	events, err := keyValueFile(filepath.Join(path, "memory.events.local"))
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("workload pool memory events: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(path, "memory.pressure"))
	if err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("workload pool memory pressure: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "full" {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "avg10="); ok {
				pressure, err := strconv.ParseFloat(value, 64)
				return usage, limit, pressure, events["oom_kill"], events["oom"], err
			}
		}
	}
	return nil, 0, 0, 0, 0, errors.New("workload memory pressure is unavailable")
}
