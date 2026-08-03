//go:build linux

package control

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/errdefs"
)

func (r *Runtime) GetNamespace(ctx context.Context, namespaceID string) (*vmmdv1.Namespace, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	namespace.ObservedStatus = "ready"
	return namespace, nil
}

func (r *Runtime) ListNamespaces(ctx context.Context) ([]*vmmdv1.Namespace, error) {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	result := make([]*vmmdv1.Namespace, 0, len(names))
	for _, name := range names {
		namespace, err := r.GetNamespace(ctx, name)
		if err != nil {
			return nil, err
		}
		result = append(result, namespace)
	}
	return result, nil
}

func (r *Runtime) GetVolume(ctx context.Context, namespaceID, volumeID string) (*vmmdv1.Volume, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	info, err := os.Stat(volumePath(namespaceID, volumeID))
	if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
		return nil, errdefs.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	projectID, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	usage, err := projectUsage(r.stateDevice, projectID)
	if err != nil {
		return nil, err
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	volume := &vmmdv1.Volume{Id: volumeID, NamespaceId: namespaceID, CurrentBytes: usage, LimitBytes: namespace.GetLimits().GetDiskBytes()}
	for _, container := range containers {
		for _, mount := range container.GetVolumes() {
			if mount.GetVolumeId() == volumeID {
				volume.MountedBy = append(volume.MountedBy, container.GetId())
			}
		}
	}
	return volume, nil
}

func (r *Runtime) ListVolumes(ctx context.Context, namespaceID string) ([]*vmmdv1.Volume, error) {
	entries, err := directoryEntries(volumePath(namespaceID, ""))
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Volume, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		volume, err := r.GetVolume(ctx, namespaceID, entry.Name())
		if err != nil {
			return nil, err
		}
		result = append(result, volume)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].GetId() < result[right].GetId() })
	return result, nil
}

func (r *Runtime) ListImages(ctx context.Context, namespaceID string) ([]*vmmdv1.Image, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return nil, err
	}
	images, err := r.client.ListImages(namespaceContext(ctx, namespaceID))
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Image, 0, len(images))
	for _, image := range images {
		size, err := image.Size(ctx)
		if err != nil {
			return nil, err
		}
		result = append(result, &vmmdv1.Image{Reference: image.Name(), Digest: image.Target().Digest.String(), LogicalBytes: uint64(size)})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].GetReference() < result[right].GetReference() })
	return result, nil
}

func (r *Runtime) GetImage(ctx context.Context, namespaceID, reference string) (*vmmdv1.Image, error) {
	image, err := r.client.GetImage(namespaceContext(ctx, namespaceID), reference)
	if err != nil {
		return nil, err
	}
	size, err := image.Size(ctx)
	if err != nil {
		return nil, err
	}
	return &vmmdv1.Image{Reference: image.Name(), Digest: image.Target().Digest.String(), LogicalBytes: uint64(size)}, nil
}

func (r *Runtime) NamespaceUsage(ctx context.Context, namespaceID string) (*vmmdv1.Usage, error) {
	namespace, err := r.GetNamespace(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	usage, err := readCgroupUsage(filepath.Join("/sys/fs/cgroup/agent-vmm", namespaceID))
	if err != nil {
		return unavailableUsage(err), nil
	}
	disk, err := projectUsage(r.stateDevice, namespace.GetProjectId())
	if err != nil {
		return unavailableUsage(err), nil
	}
	usage.DiskBytes = disk
	return usage, nil
}

func (r *Runtime) ContainerUsage(ctx context.Context, namespaceID, containerID string) (*vmmdv1.Usage, error) {
	if _, err := r.InspectContainer(ctx, namespaceID, containerID); err != nil {
		return nil, err
	}
	usage, err := readCgroupUsage(filepath.Join("/sys/fs/cgroup/agent-vmm", namespaceID, containerID))
	if err != nil {
		return unavailableUsage(err), nil
	}
	return usage, nil
}

func (r *Runtime) SystemUsage() (*vmmdv1.Usage, error) {
	return readNetworkUsage("/sys/class/net/eth0/statistics")
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
	return &vmmdv1.Usage{SampledAtUnixNano: time.Now().UnixNano(), Unavailable: true, UnavailableReason: fmt.Sprintf("collect cgroup or quota usage: %v", err)}
}
