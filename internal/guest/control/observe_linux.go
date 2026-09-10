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
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
)

func (r *Runtime) GetNamespace(ctx context.Context, namespaceID string) (*vmmdv1.Namespace, error) {
	return r.getNamespaceForRole(ctx, namespaceID, false)
}

func (r *Runtime) getNamespaceForRole(ctx context.Context, namespaceID string, system bool) (*vmmdv1.Namespace, error) {
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
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
		if name == runtimeContainerdNamespace || isSystemNamespace(name) {
			continue
		}
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
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	return r.volumeSnapshot(namespaceID, volumeID, namespace.GetLimits().GetDiskBytes(), containers)
}

type volumeUsageEntry struct {
	bytes      uint64
	observedAt time.Time
	err        error
}

const volumeUsageTTL = 30 * time.Second

func (r *Runtime) volumeSnapshot(namespaceID, volumeID string, limit uint64, containers []*vmmdv1.Container) (*vmmdv1.Volume, error) {
	usage, err := r.cachedVolumeUsage(namespaceID, volumeID)
	if err != nil {
		return nil, err
	}
	volume := &vmmdv1.Volume{Id: volumeID, NamespaceId: namespaceID, CurrentBytes: usage, LimitBytes: limit}
	for _, container := range containers {
		for _, mount := range container.GetVolumes() {
			if mount.GetVolumeId() == volumeID {
				volume.MountedBy = append(volume.MountedBy, container.GetId())
			}
		}
	}
	return volume, nil
}

func (r *Runtime) cachedVolumeUsage(namespaceID, volumeID string) (uint64, error) {
	key := namespaceID + "/" + volumeID
	now := time.Now()
	r.volumeUsageMu.Lock()
	entry, ok := r.volumeUsage[key]
	r.volumeUsageMu.Unlock()
	if ok && now.Sub(entry.observedAt) < volumeUsageTTL {
		return entry.bytes, nil
	}
	usage, err := r.sampleVolumeUsage(volumePath(namespaceID, volumeID))
	if err != nil {
		return 0, err
	}
	r.volumeUsageMu.Lock()
	r.volumeUsage[key] = volumeUsageEntry{bytes: usage, observedAt: now}
	r.volumeUsageMu.Unlock()
	return usage, nil
}

func (r *Runtime) cachedOrScheduleVolumeUsage(namespaceID, volumeID string) volumeUsageEntry {
	key := namespaceID + "/" + volumeID
	now := time.Now()
	r.volumeUsageMu.Lock()
	entry, ok := r.volumeUsage[key]
	if !r.volumeSampling[key] && (!ok || now.Sub(entry.observedAt) >= volumeUsageTTL) {
		if r.volumeSampleSlots == nil {
			r.volumeSampleSlots = make(chan struct{}, 2)
		}
		select {
		case r.volumeSampleSlots <- struct{}{}:
			r.volumeSampling[key] = true
			generation := r.volumeGeneration[key]
			go func() {
				usage, err := r.sampleVolumeUsage(volumePath(namespaceID, volumeID))
				r.volumeUsageMu.Lock()
				if r.volumeGeneration[key] == generation {
					r.volumeUsage[key] = volumeUsageEntry{bytes: usage, observedAt: time.Now(), err: err}
					delete(r.volumeSampling, key)
				}
				r.volumeUsageMu.Unlock()
				<-r.volumeSampleSlots
			}()
		default:
		}
	}
	r.volumeUsageMu.Unlock()
	return entry
}

func (r *Runtime) sampleVolumeUsage(path string) (uint64, error) {
	if r.volumeSampler != nil {
		return r.volumeSampler(path)
	}
	return directoryUsage(path)
}

func (r *Runtime) invalidateVolumeUsage(namespaceID, volumeID string) {
	r.volumeUsageMu.Lock()
	delete(r.volumeUsage, namespaceID+"/"+volumeID)
	key := namespaceID + "/" + volumeID
	delete(r.volumeSampling, key)
	r.volumeGeneration[key]++
	r.volumeUsageMu.Unlock()
}

func directoryUsage(root string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Blocks > 0 {
			total += uint64(stat.Blocks) * 512
		}
		return nil
	})
	return total, err
}

func (r *Runtime) ListVolumes(ctx context.Context, namespaceID string) ([]*vmmdv1.Volume, error) {
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
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	entries, err := directoryEntries(volumePath(namespaceID, ""))
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Volume, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		usage := r.cachedOrScheduleVolumeUsage(namespaceID, entry.Name())
		volume := &vmmdv1.Volume{Id: entry.Name(), NamespaceId: namespaceID, CurrentBytes: usage.bytes, LimitBytes: namespace.GetLimits().GetDiskBytes()}
		if usage.observedAt.IsZero() {
			volume.UsageStale = true
			volume.UsageUnavailableReason = "volume usage is being sampled"
		} else {
			volume.UsageObservedAtUnixNano = usage.observedAt.UnixNano()
			volume.UsageStale = time.Since(usage.observedAt) >= volumeUsageTTL
			if usage.err != nil {
				volume.UsageStale = true
				volume.UsageUnavailableReason = usageFailureReason(usage.err)
			}
		}
		for _, container := range containers {
			for _, mount := range container.GetVolumes() {
				if mount.GetVolumeId() == entry.Name() {
					volume.MountedBy = append(volume.MountedBy, container.GetId())
				}
			}
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
	logicalContext := namespaceContext(ctx, namespaceID)
	images, err := r.client.ListImages(logicalContext)
	if err != nil {
		return nil, err
	}
	result := make([]*vmmdv1.Image, 0, len(images))
	for _, image := range images {
		runtimeImage, err := r.client.GetImage(runtimeContext(ctx), runtimeImageReference(image.Target().Digest))
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		unpacked, err := runtimeImage.IsUnpacked(runtimeContext(ctx), namespaceSnapshotter(namespaceID))
		if err != nil {
			// A cancelled or interrupted import can leave containerd image
			// metadata behind after its content or snapshot has disappeared.
			// Such a record is not a usable image and must not prevent the
			// authoritative importer from observing it as absent and retrying.
			if unmaterializedImage(err) {
				continue
			}
			return nil, err
		}
		if !unpacked {
			continue
		}
		size, err := runtimeImage.Size(runtimeContext(ctx))
		if err != nil {
			return nil, err
		}
		metadata, err := r.client.ImageService().Get(logicalContext, image.Name())
		if err != nil {
			return nil, err
		}
		result = append(result, &vmmdv1.Image{Reference: image.Name(), Digest: image.Target().Digest.String(), LogicalBytes: uint64(size), Platforms: []string{"linux/arm64"}, GenerationId: imageGenerationID(metadata)})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].GetReference() < result[right].GetReference() })
	return result, nil
}

func unmaterializedImage(err error) bool {
	return errdefs.IsNotFound(err)
}

func (r *Runtime) GetImage(ctx context.Context, namespaceID, reference string) (*vmmdv1.Image, error) {
	return r.getImageForRole(ctx, namespaceID, reference, false)
}

func (r *Runtime) getImageForRole(ctx context.Context, namespaceID, reference string, system bool) (*vmmdv1.Image, error) {
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return nil, err
	}
	canonical, err := normalizeImageReference(reference)
	if err != nil {
		return nil, imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", reference)
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	logical, err := r.client.GetImage(logicalContext, canonical)
	if err != nil {
		return nil, err
	}
	nsctx := runtimeContext(ctx)
	image, err := r.client.GetImage(nsctx, runtimeImageReference(logical.Target().Digest))
	if err != nil {
		return nil, err
	}
	if logical.Target().Digest != image.Target().Digest {
		return nil, imageError(codes.FailedPrecondition, "IMAGE_ROOTFS_UNAVAILABLE", "image grant does not match materialized root filesystem", canonical)
	}
	unpacked, err := image.IsUnpacked(nsctx, namespaceSnapshotter(namespaceID))
	if err != nil {
		return nil, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image unpack state could not be read", canonical)
	}
	if !unpacked {
		return nil, imageError(codes.FailedPrecondition, "IMAGE_ROOTFS_UNAVAILABLE", "image root filesystem is not materialized; import the exact image again", canonical)
	}
	size, err := image.Size(nsctx)
	if err != nil {
		return nil, err
	}
	metadata, err := r.client.ImageService().Get(logicalContext, canonical)
	if err != nil {
		return nil, err
	}
	return &vmmdv1.Image{Reference: canonical, Digest: image.Target().Digest.String(), LogicalBytes: uint64(size), Platforms: []string{"linux/arm64"}, GenerationId: imageGenerationID(metadata)}, nil
}

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
	return readSystemUsage(filepath.Join("/sys/class/net", netprotocol.TunName, "statistics"), controlRoot)
}

func readSystemUsage(networkPath, controlPath string) (*vmmdv1.Usage, error) {
	usage, err := readNetworkUsage(networkPath)
	if err != nil {
		return nil, err
	}
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

func workloadPoolUsage(path string) (*vmmdv1.Usage, uint64, float64, uint64, error) {
	usage, err := readCgroupUsage(path)
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("workload pool usage: %w", err)
	}
	limit, err := uintFile(filepath.Join(path, "memory.max"))
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("workload pool memory limit: %w", err)
	}
	events, err := keyValueFile(filepath.Join(path, "memory.events"))
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("workload pool memory events: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(path, "memory.pressure"))
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("workload pool memory pressure: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "full" {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "avg10="); ok {
				pressure, err := strconv.ParseFloat(value, 64)
				return usage, limit, pressure, events["oom_kill"], err
			}
		}
	}
	return nil, 0, 0, 0, errors.New("workload memory pressure is unavailable")
}
