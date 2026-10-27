//go:build linux

package control

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

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
	generationID, err := volumeGenerationID(volumePath(namespaceID, volumeID))
	if err != nil {
		return nil, err
	}
	volume := &vmmdv1.Volume{Id: volumeID, NamespaceId: namespaceID, CurrentBytes: usage, LimitBytes: limit, GenerationId: generationID}
	for _, container := range containers {
		for _, mount := range container.GetVolumes() {
			if mount.GetOwnedVolume() == volumeID {
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
