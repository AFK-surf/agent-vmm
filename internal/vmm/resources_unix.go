//go:build darwin || linux

package vmm

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ResourceSnapshot reports facts owned by the Host and VMM processes and the
// state disk.
// Missing measurements stay absent and carry a bounded reason.
func (manager *Manager) ResourceSnapshot() *hostv1.LayerResources {
	pids := uint32(1)
	result := &hostv1.LayerResources{Pids: &pids, SampledAt: timestamppb.Now()}
	var unavailable []string
	memory, err := processMemoryBytes()
	childMemory, children, childErr := manager.childProcessResources()
	pids += children
	if err = errors.Join(err, childErr); err == nil {
		memory += childMemory
		result.MemoryBytes = &memory
	} else {
		unavailable = append(unavailable, err.Error())
	}
	if entries, err := os.ReadDir(processFDDirectory); err == nil && len(entries) <= int(^uint32(0)) {
		count := uint32(len(entries))
		result.OpenFds = &count
	} else {
		unavailable = append(unavailable, "host file descriptors")
	}
	statePath := filepath.Join(manager.config.InstanceDirectory, "state.raw")
	if info, err := os.Stat(statePath); err == nil && info.Mode().IsRegular() {
		used, allocatedErr := allocatedBytes(info)
		if allocatedErr == nil {
			committed := uint64(info.Size())
			reserve := uint64(hostEmergencyBytes)
			result.Disk = &hostv1.DiskAccounting{UsedBytes: &used, CommittedBytes: &committed, SystemReserveBytes: &reserve}
			var filesystem unix.Statfs_t
			if statErr := unix.Statfs(filepath.Dir(statePath), &filesystem); statErr == nil && filesystem.Bsize > 0 {
				capacity := filesystem.Blocks * uint64(filesystem.Bsize)
				result.Disk.CapacityBytes = &capacity
			} else {
				unavailable = append(unavailable, "host filesystem capacity")
			}
		} else {
			unavailable = append(unavailable, "host state disk allocation")
		}
	} else {
		unavailable = append(unavailable, "host state disk")
	}
	result.UnavailableReason = strings.Join(unavailable, ", ")
	return result
}
