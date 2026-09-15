//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"golang.org/x/sys/unix"
)

const maxImportReservationDirectoryEntries = 64

func (r *Runtime) guestDiskSnapshot(ctx context.Context) (guestdisk.Snapshot, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(stateRoot, &stats); err != nil {
		return guestdisk.Snapshot{}, err
	}
	used := (stats.Blocks - stats.Bfree) * uint64(stats.Bsize)
	capacity := used + stats.Bavail*uint64(stats.Bsize)
	imports, _, err := importReservationSummary(filepath.Join(stateRoot, "import-reservations"))
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	if imports > used {
		return guestdisk.Snapshot{}, errors.New("Guest import reservation exceeds physical usage")
	}
	return guestdisk.Snapshot{Inodes: stats.Files, FreeInodes: stats.Ffree, CapacityBytes: capacity, UsedBytes: used - imports, ImportReservationBytes: imports,
		EmergencyBytes: guestdisk.DefaultEmergencyBytes, MetadataGrowthBytes: guestdisk.DefaultMetadataBytes, AllocationUnitBytes: uint64(stats.Bsize)}, nil
}

func (r *Runtime) diskAdmissionSnapshot(ctx context.Context, cold bool, transaction *guestdisk.Transaction) (guestdisk.Snapshot, error) {
	snapshot, err := r.guestDiskSnapshot(ctx)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	if snapshot.Pressure() >= guestdisk.PressureGC {
		if _, err := r.reclaimOCIWithMaterializerTransaction(ctx, maxGCDeletes, false, transaction); err != nil {
			return guestdisk.Snapshot{}, err
		}
		snapshot, err = r.guestDiskSnapshot(ctx)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
	}
	if snapshot.Pressure() == guestdisk.PressureCritical || cold && snapshot.Pressure() >= guestdisk.PressureRejectCold {
		return snapshot, guestdisk.ErrCapacityUnavailable
	}
	return snapshot, nil
}

func importReservationSummary(directory string) (uint64, uint32, error) {
	handle, err := os.Open(directory)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer handle.Close()
	entries, err := handle.ReadDir(maxImportReservationDirectoryEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, 0, err
	}
	if len(entries) > maxImportReservationDirectoryEntries {
		return 0, 0, errors.New("Guest import reservation directory exceeds its observation bound")
	}
	var total uint64
	var count uint32
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".hold" {
			continue
		}
		if count == ^uint32(0) {
			return 0, 0, errors.New("Guest import reservation count overflow")
		}
		count++
		info, err := entry.Info()
		if err != nil {
			return 0, 0, err
		}
		value := uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
		var carry uint64
		total, carry = bits.Add64(total, value, 0)
		if carry != 0 {
			return 0, 0, errors.New("Guest import reservation accounting overflow")
		}
	}
	return total, count, nil
}
