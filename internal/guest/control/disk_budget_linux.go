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
	root := filepath.Join(stateRoot, "import-reservations")
	limit, err := activeBuilderQuotaLimit(root)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	var usageBefore uint64
	if limit != 0 {
		usageBefore, err = projectUsage(r.stateDevice, cacheProjectID)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
	}
	namespaceBefore, err := r.namespaceDiskCommitments(ctx, false)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	var before unix.Statfs_t
	if err := unix.Statfs(stateRoot, &before); err != nil {
		return guestdisk.Snapshot{}, err
	}
	imports, _, err := importReservationSummary(root)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	namespaceAfter, err := r.namespaceDiskCommitments(ctx, false)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	namespaceRemaining, err := namespaceRemainingCommitments(namespaceBefore, namespaceAfter)
	if err != nil {
		return guestdisk.Snapshot{}, err
	}
	stats := before
	var credit uint64
	if limit != 0 {
		usageAfter, err := projectUsage(r.stateDevice, cacheProjectID)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
		usage := min(usageBefore, usageAfter)
		if limit > usage {
			credit = limit - usage
		}
	}
	if err := unix.Statfs(stateRoot, &stats); err != nil {
		return guestdisk.Snapshot{}, err
	}
	used := max((before.Blocks-before.Bfree)*uint64(before.Bsize), (stats.Blocks-stats.Bfree)*uint64(stats.Bsize))
	available := min(before.Bavail*uint64(before.Bsize), stats.Bavail*uint64(stats.Bsize))
	capacity := used + available
	if imports > used {
		return guestdisk.Snapshot{}, errors.New("Guest import reservation exceeds physical usage")
	}
	reservations, carry := bits.Add64(imports, credit, 0)
	if carry != 0 {
		return guestdisk.Snapshot{}, errors.New("Guest writer reservation accounting overflow")
	}
	return guestdisk.Snapshot{Inodes: min(before.Files, stats.Files), FreeInodes: min(before.Ffree, stats.Ffree), CapacityBytes: capacity, UsedBytes: used - imports, ImportReservationBytes: reservations, NamespaceCommitmentBytes: namespaceRemaining,
		EmergencyBytes: guestdisk.DefaultEmergencyBytes, MetadataGrowthBytes: guestdisk.DefaultMetadataBytes, AllocationUnitBytes: uint64(stats.Bsize)}, nil
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
