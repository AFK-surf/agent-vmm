//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

func builderReservationHeader(identity string) *vmmdv1.ImportImageHeader {
	sum := sha256.Sum256([]byte("builder\x00" + identity))
	return &vmmdv1.ImportImageHeader{RequestId: "oci-growth-" + hex.EncodeToString(sum[:24]), ManifestDigest: "sha256:" + hex.EncodeToString(sum[:])}
}

func builderReservationPath(identity string) string {
	header := builderReservationHeader(identity)
	sum := sha256.Sum256([]byte(header.ManifestDigest + "\x00" + header.RequestId))
	return filepath.Join(stateRoot, "import-reservations", hex.EncodeToString(sum[:]))
}

func activeBuilderQuotaLimit(root string) (uint64, error) {
	entries, err := directoryEntriesBounded(root, maxImportReservationDirectoryEntries+1)
	if err != nil {
		return 0, err
	}
	if len(entries) > maxImportReservationDirectoryEntries {
		return 0, errors.New("Guest import reservation directory exceeds its observation bound")
	}
	var limit uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".writer") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			return 0, err
		}
		value, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return 0, err
		}
		limit = max(limit, value)
	}
	return limit, nil
}

func (r *Runtime) guardBuilderCacheGrowth(ctx context.Context, identity string, run func() error) (runErr error) {
	err := r.materializationSlot.Acquire(ctx, 1)
	unlock := func() { r.materializationSlot.Release(1) }
	if err != nil {
		return err
	}
	defer unlock()
	reservation, err := r.reserveImport(ctx, builderReservationHeader(identity))
	if err != nil {
		return err
	}
	credit := reservation.path + ".writer"
	err = r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		allowance, err := reservation.AllocatedBytes()
		if err != nil {
			return err
		}
		usage, err := projectUsage(r.stateDevice, cacheProjectID)
		if err != nil {
			return err
		}
		limit, err := ociProjectQuotaLimit(usage, allowance)
		if err != nil {
			return err
		}
		return transferBuilderReservation(
			func() error { return persistBuilderQuota(credit, limit) },
			func() error { return setProjectQuota(r.stateDevice, cacheProjectID, limit) },
			func() error { return reservation.Consume(allowance) },
		)
	})
	if err != nil {
		settleErr := r.settleBuilderCacheReservation(identity)
		reservation.Release()
		return errors.Join(err, settleErr)
	}
	runErr = run()
	var unsettled *guestbuild.WriterUnsettledError
	if errors.As(runErr, &unsettled) {
		return runErr
	}
	return errors.Join(runErr, r.settleBuilderCacheReservation(identity))
}

func (r *Runtime) settleBuilderCacheReservation(identity string) error {
	path := builderReservationPath(identity)
	if _, err := os.Stat(path + ".writer"); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return r.diskBudget.Settle(func() error {
		if err := os.Remove(path + ".writer"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := removeImportReservation(path, path+".hold"); err != nil {
			return err
		}
		return r.settleOCIProjectQuota()
	})
}

func transferBuilderReservation(persistCredit, openQuota, releaseHold func() error) error {
	if err := persistCredit(); err != nil {
		return err
	}
	if err := openQuota(); err != nil {
		return err
	}
	return releaseHold()
}

func persistBuilderQuota(path string, limit uint64) error {
	return atomicfile.PublishOnce(path, []byte(strconv.FormatUint(limit, 10)), 0600)
}

func tightenCacheProjectQuota(device string) error {
	usage, err := projectUsage(device, cacheProjectID)
	if err != nil {
		return err
	}
	limit, err := activeBuilderQuotaLimit(filepath.Join(stateRoot, "import-reservations"))
	if err != nil {
		return err
	}
	if limit == 0 {
		limit = max(usage, uint64(1024))
	}
	return setProjectQuota(device, cacheProjectID, limit)
}
