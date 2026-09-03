//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"github.com/containerd/containerd/v2/core/leases"
	"golang.org/x/sys/unix"
)

type importReservation struct {
	mu        sync.Mutex
	path      string
	holdPath  string
	remaining uint64
}

// PrepareOCIStorage establishes the kernel-enforced global OCI project before
// containerd starts, so its first content, snapshot, and metadata inode inherits
// the same hard boundary.
func PrepareOCIStorage(stateDevice string) error {
	bootstrapPath := filepath.Join(stateRoot, "containerd-bootstrap-reservation")
	// A crash before the previous bootstrap allowance was converted can leave
	// only its physical hold. Startup is the sole owner at this point, so remove
	// that exact stale reservation before deriving a new ceiling.
	if err := removeImportReservation(bootstrapPath, bootstrapPath+".hold"); err != nil {
		return fmt.Errorf("remove stale containerd bootstrap reservation: %w", err)
	}
	for _, root := range []string{containerdRoot, filepath.Join(stateRoot, "imports")} {
		if err := os.MkdirAll(root, 0o700); err != nil {
			return err
		}
		if err := assignOCIProjectTree(root, projectID, assignProject); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(containerdRoot)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return reserveFreshContainerdBootstrap(stateDevice)
	}
	return openOCIStartupMetadataAllowance(stateDevice)
}

func openOCIStartupMetadataAllowance(stateDevice string) error {
	usage, err := projectUsage(stateDevice, ociProjectID)
	if err != nil {
		return err
	}
	limit, err := ociProjectQuotaLimit(usage, guestdisk.DefaultMetadataBytes)
	if err != nil {
		return err
	}
	return setProjectQuota(stateDevice, ociProjectID, limit)
}

func (r *Runtime) openOCIProjectAllowance(reservation *importReservation) error {
	allowance, err := reservation.AllocatedBytes()
	if err != nil {
		return err
	}
	usage, err := projectUsage(r.stateDevice, ociProjectID)
	if err != nil {
		return err
	}
	limit, err := ociProjectQuotaLimit(usage, allowance)
	if err != nil {
		return err
	}
	return setProjectQuota(r.stateDevice, ociProjectID, limit)
}

// reserveFreshContainerdBootstrap is the pre-Runtime portion of the one OCI
// growth owner. A fresh containerd must create its metadata before its client
// exists, so the same physical-reservation/project-quota protocol is applied
// around startup. No workload quota exists while the tree is empty. After the
// physically backed allowance is proven, the hold is converted to a kernel
// ceiling; ordinary containerd writes cannot consume E or M. NewRuntime first
// drains scheduled startup GC, then tightens to observed usage.
func reserveFreshContainerdBootstrap(stateDevice string) error {
	var stats unix.Statfs_t
	if err := unix.Statfs(stateRoot, &stats); err != nil {
		return err
	}
	used := (stats.Blocks - stats.Bfree) * uint64(stats.Bsize)
	snapshot := guestdisk.Snapshot{CapacityBytes: used + stats.Bavail*uint64(stats.Bsize), UsedBytes: used,
		EmergencyBytes: guestdisk.DefaultEmergencyBytes, MetadataGrowthBytes: guestdisk.DefaultMetadataBytes,
		AllocationUnitBytes: uint64(stats.Bsize)}
	path := filepath.Join(stateRoot, "containerd-bootstrap-reservation")
	reservation := &importReservation{path: path, holdPath: path + ".hold"}
	budget := new(guestdisk.Budget)
	err := budget.ReserveImportHeadroom(func(*guestdisk.Transaction) (guestdisk.Snapshot, error) { return snapshot, nil }, func(allowance uint64) error {
		reservation.remaining = allowance
		if err := writeImportReservation(reservation.path, reservation.holdPath, allowance); err != nil {
			return err
		}
		allocated, err := reservation.AllocatedBytes()
		if err != nil {
			return err
		}
		usage, err := projectUsage(stateDevice, ociProjectID)
		if err != nil {
			return err
		}
		limit, err := ociProjectQuotaLimit(usage, allocated)
		if err != nil {
			return err
		}
		return setProjectQuota(stateDevice, ociProjectID, limit)
	})
	// Once the kernel ceiling exists, releasing the hold converts I to bounded
	// writable capacity. Keeping it would force containerd to consume E/M even
	// though the project ceiling correctly excludes those floors. NewRuntime uses
	// the startup GC barrier before restoring the exact idle ceiling.
	reservation.Release()
	return err
}

func assignOCIProjectTree(root string, readProject func(string) (uint32, error), assign func(string, uint32) error) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		current, err := readProject(path)
		if err != nil {
			return err
		}
		if current != 0 && current != ociProjectID {
			if path == root {
				return fmt.Errorf("OCI root %s belongs to foreign project %d", root, current)
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if current == ociProjectID && !info.IsDir() {
			return nil
		}
		if err := assign(path, ociProjectID); err != nil {
			return fmt.Errorf("assign OCI project to %s: %w", path, err)
		}
		return nil
	})
}

func (r *Runtime) tightenOCIProjectQuota() error {
	return tightenOCIProjectQuota(r.stateDevice)
}

type leaseGCBarrier interface {
	Create(context.Context, ...leases.Opt) (leases.Lease, error)
	Delete(context.Context, leases.Lease, ...leases.DeleteOpt) error
}

func (r *Runtime) settleOCIProjectQuota(ctx context.Context) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return settleOCIProjectQuotaWith(settleCtx, r.client.LeasesService(), func() error {
		return retryOCIProjectQuota(r.tightenOCIProjectQuota, 10, 100*time.Millisecond)
	})
}

// A synchronous lease deletion asks containerd's official GC scheduler to
// finish its pending collection before the temporary project allowance closes.
func settleOCIProjectQuotaWith(ctx context.Context, manager leaseGCBarrier, tighten func() error) error {
	barrier, err := manager.Create(ctx, leases.WithRandomID())
	if err != nil {
		return fmt.Errorf("create containerd GC barrier: %w", err)
	}
	if err := manager.Delete(ctx, barrier, leases.SynchronousDelete); err != nil {
		return fmt.Errorf("complete containerd GC barrier: %w", err)
	}
	return tighten()
}

func tightenOCIProjectQuota(stateDevice string) error {
	usage, err := projectUsage(stateDevice, ociProjectID)
	if err != nil {
		return err
	}
	return setProjectQuota(stateDevice, ociProjectID, usage)
}

func (r *Runtime) guardBuilderOCIGrowth(ctx context.Context, identity string, run func() error) error {
	return r.guardOCIPhysicalGrowth(ctx, "builder\x00"+identity, run)
}

func (r *Runtime) lockImportSlot(digest string) (func(), error) {
	unlock, ok := r.materializer.LockImport(digest)
	if !ok {
		return nil, resourceCapacityExhausted(resourcev2.CapacityImportSlot, "Guest OCI import slot is occupied")
	}
	return unlock, nil
}

// guardOCIPhysicalGrowth serializes any operation that can materialize bulk
// content or native-snapshot blocks through the same physical I reservation
// and project-9000 hard ceiling as image import.
func (r *Runtime) guardOCIPhysicalGrowth(ctx context.Context, identity string, run func() error) error {
	digest := sha256.Sum256([]byte(identity))
	digestText := "sha256:" + hex.EncodeToString(digest[:])
	unlock, err := r.lockImportSlot(digestText)
	if err != nil {
		return err
	}
	defer unlock()
	header := &vmmdv1.ImportImageHeader{RequestId: "oci-growth-" + hex.EncodeToString(digest[:24]), ManifestDigest: digestText}
	reservation, err := r.reserveImport(ctx, header, true)
	if err != nil {
		return err
	}
	defer reservation.Release()
	return r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return r.guardOCIProjectGrowth(transaction, reservation, run)
	})
}

// guardOCIMetadataGrowth opens only true slack after U+W+I+E+M, capped by M.
// Consuming the entire temporary allowance therefore cannot invade a committed
// floor. A successful settlement restores the exact idle ceiling; persistent
// settlement failure seals this process and requires a guest restart.
func (r *Runtime) guardOCIMetadataGrowth(ctx context.Context, run func() error) error {
	return r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return r.guardOCIMetadataGrowthLocked(ctx, transaction, run)
	})
}

func (r *Runtime) guardOCIMetadataGrowthLocked(ctx context.Context, transaction *guestdisk.Transaction, run func() error) (runErr error) {
	return r.guardOCIMetadataGrowthLockedWithCommit(ctx, transaction, 0, run)
}

func (r *Runtime) guardOCIMetadataGrowthLockedWithCommit(ctx context.Context, transaction *guestdisk.Transaction, additionalWritable uint64, run func() error) (runErr error) {
	snapshot, err := r.guestDiskSnapshot(ctx)
	if err != nil {
		return err
	}
	allowance, err := ociMetadataAllowance(snapshot, additionalWritable)
	if err != nil {
		return err
	}
	usage, err := projectUsage(r.stateDevice, ociProjectID)
	if err != nil {
		return err
	}
	limit, err := ociProjectQuotaLimit(usage, allowance)
	if err != nil {
		return err
	}
	if err := setProjectQuota(r.stateDevice, ociProjectID, limit); err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, transaction.Settle(func() error { return r.settleOCIProjectQuota(ctx) }))
	}()
	return run()
}

func retryOCIProjectQuota(tighten func() error, attempts int, retryDelay time.Duration) error {
	var tightenErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if tightenErr = tighten(); tightenErr == nil {
			return nil
		}
		if attempt+1 < attempts {
			time.Sleep(retryDelay)
		}
	}
	return fmt.Errorf("tighten OCI project quota after %d attempts: %w", attempts, tightenErr)
}

func (r *Runtime) ociGrowthError() error {
	if r != nil && r.diskBudget != nil {
		return r.diskBudget.GrowthError()
	}
	return nil
}

func ociMetadataAllowance(snapshot guestdisk.Snapshot, additionalWritable uint64) (uint64, error) {
	values := []uint64{snapshot.UsedBytes, snapshot.WritableHeadroomBytes, additionalWritable, snapshot.ImportReservationBytes, snapshot.EmergencyBytes, snapshot.MetadataGrowthBytes}
	committed := uint64(0)
	for _, value := range values {
		var carry uint64
		committed, carry = bits.Add64(committed, value, 0)
		if carry != 0 || committed > snapshot.CapacityBytes {
			return 0, guestdisk.ErrCapacityUnavailable
		}
	}
	allowance := min(guestdisk.DefaultMetadataBytes, snapshot.CapacityBytes-committed)
	if allowance == 0 {
		return 0, guestdisk.ErrCapacityUnavailable
	}
	return allowance, nil
}

func ociProjectQuotaLimit(usage, allowance uint64) (uint64, error) {
	limit, carry := bits.Add64(usage, allowance, 0)
	if carry != 0 {
		return 0, guestdisk.ErrCapacityUnavailable
	}
	return limit, nil
}

func (r *Runtime) guardOCIProjectGrowth(transaction *guestdisk.Transaction, reservation *importReservation, run func() error) (runErr error) {
	allowance, err := reservation.AllocatedBytes()
	if err != nil {
		return err
	}
	usage, err := projectUsage(r.stateDevice, ociProjectID)
	if err != nil {
		return err
	}
	limit, err := ociProjectQuotaLimit(usage, allowance)
	if err != nil {
		return err
	}
	if err := setProjectQuota(r.stateDevice, ociProjectID, limit); err != nil {
		return err
	}
	reservation.mu.Lock()
	release := reservation.remaining
	reservation.mu.Unlock()
	if err := reservation.Consume(release); err != nil {
		return errors.Join(err, transaction.Settle(func() error { return r.settleOCIProjectQuota(context.Background()) }))
	}
	defer func() {
		runErr = errors.Join(runErr, transaction.Settle(func() error { return r.settleOCIProjectQuota(context.Background()) }))
	}()
	return run()
}

func (r *Runtime) reserveImport(ctx context.Context, header *vmmdv1.ImportImageHeader, materializerHeld bool) (*importReservation, error) {
	loadSnapshot := func(transaction *guestdisk.Transaction) (guestdisk.Snapshot, error) {
		snapshot, err := r.guestDiskSnapshot(ctx)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
		if snapshot.Pressure() >= guestdisk.PressureGC {
			if _, err := r.reclaimOCIWithMaterializerTransaction(ctx, maxGCDeletes, materializerHeld, transaction); err != nil {
				return guestdisk.Snapshot{}, err
			}
			snapshot, err = r.guestDiskSnapshot(ctx)
			if err != nil {
				return guestdisk.Snapshot{}, err
			}
		}
		if snapshot.Pressure() >= guestdisk.PressureRejectCold {
			return guestdisk.Snapshot{}, guestdisk.ErrCapacityUnavailable
		}
		return snapshot, nil
	}
	identity := sha256.Sum256([]byte(header.GetManifestDigest() + "\x00" + header.GetRequestId()))
	path := filepath.Join(stateRoot, "import-reservations", hex.EncodeToString(identity[:]))
	result := &importReservation{path: path, holdPath: path + ".hold"}
	commit := func(reserved uint64) error {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		if data, err := os.ReadFile(path); err == nil {
			existing, parseErr := strconv.ParseUint(string(data), 10, 64)
			if parseErr != nil || existing != reserved {
				return errors.New("import reservation identity conflicts with durable state")
			}
			result.remaining = existing
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		result.remaining = reserved
		return writeImportReservation(path, result.holdPath, reserved)
	}
	err := r.diskBudget.ReserveImportHeadroom(loadSnapshot, commit)
	if errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		if _, gcErr := r.reclaimOCIWithMaterializer(ctx, maxGCDeletes, materializerHeld); gcErr != nil {
			return nil, gcErr
		}
		err = r.diskBudget.ReserveImportHeadroom(loadSnapshot, commit)
	}
	if errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		return nil, resourceCapacityExhausted(resourcev2.CapacityStorageHeadroom, "Guest storage headroom is unavailable")
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (reservation *importReservation) Consume(bytes uint64) error {
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	if bytes >= reservation.remaining {
		reservation.remaining = 0
	} else {
		reservation.remaining -= bytes
	}
	return writeImportReservation(reservation.path, reservation.holdPath, reservation.remaining)
}
func (reservation *importReservation) Release() {
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	_ = removeImportReservation(reservation.path, reservation.holdPath)
}

func removeImportReservation(path, holdPath string) error {
	for _, target := range []string{path, holdPath} {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (reservation *importReservation) AllocatedBytes() (uint64, error) {
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	info, err := os.Stat(reservation.holdPath)
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("import reservation has no filesystem allocation facts")
	}
	return uint64(stat.Blocks) * 512, nil
}

func writeImportReservation(path, holdPath string, value uint64) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	hold, err := os.OpenFile(holdPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	info, err := hold.Stat()
	if err == nil && uint64(info.Size()) < value {
		err = unix.Fallocate(int(hold.Fd()), 0, 0, int64(value))
	} else if err == nil {
		err = hold.Truncate(int64(value))
	}
	if err == nil {
		err = hold.Sync()
	}
	if closeErr := hold.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".reservation-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.WriteString(strconv.FormatUint(value, 10)); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
