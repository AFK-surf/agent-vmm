//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"golang.org/x/sys/unix"
)

type importReservation struct {
	mu        sync.Mutex
	path      string
	holdPath  string
	remaining uint64
}

func (r *Runtime) guardBuilderOCIGrowth(ctx context.Context, identity string, run func() error) error {
	if strings.HasPrefix(identity, "cache-write\x00") {
		return r.guardBuilderCacheGrowth(ctx, identity, run)
	}
	return r.guardOCIPhysicalGrowth(ctx, "builder\x00"+identity, true, run)
}

// lockImportSlot waits in FIFO order for the Guest-global materialization
// slot until the request deadline.
func (r *Runtime) lockImportSlot(ctx context.Context, digest string) (func(), error) {
	unlock, err := r.materializer.LockImport(ctx, digest)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, resourceCapacityExhausted(resourcev2.CapacityImportSlot, "import_slot", "Guest OCI import slot stayed occupied until the request deadline")
	}
	return unlock, err
}

// guardOCIPhysicalGrowth serializes any operation that can materialize bulk
// content or overlay-snapshot blocks through the same physical I reservation
// and project-9000 hard ceiling as image import. BuildMaterializationBudget.tla
// keeps local reserve-to-settle indivisible to metadata and fences canceled waits.
func (r *Runtime) guardOCIPhysicalGrowth(ctx context.Context, identity string, waitForSlot bool, run func() error) error {
	digest := sha256.Sum256([]byte(identity))
	digestText := "sha256:" + hex.EncodeToString(digest[:])
	var unlock func()
	var err error
	if waitForSlot {
		unlock, err = r.materializer.LockBuild(ctx)
	} else {
		unlock, err = r.lockImportSlot(ctx, digestText)
	}
	if err != nil {
		return err
	}
	defer unlock()
	header := &vmmdv1.ImportImageHeader{RequestId: "oci-growth-" + hex.EncodeToString(digest[:24]), ManifestDigest: digestText}
	return r.guardOCIPhysicalGrowthLocked(ctx, header, run)
}

func (r *Runtime) guardOCIPhysicalGrowthLocked(ctx context.Context, header *vmmdv1.ImportImageHeader, run func() error) error {
	return r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		reservation, err := r.reserveImportLocked(ctx, header, transaction)
		if err != nil {
			return err
		}
		defer reservation.Release()
		return r.guardOCIProjectGrowth(transaction, reservation, run)
	})
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

func (r *Runtime) reserveImport(ctx context.Context, header *vmmdv1.ImportImageHeader) (*importReservation, error) {
	var reservation *importReservation
	err := r.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		var err error
		reservation, err = r.reserveImportLocked(ctx, header, transaction)
		return err
	})
	return reservation, err
}

func (r *Runtime) reserveImportLocked(ctx context.Context, header *vmmdv1.ImportImageHeader, transaction *guestdisk.Transaction) (*importReservation, error) {
	var lastSnapshot guestdisk.Snapshot
	loadSnapshot := func(transaction *guestdisk.Transaction) (guestdisk.Snapshot, error) {
		snapshot, err := r.guestDiskSnapshot(ctx)
		if err != nil {
			return guestdisk.Snapshot{}, err
		}
		lastSnapshot = snapshot

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
	required, reserve := guestdisk.MinimumImportBytes, transaction.ReserveImportHeadroom
	if header.GetArchiveSize() > 0 {
		required = guestdisk.ImportReservationBytes(header.GetArchiveSize())
		reserve = func(load func(*guestdisk.Transaction) (guestdisk.Snapshot, error), commit func(uint64) error) error {
			return transaction.ReserveImport(load, required, commit)
		}
	}
	err := reserve(loadSnapshot, commit)
	if errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		log.Printf("OCI import reservation unavailable: request_id=%q manifest_digest=%q required_bytes=%d capacity=%q", header.GetRequestId(), header.GetManifestDigest(), required, ociReclaimCapacityReason("import_headroom_unavailable", lastSnapshot))
		if _, gcErr := r.reclaimOCIWithMaterializerTransaction(ctx, maxGCDeletes, true, transaction, ociReclaimCapacityReason("import_headroom_unavailable", lastSnapshot)); gcErr != nil {
			return nil, gcErr
		}
		err = reserve(loadSnapshot, commit)
	}
	if errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		available, _ := lastSnapshot.AvailableBytes()
		return nil, resourceCapacityExhaustedWithBytes(resourcev2.CapacityStorageHeadroom, "import_admission", "Guest storage headroom is unavailable", available, required)
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

// PrepareOCIStorage assigns immutable OCI data to the shared pool project.
func PrepareOCIStorage(stateDevice string) error {
	if err := prepareOCIStorage(stateDevice,
		[]string{stateRoot + "/containerd", stateRoot + "/containerd/content", stateRoot + "/containerd/overlayfs", stateRoot + "/imports", stateRoot + "/appliance-content", stateRoot + "/appliance-content-projections"},
		os.MkdirAll, projectID, assignProject, tightenOCIProjectQuota); err != nil {
		return err
	}
	root := filepath.Join(stateRoot, "preparation-cache")
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	if err := assignProject(root, cacheProjectID); err != nil {
		return err
	}
	return tightenCacheProjectQuota(stateDevice)
}

func prepareOCIStorage(stateDevice string, stateRoots []string,
	mkdir func(string, os.FileMode) error, readProject func(string) (uint32, error), assign func(string, uint32) error,
	tightenState func(string) error,
) error {
	for _, root := range stateRoots {
		if err := mkdir(root, 0700); err != nil {
			return err
		}
		if err := assignOCIRoot(root, readProject, assign); err != nil {
			return err
		}
	}
	return tightenState(stateDevice)
}

func tightenOCIProjectQuota(stateDevice string) error {
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

func (r *Runtime) settleOCIProjectQuota(ctx context.Context) error {
	return retryOCIProjectQuota(func() error {
		return errors.Join(tightenOCIProjectQuota(r.stateDevice), tightenCacheProjectQuota(r.stateDevice))
	}, 10, 100*time.Millisecond)
}
func assignOCIRoot(root string, readProject func(string) (uint32, error), assign func(string, uint32) error) error {
	current, err := readProject(root)
	if err != nil {
		return err
	}
	if current != 0 && current != ociProjectID {
		return fmt.Errorf("OCI root %s belongs to foreign project %d", root, current)
	}
	return assign(root, ociProjectID)
}

func ociReclaimCapacityReason(trigger string, snapshot guestdisk.Snapshot) string {
	available, valid := snapshot.AvailableBytes()
	return fmt.Sprintf("%s pressure=%d available_bytes=%d available_valid=%t required_import_bytes=%d capacity_bytes=%d used_bytes=%d import_reserved_bytes=%d namespace_committed_bytes=%d inodes=%d free_inodes=%d", trigger, snapshot.Pressure(), available, valid, guestdisk.MinimumImportBytes, snapshot.CapacityBytes, snapshot.UsedBytes, snapshot.ImportReservationBytes, snapshot.NamespaceCommitmentBytes, snapshot.Inodes, snapshot.FreeInodes)
}
