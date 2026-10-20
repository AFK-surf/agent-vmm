//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	"github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

func TestOCIProjectAssignmentTouchesOnlyOwnerRoot(t *testing.T) {
	root := t.TempDir()
	global := filepath.Join(root, "content", "blob")
	writable := filepath.Join(root, "snapshots", "active-workload")
	if err := os.MkdirAll(filepath.Dir(global), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(global, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(writable, 0o700); err != nil {
		t.Fatal(err)
	}
	projects := map[string]uint32{writable: 10001, global: ociProjectID}
	read := func(path string) (uint32, error) { return projects[path], nil }
	assign := func(path string, id uint32) error {
		if path != root {
			t.Fatalf("startup visited content path %s", path)
		}
		projects[path] = id
		return nil
	}
	if err := assignOCIRoot(root, read, assign); err != nil {
		t.Fatal(err)
	}
	if projects[root] != ociProjectID || projects[global] != ociProjectID {
		t.Fatalf("global OCI tree projects=%v", projects)
	}
	if projects[writable] != 10001 {
		t.Fatalf("workload writable snapshot project=%d", projects[writable])
	}
}

func TestPrepareOCIStorageKeepsImagesOnPoolDevice(t *testing.T) {
	configuration, err := os.ReadFile("../../../build/guest/root/etc/containerd/config.toml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(configuration), `[plugins."io.containerd.snapshotter.v1.native"]`) || !strings.Contains(string(configuration), `root_path = "/state/containerd/overlayfs"`) {
		t.Fatal("image snapshots must use the shared overlay pool")
	}
	base := t.TempDir()
	roots := []string{filepath.Join(base, "content"), filepath.Join(base, "snapshots")}
	projects := map[string]uint32{}
	var tightenedDevice string
	err = prepareOCIStorage("pool-device", roots, os.MkdirAll,
		func(path string) (uint32, error) { return projects[path], nil },
		func(path string, project uint32) error { projects[path] = project; return nil },
		func(device string) error { tightenedDevice = device; return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range roots {
		if projects[root] != ociProjectID {
			t.Fatalf("OCI root %s project=%d", root, projects[root])
		}
	}
	if tightenedDevice != "pool-device" {
		t.Fatalf("quota tightened on %q", tightenedDevice)
	}
}

func TestRuntimeImportSlotOwnerReportsExactCapacityDimension(t *testing.T) {
	runtime := &Runtime{materializer: new(ocimaterializer.Manager)}
	unlock, err := runtime.lockImportSlot("sha256:first")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if _, err = runtime.lockImportSlot("sha256:second"); err == nil {
		t.Fatal("unrelated import acquired an occupied global slot")
	}
	for _, detail := range status.Convert(err).Details() {
		if value, ok := detail.(*errdetails.ErrorInfo); ok && value.GetMetadata()["resource_dimension"] == "import_slot" {
			return
		}
	}
	t.Fatalf("import slot error omitted exact owner dimension: %v", err)
}

func TestBuildMaterializationWaitHonorsCancellation(t *testing.T) {
	runtime := &Runtime{materializer: new(ocimaterializer.Manager)}
	unlock, err := runtime.lockImportSlot("sha256:occupied")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = runtime.guardOCIPhysicalGrowth(ctx, "second-build", true, func() error { t.Fatal("occupied build materialization ran"); return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admitted build returned %v instead of waiting to its deadline", err)
	}
}

func TestOCIProjectAssignmentFailsClosedBeforeImport(t *testing.T) {
	root := t.TempDir()
	want := errors.New("project quota unavailable")
	err := assignOCIRoot(root, func(string) (uint32, error) { return 0, nil }, func(string, uint32) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("assignment error=%v", err)
	}
}

func TestOCIProjectAssignmentRejectsForeignRoot(t *testing.T) {
	root := t.TempDir()
	err := assignOCIRoot(root, func(path string) (uint32, error) {
		if path == root {
			return 12345, nil
		}
		return 0, nil
	}, func(string, uint32) error { return nil })
	if err == nil {
		t.Fatal("foreign OCI root was skipped instead of failing closed")
	}
}

func TestOCIProjectQuotaCeilingCannotConsumeProtectedHeadroom(t *testing.T) {
	const usage = uint64(11 << 30)
	const importAllowance = uint64(3 << 30)
	limit, err := ociProjectQuotaLimit(usage, importAllowance)
	if err != nil || limit != usage+importAllowance {
		t.Fatalf("limit=%d err=%v", limit, err)
	}
	// The kernel stops expansion at the admitted OCI allowance.
	if _, err := ociProjectQuotaLimit(math.MaxUint64, 1); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		t.Fatalf("overflow error=%v", err)
	}
}

func TestOCIProjectQuotaSettlementRetriesAndPreservesRunFailure(t *testing.T) {
	runErr := errors.New("containerd metadata mutation failed")
	attempts := 0
	runtime := &Runtime{diskBudget: new(guestdisk.Budget)}
	err := errors.Join(runErr, runtime.diskBudget.Settle(func() error {
		return retryOCIProjectQuota(func() error {
			attempts++
			if attempts < 3 {
				return errors.New("transient quota ioctl failure")
			}
			return nil
		}, 3, 0)
	}))
	if !errors.Is(err, runErr) || attempts != 3 {
		t.Fatalf("settlement error=%v attempts=%d", err, attempts)
	}
}

func TestOCIProjectQuotaSettlementReportsRunAndPersistentTightenFailures(t *testing.T) {
	runErr := errors.New("containerd metadata mutation failed")
	tightenErr := errors.New("quota ioctl unavailable")
	runtime := &Runtime{diskBudget: new(guestdisk.Budget)}
	err := errors.Join(runErr, runtime.diskBudget.Settle(func() error {
		return retryOCIProjectQuota(func() error { return tightenErr }, 2, 0)
	}))
	if !errors.Is(err, runErr) || !errors.Is(err, tightenErr) {
		t.Fatalf("settlement dropped an owner failure: %v", err)
	}
	if !errors.Is(runtime.ociGrowthError(), guestdisk.ErrGrowthSealed) {
		t.Fatal("persistent settlement failure did not seal OCI growth")
	}
}

func TestSealedImportAllowsControlInventory(t *testing.T) {
	runtime := &Runtime{diskBudget: new(guestdisk.Budget), materializer: new(ocimaterializer.Manager)}
	_ = runtime.diskBudget.Settle(func() error { return errors.New("persistent settlement failure") })
	if err := runtime.guardOCIPhysicalGrowth(context.Background(), "sealed-test", false, func() error { t.Fatal("sealed import ran"); return nil }); !errors.Is(err, guestdisk.ErrGrowthSealed) {
		t.Fatalf("physical gate: %v", err)
	}
	ran := false
	if err := runtime.diskBudget.MutateInventory(func(*guestdisk.Transaction) error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("control inventory blocked: %v", err)
	}
}

func TestImportReservationIsPhysicallyBackedAndReleased(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "reservation")
	hold := path + ".hold"
	if err := writeImportReservation(path, hold, 8<<20); err != nil {
		t.Fatal(err)
	}
	reserved, count, err := importReservationSummary(directory)
	if err != nil || reserved < 8<<20 || count != 1 {
		t.Fatalf("physical reservation=%d count=%d err=%v", reserved, count, err)
	}
	reservation := &importReservation{path: path, holdPath: hold, remaining: 8 << 20}
	if err := reservation.Consume(3 << 20); err != nil {
		t.Fatal(err)
	}
	shrunk, count, err := importReservationSummary(directory)
	if err != nil || shrunk >= reserved || shrunk < 5<<20 || count != 1 {
		t.Fatalf("converted reservation=%d before=%d count=%d err=%v", shrunk, reserved, count, err)
	}
	reservation.Release()
	if _, err := os.Stat(hold); !os.IsNotExist(err) {
		t.Fatalf("completed import retained physical hold: %v", err)
	}
	reserved, count, err = importReservationSummary(directory)
	if err != nil || reserved != 0 || count != 0 {
		t.Fatalf("released reservation summary=%d count=%d err=%v", reserved, count, err)
	}
}

func TestImportReservationSummaryRejectsUnboundedDirectory(t *testing.T) {
	directory := t.TempDir()
	for index := 0; index <= maxImportReservationDirectoryEntries; index++ {
		path := filepath.Join(directory, fmt.Sprintf("reservation-%d.hold", index))
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := importReservationSummary(directory); err == nil {
		t.Fatal("unbounded import reservation directory was accepted")
	}
}

func TestImageGrowthUsesAlreadyHeldMaterializerSlot(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		t.Run(fmt.Sprintf("wait=%t", waiting), func(t *testing.T) {
			runtime := &Runtime{materializer: new(ocimaterializer.Manager), diskBudget: new(guestdisk.Budget)}
			_ = runtime.diskBudget.Settle(func() error { return errors.New("quota settlement failed") })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var unlock func()
			var err error
			if waiting {
				unlock, err = runtime.materializer.LockBuild(ctx)
			} else {
				unlock, err = runtime.lockImportSlot("sha256:copy")
			}
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			err = runtime.guardOCIPhysicalGrowthLocked(ctx, &vmmdv1.ImportImageHeader{RequestId: "copy", ManifestDigest: "sha256:copy"}, func() error { t.Fatal("sealed capacity admitted growth"); return nil })
			if !errors.Is(err, guestdisk.ErrGrowthSealed) {
				t.Fatalf("held slot masked existing capacity result: %v", err)
			}
			if ctx.Err() != nil {
				t.Fatal("held materializer waited on itself")
			}
			if _, err := runtime.lockImportSlot("sha256:other"); err == nil {
				t.Fatal("growth released another owner's slot")
			}
		})
	}
}

func TestLongBuilderQuotaCreditLeavesBudgetLockAvailable(t *testing.T) {
	if path := os.Getenv("AGENT_VMM_CACHE_QUOTA_HELPER"); path != "" {
		file, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		offset, err := strconv.ParseInt(os.Getenv("AGENT_VMM_CACHE_QUOTA_OFFSET"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Fallocate(int(file.Fd()), 0, offset, 8<<20); !errors.Is(err, unix.EDQUOT) {
			t.Fatalf("cache exceeded project hard quota: %v", err)
		}
		return
	}
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable /state ext4 mount with project quotas")
	}
	namespaceFacts := &operationNamespaces{labels: map[string]map[string]string{}}
	client, err := containerd.New("", containerd.WithServices(containerd.WithNamespaceService(namespaceFacts)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	runtime := &Runtime{client: client, stateDevice: device, diskBudget: new(guestdisk.Budget), materializer: new(ocimaterializer.Manager), inventory: newRuntimeInventoryIndex()}
	if err := PrepareOCIStorage(device); err != nil {
		t.Fatal(err)
	}
	workload := filepath.Join(stateRoot, "volumes", "budget-workload")
	if err := os.MkdirAll(workload, 0700); err != nil {
		t.Fatal(err)
	}
	if err := assignProject(workload, 10001); err != nil {
		t.Fatal(err)
	}
	if err := setProjectQuota(device, 10001, 2<<30); err != nil {
		t.Fatal(err)
	}
	namespaceFacts.labels["budget-workload"] = namespaceLabels(&vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 64 << 20, Pids: 64, DiskBytes: 2 << 30}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10001, "10.42.1.0/24")
	namespaceFacts.labels["system.fixture"] = namespaceLabels(&vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 64 << 20, Pids: 64, DiskBytes: 3 << 30}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10002, "10.42.2.0/24")
	initial, err := runtime.guestDiskSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if initial.NamespaceCommitmentBytes > 2<<30 || initial.NamespaceCommitmentBytes < (2<<30)-(1<<20) {
		t.Fatalf("ordinary quota commitment/system exclusion: %d", initial.NamespaceCommitmentBytes)
	}

	cache := filepath.Join(stateRoot, "preparation-cache", "budget-fixture", "buildkit")
	old := filepath.Join(stateRoot, "preparation-cache", "discardable")
	if err := runtime.guardBuilderOCIGrowth(context.Background(), "cache-write\x00seed-fixture", func() error {
		if err := os.MkdirAll(cache, 0700); err != nil {
			return err
		}
		return os.WriteFile(old, make([]byte, 32<<20), 0600)
	}); err != nil {
		t.Fatal(err)
	}
	identity := "cache-write\x00quota-fixture"
	started := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runtime.guardBuilderOCIGrowth(context.Background(), identity, func() error {
			if err := os.WriteFile(filepath.Join(cache, "registry"), make([]byte, 32<<20), 0600); err != nil {
				return err
			}
			close(started)
			<-stop
			return nil
		})
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("writer admission: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not start")
	}
	defer func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}()
	quota, err := activeBuilderQuotaLimit(filepath.Join(stateRoot, "import-reservations"))
	if err != nil || quota == 0 {
		t.Fatalf("missing writer credit: %d %v", quota, err)
	}
	observed := make(chan error, 1)
	go func() {
		observed <- runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
			if _, err := runtime.reclaimOCIWithMaterializerTransaction(context.Background(), maxGCDeletes, false, transaction, "active_quota_credit_test"); err != nil {
				return err
			}
			snapshot, err := runtime.guestDiskSnapshot(context.Background())
			if err != nil {
				return err
			}
			if available, ok := snapshot.AvailableBytes(); ok && available >= guestdisk.MinimumImportBytes {
				return fmt.Errorf("unwritten credit released to admission: %d", available)
			}
			if err := runtime.admitNamespaceDiskGrowth(context.Background(), "another-workload", 64<<20, nil); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
				return fmt.Errorf("new namespace consumed writer credit: %v", err)
			}
			if err := runtime.admitNamespaceDiskGrowth(context.Background(), "budget-workload", 3<<30, namespaceFacts.labels["budget-workload"]); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
				return fmt.Errorf("namespace quota increase consumed writer credit: %v", err)
			}
			if err := os.WriteFile(filepath.Join(workload, "active-workspace"), make([]byte, 64<<20), 0600); err != nil {
				return err
			}
			withWorkload, err := runtime.guestDiskSnapshot(context.Background())
			if err != nil {
				return err
			}
			if withWorkload.NamespaceCommitmentBytes >= snapshot.NamespaceCommitmentBytes {
				return errors.New("written ordinary blocks were double charged to K")
			}
			var physical unix.Statfs_t
			if err := unix.Statfs(stateRoot, &physical); err != nil {
				return err
			}
			if physical.Bavail*uint64(physical.Bsize) < guestdisk.DefaultEmergencyBytes+guestdisk.DefaultMetadataBytes {
				return errors.New("concurrent I/workload writer consumed recovery reserves")
			}

			if err := os.Remove(old); err != nil {
				return err
			}
			if err := tightenOCIProjectQuota(device); err != nil {
				return err
			}
			after, err := runtime.guestDiskSnapshot(context.Background())
			if err != nil {
				return err
			}
			if after.ImportReservationBytes <= snapshot.ImportReservationBytes {
				return errors.New("GC did not restore unused writer credit")
			}
			usage, err := projectUsage(device, cacheProjectID)
			if err != nil {
				return err
			}
			full, err := os.OpenFile(filepath.Join(cache, "pressure"), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				return err
			}
			defer full.Close()
			length := int64((quota - usage - (1 << 20)) &^ 4095)
			if err := unix.Fallocate(int(full.Fd()), 0, 0, length); err != nil {
				return fmt.Errorf("fill cache hard quota: %w", err)
			}
			if err := unix.Prctl(unix.PR_CAPBSET_DROP, unix.CAP_SYS_RESOURCE, 0, 0, 0); err != nil {
				return err
			}
			command := exec.Command(os.Args[0], "-test.run=^TestLongBuilderQuotaCreditLeavesBudgetLockAvailable$")
			command.Env = append(os.Environ(), "AGENT_VMM_CACHE_QUOTA_HELPER="+full.Name(), "AGENT_VMM_CACHE_QUOTA_OFFSET="+strconv.FormatInt(length, 10))
			if output, err := command.CombinedOutput(); err != nil {
				return fmt.Errorf("cache quota writer: %w: %s", err, output)
			}
			views, err := overlay.NewSnapshotter(filepath.Join(stateRoot, "containerd", "overlayfs", "pressure-view"))
			if err != nil {
				return err
			}
			defer views.Close()
			ctx := context.Background()
			if _, err := views.Prepare(ctx, "base-write", ""); err != nil {
				return err
			}
			if err := views.Commit(ctx, "base", "base-write"); err != nil {
				return err
			}
			mounts, err := views.View(ctx, "system-restart", "base")
			if err != nil {
				return err
			}
			if err := mount.WithReadonlyTempMount(ctx, mounts, func(root string) error {
				_, err := os.Stat(root)
				return err
			}); err != nil {
				return err
			}
			if err := views.Remove(ctx, "system-restart"); err != nil {
				return err
			}
			if err := views.Remove(ctx, "base"); err != nil {
				return err
			}
			if err := os.Remove(full.Name()); err != nil {
				return err
			}
			return nil
		})
	}()
	select {
	case err := <-observed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("long writer holds Stop/observation/GC budget lock")
	}
	_, err = runtime.reserveImport(context.Background(), &vmmdv1.ImportImageHeader{RequestId: "concurrent-cold", ManifestDigest: "sha256:" + strings.Repeat("1", 64)})
	if err == nil {
		t.Fatal("cold admission consumed unwritten writer credit")
	}
	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if quota, err := activeBuilderQuotaLimit(filepath.Join(stateRoot, "import-reservations")); err != nil || quota != 0 {
		t.Fatalf("settled credit retained: %d %v", quota, err)
	}
	snapshot, err := runtime.guestDiskSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if available, ok := snapshot.AvailableBytes(); !ok || available < guestdisk.MinimumImportBytes {
		t.Fatalf("settled writer did not return capacity: %d %v", available, ok)
	} else {
		t.Logf("settled writer available=%d, opened quota=%d", available, quota)
	}
	identity = "cache-write\x00unsettled-fixture"
	liveStop := make(chan struct{})
	liveDone := make(chan struct{})
	liveStarted := make(chan error, 1)
	err = runtime.guardBuilderOCIGrowth(context.Background(), identity, func() error {
		go func() {
			defer close(liveDone)
			file, err := os.OpenFile(filepath.Join(cache, "live-registry"), os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				liveStarted <- err
				return
			}
			defer file.Close()
			_, err = file.Write(make([]byte, 1<<20))
			liveStarted <- err
			if err != nil {
				return
			}
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-liveStop:
					return
				case <-ticker.C:
					if _, err := file.Write(make([]byte, 4096)); err != nil {
						return
					}
				}
			}
		}()
		if err := <-liveStarted; err != nil {
			return err
		}
		return &guestbuild.WriterUnsettledError{Err: context.Canceled}
	})
	var unsettled *guestbuild.WriterUnsettledError
	if !errors.As(err, &unsettled) {
		t.Fatalf("unknown writer source lost: %v", err)
	}
	if limit, err := activeBuilderQuotaLimit(filepath.Join(stateRoot, "import-reservations")); err != nil || limit == 0 {
		t.Fatalf("live writer credit released: %d %v", limit, err)
	}
	close(liveStop)
	<-liveDone
	if err := runtime.settleBuilderCacheReservation(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if limit, err := activeBuilderQuotaLimit(filepath.Join(stateRoot, "import-reservations")); err != nil || limit != 0 {
		t.Fatalf("exact stopped writer credit remains: %d %v", limit, err)
	}
}

func TestBuilderReservationConversionFailureRetainsPriorFacts(t *testing.T) {
	for fail := 0; fail < 4; fail++ {
		t.Run(fmt.Sprintf("step-%d", fail), func(t *testing.T) {
			root := t.TempDir()
			reservation := &importReservation{path: filepath.Join(root, "operation"), holdPath: filepath.Join(root, "operation.hold"), remaining: 8 << 20}
			if err := writeImportReservation(reservation.path, reservation.holdPath, reservation.remaining); err != nil {
				t.Fatal(err)
			}
			credit := reservation.path + ".writer"
			opened := false
			failure := errors.New("conversion step failed")
			err := transferBuilderReservation(func() error {
				if fail == 1 {
					return failure
				}
				return persistBuilderQuota(credit, 16<<20)
			}, func() error {
				if fail == 2 {
					return failure
				}
				opened = true
				return nil
			}, func() error {
				if fail == 3 {
					return failure
				}
				return reservation.Consume(8 << 20)
			})
			if fail == 0 && err != nil || fail != 0 && !errors.Is(err, failure) {
				t.Fatalf("conversion cause: %v", err)
			}
			_, creditErr := os.Stat(credit)
			if (creditErr == nil) != (fail != 1) {
				t.Fatal("credit persistence order changed")
			}
			if opened != (fail != 1 && fail != 2) {
				t.Fatal("quota opened before durable credit")
			}
			allocated, err := reservation.AllocatedBytes()
			if err != nil {
				t.Fatal(err)
			}
			if fail == 0 && allocated != 0 || fail != 0 && allocated == 0 {
				t.Fatalf("hold conversion at failed step: %d", allocated)
			}
		})
	}
}

func TestControlSystemQuotaCommitmentsShareGuestBudget(t *testing.T) {
	device := os.Getenv("AGENT_VMM_CONTROL_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable /control ext4 project quota mount")
	}
	facts := &operationNamespaces{labels: map[string]map[string]string{}}
	client, err := containerd.New("", containerd.WithServices(containerd.WithNamespaceService(facts)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	runtime := &Runtime{client: client, controlDevice: device, diskBudget: new(guestdisk.Budget)}
	grant := func(name string, project uint32, size uint64) error {
		return runtime.diskBudget.MutateInventory(func(*guestdisk.Transaction) error {
			facts.mu.Lock()
			labels := facts.labels[name]
			facts.mu.Unlock()
			if err := runtime.admitNamespaceDiskGrowth(context.Background(), name, size, labels); err != nil {
				return err
			}
			path := filepath.Join("/control/volumes", name)
			if err := os.MkdirAll(path, 0700); err != nil {
				return err
			}
			if err := assignProject(path, project); err != nil {
				return err
			}
			if err := setProjectQuota(device, project, size); err != nil {
				return err
			}
			facts.mu.Lock()
			facts.labels[name] = namespaceLabels(&vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 64 << 20, Pids: 64, DiskBytes: size}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, project, "10.42.1.0/24")
			facts.mu.Unlock()
			return nil
		})
	}
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) { results <- grant(fmt.Sprintf("system.controller%d", i), uint32(11000+i), 3<<30) }(i)
	}
	successes := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			successes++
		} else if !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("independent controllers admitted %d grants", successes)
	}
	facts.mu.Lock()
	var winner string
	var project uint32
	for name, labels := range facts.labels {
		winner = name
		project, _, err = namespaceFromLabels(name, labels)
	}
	facts.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join("/control/volumes", winner, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Fallocate(int(file.Fd()), 0, 0, 64<<20); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err := grant("system.third", 11002, 1<<30); err != nil {
		t.Fatalf("written usage double counted: %v", err)
	}
	runtime = &Runtime{client: client, controlDevice: device, diskBudget: new(guestdisk.Budget)}
	if err := grant("system.fourth", 11003, 1<<20); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		t.Fatalf("restart forgot commitments: %v", err)
	}
	if err := runtime.diskBudget.MutateInventory(func(*guestdisk.Transaction) error {
		if err := os.RemoveAll(filepath.Join("/control/volumes", winner)); err != nil {
			return err
		}
		if err := setProjectQuota(device, project, 1024); err != nil {
			return err
		}
		facts.mu.Lock()
		delete(facts.labels, winner)
		facts.mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := grant("system.fourth", 11003, 3<<30); err != nil {
		t.Fatalf("delete did not release commitment: %v", err)
	}

	for name, size := range map[string]int64{"system.third": 1 << 30, "system.fourth": 3 << 30} {
		full, err := os.Create(filepath.Join("/control/volumes", name, "full"))
		if err != nil {
			t.Fatal(err)
		}
		length := size - (8 << 20)
		if err := unix.Fallocate(int(full.Fd()), 0, 0, length); err != nil {
			t.Fatal(err)
		}
		full.Close()
		if err := unix.Prctl(unix.PR_CAPBSET_DROP, unix.CAP_SYS_RESOURCE, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(os.Args[0], "-test.run=^TestLongBuilderQuotaCreditLeavesBudgetLockAvailable$")
		command.Env = append(os.Environ(), "AGENT_VMM_CACHE_QUOTA_HELPER="+full.Name(), "AGENT_VMM_CACHE_QUOTA_OFFSET="+strconv.FormatInt(length, 10))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("bounded system writer: %v: %s", err, output)
		}
	}
	metadataPath := "/control/metadata-pressure.db"
	database, err := bolt.Open(metadataPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	durable := &diskOperationNamespaces{db: database}
	if err := durable.Create(context.Background(), runtimeContainerdNamespace, nil); err != nil {
		t.Fatal(err)
	}
	metadataClient, err := containerd.New("", containerd.WithServices(containerd.WithNamespaceService(durable)))
	if err != nil {
		t.Fatal(err)
	}
	defer metadataClient.Close()
	metadataRuntime := &Runtime{client: metadataClient, imageOperationWriterID: "fixture-owner-ended"}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: "pressure-stop", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED, WriterSettled: true}
	if err := metadataRuntime.saveImageOperation(context.Background(), "system.third", receipt, ""); err != nil {
		t.Fatalf("pressure prevented stopped receipt: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = bolt.Open(metadataPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	durable.db = database
	observed, err := metadataRuntime.imageOperation(context.Background(), "system.third", receipt.RequestId, receipt.Kind)
	if err != nil || !observed.GetWriterSettled() || observed.GetState() != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED {
		t.Fatalf("pressure/restart lost stopped receipt: %v %v", observed, err)
	}
	if err := os.RemoveAll("/control/volumes/system.third"); err != nil {
		t.Fatalf("pressure blocked actual cache cleanup: %v", err)
	}
}

func TestImportAdmissionPreservesCachesWhenHeadroomFits(t *testing.T) {
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable /state ext4 mount with project quotas")
	}
	ctx := context.Background()
	namespaceFacts := &operationNamespaces{labels: map[string]map[string]string{}}
	digest := digest.FromString("retained-import-admission")
	reference := "docker.io/library/fixture:latest"
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{runtimeContainerdNamespace: {}, "preparation": {reference: {Name: reference, Target: &types.Descriptor{Digest: digest.String()}}}}, containers: map[string]*containersapi.Container{}}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(namespaceFacts)))
	runtime := &Runtime{client: client, stateDevice: device, diskBudget: new(guestdisk.Budget), materializer: new(ocimaterializer.Manager), inventory: newRuntimeInventoryIndex(), builder: guestbuild.NewBuilder(client, nil, nil)}
	if err := PrepareOCIStorage(device); err != nil {
		t.Fatal(err)
	}
	built := &vmmdv1.ImageOperationReceipt{RequestId: "original-build", Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, ImageReference: reference, ManifestDigest: digest.String(), State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true}
	if err := runtime.retainBuiltImage(ctx, "controller", "preparation", built); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(stateRoot, "preparation-cache", "admission-fixture", "buildkit", "completed-layer")
	if err := os.MkdirAll(filepath.Dir(cache), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cache, []byte("completed cache layer"), 0600); err != nil {
		t.Fatal(err)
	}
	workload := filepath.Join(stateRoot, "volumes", "committed-workload")
	if err := os.MkdirAll(workload, 0700); err != nil {
		t.Fatal(err)
	}
	if err := assignProject(workload, 10001); err != nil {
		t.Fatal(err)
	}
	initial, err := runtime.guestDiskSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	initialAvailable, valid := initial.AvailableBytes()
	const wantedAvailable uint64 = 5264236544
	if !valid || initialAvailable <= wantedAvailable {
		t.Fatal("fixture filesystem cannot supply recorded headroom")
	}
	quota := initialAvailable - wantedAvailable
	if err := setProjectQuota(device, 10001, quota); err != nil {
		t.Fatal(err)
	}
	namespaceFacts.labels["committed-workload"] = namespaceLabels(&vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 64 << 20, Pids: 64, DiskBytes: quota}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10001, "10.42.1.0/24")
	snapshot, err := runtime.guestDiskSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	available, valid := snapshot.AvailableBytes()
	if !valid || available < guestdisk.MinimumImportBytes || snapshot.Pressure() < guestdisk.PressureGC {
		t.Fatalf("fixture is not admitted under percentage pressure: %+v", snapshot)
	}
	reservation, err := runtime.reserveImport(ctx, &vmmdv1.ImportImageHeader{RequestId: "new-import", ManifestDigest: digest.String()})
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	if _, err := runtime.retainedImage(ctx, "controller", reference, digest.String(), built.RequestId); err != nil {
		t.Errorf("import with available=%d deleted retained source: %v", available, err)
	}
	if data, err := os.ReadFile(cache); err != nil || string(data) != "completed cache layer" {
		t.Errorf("import with available=%d deleted completed BuildKit cache: %q %v", available, data, err)
	}
	if reservation.remaining != available {
		t.Fatalf("reserved=%d available=%d", reservation.remaining, available)
	}
}
