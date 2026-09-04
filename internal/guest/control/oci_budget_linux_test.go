//go:build linux

package control

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
	"github.com/containerd/containerd/v2/core/leases"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

func TestOCIProjectAssignmentOwnsGlobalTreeAndPreservesWritableSnapshotProject(t *testing.T) {
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
	projects := map[string]uint32{writable: 10001}
	read := func(path string) (uint32, error) { return projects[path], nil }
	assign := func(path string, id uint32) error { projects[path] = id; return nil }
	if err := assignOCIProjectTree(root, read, assign); err != nil {
		t.Fatal(err)
	}
	if projects[root] != ociProjectID || projects[global] != ociProjectID {
		t.Fatalf("global OCI tree projects=%v", projects)
	}
	if projects[writable] != 10001 {
		t.Fatalf("workload writable snapshot project=%d", projects[writable])
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

func TestOCIProjectAssignmentFailsClosedBeforeImport(t *testing.T) {
	root := t.TempDir()
	want := errors.New("project quota unavailable")
	err := assignOCIProjectTree(root, func(string) (uint32, error) { return 0, nil }, func(string, uint32) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("assignment error=%v", err)
	}
}

func TestOCIProjectAssignmentRejectsForeignRoot(t *testing.T) {
	root := t.TempDir()
	err := assignOCIProjectTree(root, func(path string) (uint32, error) {
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
	// The kernel project quota sees only OCI usage and its I allowance. W, E,
	// and M are deliberately absent, so an arbitrarily fast unpack hits EDQUOT
	// at this exact ceiling instead of borrowing their physical headroom.
	if _, err := ociProjectQuotaLimit(math.MaxUint64, 1); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		t.Fatalf("overflow error=%v", err)
	}
}

type leaseBarrierStub struct {
	events    []string
	createErr error
	deleteErr error
	sync      bool
}

func (stub *leaseBarrierStub) Create(context.Context, ...leases.Opt) (leases.Lease, error) {
	stub.events = append(stub.events, "create")
	return leases.Lease{ID: "barrier"}, stub.createErr
}

func (stub *leaseBarrierStub) Delete(ctx context.Context, _ leases.Lease, options ...leases.DeleteOpt) error {
	stub.events = append(stub.events, "delete")
	settings := new(leases.DeleteOptions)
	for _, option := range options {
		if err := option(ctx, settings); err != nil {
			return err
		}
	}
	stub.sync = settings.Synchronous
	return stub.deleteErr
}

func TestOCIQuotaSettlementWaitsForSynchronousGCBeforeExactTighten(t *testing.T) {
	barrier := new(leaseBarrierStub)
	err := settleOCIProjectQuotaWith(context.Background(), barrier, func() error {
		barrier.events = append(barrier.events, "tighten")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(barrier.events, ","); got != "create,delete,tighten" || !barrier.sync {
		t.Fatalf("settlement order=%s synchronous=%t", got, barrier.sync)
	}
}

func TestOCIQuotaSettlementFailureDoesNotTighten(t *testing.T) {
	want := errors.New("GC barrier unavailable")
	barrier := &leaseBarrierStub{deleteErr: want}
	tightened := false
	err := settleOCIProjectQuotaWith(context.Background(), barrier, func() error {
		tightened = true
		return nil
	})
	if !errors.Is(err, want) || tightened {
		t.Fatalf("settlement error=%v tightened=%t", err, tightened)
	}
}

func TestOCIStartupAllowanceIsOnlyTheAccountedMetadataFloor(t *testing.T) {
	limit, err := ociProjectQuotaLimit(11<<30, guestdisk.DefaultMetadataBytes)
	if err != nil || limit != 12<<30 {
		t.Fatalf("startup limit=%d err=%v", limit, err)
	}
}

func TestOCIMetadataAllowancePreservesEveryCommittedFloor(t *testing.T) {
	snapshot := guestdisk.Snapshot{CapacityBytes: 20 << 30, UsedBytes: 5 << 30, WritableHeadroomBytes: 4 << 30,
		ImportReservationBytes: 2 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}
	allowance, err := ociMetadataAllowance(snapshot, 3<<30)
	if err != nil || allowance != 1<<30 {
		t.Fatalf("metadata allowance=%d err=%v", allowance, err)
	}
	if _, err := ociMetadataAllowance(snapshot, 4<<30); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
		t.Fatalf("metadata growth borrowed committed floor: %v", err)
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

func TestSealedOCIProjectQuotaGateRejectsEveryGrowthCallback(t *testing.T) {
	runtime := &Runtime{diskBudget: new(guestdisk.Budget), materializer: new(ocimaterializer.Manager)}
	_ = runtime.diskBudget.Settle(func() error { return errors.New("persistent settlement failure") })
	callbacks := 0
	for name, guard := range map[string]func(func() error) error{
		"metadata": func(run func() error) error {
			return runtime.guardOCIMetadataGrowth(context.Background(), run)
		},
		"metadata-locked": func(run func() error) error {
			return runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
				return runtime.guardOCIMetadataGrowthLockedWithCommit(context.Background(), transaction, 1, run)
			})
		},
		"physical": func(run func() error) error {
			return runtime.guardOCIPhysicalGrowth(context.Background(), "sealed-test", run)
		},
	} {
		err := guard(func() error { callbacks++; return nil })
		if !errors.Is(err, guestdisk.ErrGrowthSealed) {
			t.Fatalf("%s gate error=%v", name, err)
		}
	}
	if callbacks != 0 {
		t.Fatalf("sealed OCI gate ran %d growth callbacks", callbacks)
	}
}

func TestOCIMetadataAllowanceIsOnlySlackBeyondCommittedFloors(t *testing.T) {
	snapshot := guestdisk.Snapshot{CapacityBytes: 20 << 30, UsedBytes: 5 << 30, WritableHeadroomBytes: 4 << 30,
		ImportReservationBytes: 2 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}
	const additionalWritable = uint64(3 << 30)
	allowance, err := ociMetadataAllowance(snapshot, additionalWritable)
	if err != nil {
		t.Fatal(err)
	}
	consumed := snapshot.UsedBytes + allowance + snapshot.WritableHeadroomBytes + additionalWritable +
		snapshot.ImportReservationBytes + snapshot.EmergencyBytes + snapshot.MetadataGrowthBytes
	if consumed > snapshot.CapacityBytes || allowance != snapshot.CapacityBytes-(consumed-allowance) {
		t.Fatalf("full metadata allowance breaks U+W+I+E+M<=D: allowance=%d committed_after=%d capacity=%d", allowance, consumed, snapshot.CapacityBytes)
	}
}

func TestGuestStartupAllowanceClosesToExactOCIIdleCeiling(t *testing.T) {
	if os.Getenv("AGENT_VMM_PRIVILEGED_QUOTA_TEST") != "1" || os.Geteuid() != 0 {
		t.Skip("privileged project-quota integration is not enabled")
	}
	if entries, err := os.ReadDir(stateRoot); err == nil && len(entries) != 0 {
		t.Skip("refusing to mount over a non-empty /state")
	}
	backing := filepath.Join(t.TempDir(), "state.raw")
	file, err := os.OpenFile(backing, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Fallocate(int(file.Fd()), 0, 0, 256<<20); err != nil {
		file.Close()
		t.Skipf("fallocate unavailable: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("losetup", "--find", "--show", backing).CombinedOutput()
	if err != nil {
		t.Skipf("loop device unavailable: %v: %s", err, output)
	}
	device := strings.TrimSpace(string(output))
	defer exec.Command("losetup", "--detach", device).Run()
	if output, err = exec.Command("mkfs.ext4", "-F", "-q", "-O", "project,quota", "-E", "nodiscard,lazy_itable_init=0,lazy_journal_init=0", device).CombinedOutput(); err != nil {
		t.Fatalf("mkfs.ext4: %v: %s", err, output)
	}
	if err = os.MkdirAll(stateRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err = unix.Mount(device, stateRoot, "ext4", unix.MS_NOATIME, "prjquota"); err != nil {
		t.Skipf("project-quota mount unavailable: %v", err)
	}
	defer func() {
		_ = unix.Unmount(stateRoot, 0)
		_ = os.Remove(stateRoot)
	}()
	if err = os.MkdirAll(containerdRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = assignProject(containerdRoot, ociProjectID); err != nil {
		t.Fatalf("assign first-boot OCI root: %v", err)
	}
	seed := filepath.Join(containerdRoot, "metadata", "seed")
	if err = os.MkdirAll(filepath.Dir(seed), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(seed, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := &Runtime{diskBudget: new(guestdisk.Budget)}
	_ = previous.diskBudget.Settle(func() error { return errors.New("persistent settlement failure") })
	if !errors.Is(previous.ociGrowthError(), guestdisk.ErrGrowthSealed) {
		t.Fatal("old process was not terminally sealed")
	}
	startup, prepareErr := PrepareOCIStorage(device)
	if prepareErr != nil {
		t.Fatalf("open restart startup allowance: %v", prepareErr)
	}
	if !startup.Existing {
		t.Fatal("non-empty OCI tree was classified as fresh")
	}
	before, err := projectUsage(device, ociProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if _, lookupErr := exec.LookPath("setpriv"); lookupErr != nil {
		t.Skip("setpriv is unavailable")
	}
	if err = os.Chmod(containerdRoot, 0o777); err != nil {
		t.Fatal(err)
	}
	output, err = exec.Command("setpriv", "--reuid=65534", "--regid=65534", "--clear-groups", "--bounding-set=-sys_resource", "--inh-caps=-all", "--ambient-caps=-all",
		"dd", "if=/dev/zero", "of="+filepath.Join(containerdRoot, "after-restart"), "bs=1M", "count=2", "conv=fsync").CombinedOutput()
	if err != nil {
		t.Fatalf("startup metadata allowance was unavailable: err=%v output=%s", err, output)
	}
	if err = tightenOCIProjectQuota(device); err != nil {
		t.Fatalf("close startup allowance: %v", err)
	}
	output, err = exec.Command("setpriv", "--reuid=65534", "--regid=65534", "--clear-groups", "--bounding-set=-sys_resource", "--inh-caps=-all", "--ambient-caps=-all",
		"dd", "if=/dev/zero", "of="+filepath.Join(containerdRoot, "after-settlement"), "bs=1M", "count=2", "conv=fsync").CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Disk quota exceeded") && !strings.Contains(string(output), "No space left on device") {
		t.Fatalf("settlement did not restore exact idle ceiling: err=%v output=%s", err, output)
	}
	after, usageErr := projectUsage(device, ociProjectID)
	if usageErr != nil || after > before+(3<<20) {
		t.Fatalf("restart idle ceiling drifted: before=%d after=%d err=%v", before, after, usageErr)
	}
}

func TestOCIStartupSettlementDistinguishesFreshAndExistingState(t *testing.T) {
	for _, test := range []struct {
		name     string
		startup  OCIStartupState
		expected string
	}{{name: "fresh", expected: "tighten"}, {name: "existing", startup: OCIStartupState{Existing: true}, expected: "barrier"}} {
		t.Run(test.name, func(t *testing.T) {
			called := ""
			err := settleOCIStartupQuota(test.startup, func() error { called = "barrier"; return nil }, func() error { called = "tighten"; return nil })
			if err != nil || called != test.expected {
				t.Fatalf("settlement called=%q err=%v want=%q", called, err, test.expected)
			}
		})
	}
}

func TestImportReservationIsPhysicallyBackedAndReleased(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "reservation")
	hold := path + ".hold"
	if err := writeImportReservation(path, hold, 8<<20); err != nil {
		t.Fatal(err)
	}
	reserved, err := importReservationBytes(directory)
	if err != nil || reserved < 8<<20 {
		t.Fatalf("physical reservation=%d err=%v", reserved, err)
	}
	reservation := &importReservation{path: path, holdPath: hold, remaining: 8 << 20}
	if err := reservation.Consume(3 << 20); err != nil {
		t.Fatal(err)
	}
	shrunk, err := importReservationBytes(directory)
	if err != nil || shrunk >= reserved || shrunk < 5<<20 {
		t.Fatalf("converted reservation=%d before=%d err=%v", shrunk, reserved, err)
	}
	reservation.Release()
	if _, err := os.Stat(hold); !os.IsNotExist(err) {
		t.Fatalf("completed import retained physical hold: %v", err)
	}
}
