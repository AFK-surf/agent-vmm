//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
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

func TestPrepareOCIStorageBoundsNativeImagesOnControlDevice(t *testing.T) {
	base := t.TempDir()
	stateRoots := []string{filepath.Join(base, "state", "content"), filepath.Join(base, "state", "snapshots")}
	nativeRoot := filepath.Join(base, "control", "native")
	projects := make(map[string]uint32)
	var quotaDevice string
	var quotaProject uint32
	var quotaBytes uint64
	var tightenedDevice string
	err := prepareOCIStorage("state-device", "control-device", stateRoots, nativeRoot, os.MkdirAll,
		func(path string) (uint32, error) { return projects[path], nil },
		func(path string, project uint32) error { projects[path] = project; return nil },
		func(device string, project uint32, bytes uint64) error {
			quotaDevice, quotaProject, quotaBytes = device, project, bytes
			return nil
		},
		func(device string) error { tightenedDevice = device; return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range append(stateRoots, nativeRoot) {
		if projects[root] != ociProjectID {
			t.Fatalf("OCI root %s project=%d", root, projects[root])
		}
	}
	if quotaDevice != "control-device" || quotaProject != ociProjectID || quotaBytes != guestdisk.ControlImageBytes {
		t.Fatalf("control quota device=%q project=%d bytes=%d", quotaDevice, quotaProject, quotaBytes)
	}
	if tightenedDevice != "state-device" {
		t.Fatalf("state quota tightened on %q", tightenedDevice)
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
