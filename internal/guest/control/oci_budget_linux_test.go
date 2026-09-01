//go:build linux

package control

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
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
