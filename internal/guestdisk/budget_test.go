package guestdisk

import (
	"errors"
	"math"
	"testing"
)

func TestSnapshotReportsCanonicalBudgetSummary(t *testing.T) {
	snapshot := Snapshot{
		CapacityBytes: 64 << 30, UsedBytes: 10 << 30,
		ImportReservationBytes: 2 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30,
		AllocationUnitBytes: 4096,
	}
	reserve, ok := snapshot.SystemReserveBytes()
	if !ok || reserve != 5<<30 {
		t.Fatalf("system reserve=(%d, %v), want (5 GiB, true)", reserve, ok)
	}
	available, ok := snapshot.AvailableBytes()
	if !ok || available != 47<<30 {
		t.Fatalf("available=(%d, %v), want (47 GiB, true)", available, ok)
	}
}

func TestSnapshotReportsZeroAvailableWhenCommitmentsExceedCapacity(t *testing.T) {
	snapshot := Snapshot{
		CapacityBytes: 32 << 30, UsedBytes: 30 << 30,
		EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30, AllocationUnitBytes: 4096,
	}
	available, ok := snapshot.AvailableBytes()
	if ok || available != 0 {
		t.Fatalf("available=(%d, %v), want (0, false)", available, ok)
	}
}

func TestOverflowFailsClosed(t *testing.T) {
	snapshot := Snapshot{CapacityBytes: math.MaxUint64, UsedBytes: math.MaxUint64 - 3, EmergencyBytes: 4}
	if snapshot.Safe() {
		t.Fatal("checked invariant accepted overflowing commitments")
	}
	snapshot = Snapshot{CapacityBytes: math.MaxUint64, ImportReservationBytes: math.MaxUint64 - 1}
	if err := new(Budget).ReserveImportHeadroom(func(*Transaction) (Snapshot, error) { return snapshot, nil }, func(uint64) error { return nil }); err == nil {
		t.Fatal("import reservation overflow was accepted")
	}
}

func TestImportHasTwoGiBMinimumReservation(t *testing.T) {
	for _, available := range []uint64{7 << 30, 2551894016, MinimumImportBytes, MinimumImportBytes - 4096} {
		snapshot := Snapshot{CapacityBytes: 64 << 30, NamespaceCommitmentBytes: 8<<30 - 452395008, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30, AllocationUnitBytes: 4096}
		snapshot.UsedBytes = snapshot.CapacityBytes - snapshot.NamespaceCommitmentBytes - snapshot.EmergencyBytes - snapshot.MetadataGrowthBytes - available
		var reserved uint64
		err := new(Budget).ReserveImportHeadroom(func(*Transaction) (Snapshot, error) { return snapshot, nil }, func(value uint64) error { reserved = value; return nil })
		if available < MinimumImportBytes {
			if !errors.Is(err, ErrCapacityUnavailable) || reserved != 0 {
				t.Fatalf("insufficient headroom reserved=%d err=%v", reserved, err)
			}
		} else if err != nil || reserved != available {
			t.Fatalf("available=%d reserved=%d err=%v", available, reserved, err)
		}
	}
}

func TestColdImportReservesAllUncommittedHeadroomWithoutCompressionGuess(t *testing.T) {
	for _, test := range []struct {
		name     string
		snapshot Snapshot
		reserved uint64
	}{
		{name: "normal", snapshot: Snapshot{CapacityBytes: 32 << 30, UsedBytes: 13 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}, reserved: 14 << 30},
		{name: "committed guest below pressure watermark", snapshot: Snapshot{CapacityBytes: 55837716480, UsedBytes: 15151292416, NamespaceCommitmentBytes: 30053478400, EmergencyBytes: DefaultEmergencyBytes, MetadataGrowthBytes: DefaultMetadataBytes, AllocationUnitBytes: 4096, Inodes: 3670016, FreeInodes: 3475784}, reserved: 5264236544},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reserved uint64
			if err := new(Budget).ReserveImportHeadroom(func(*Transaction) (Snapshot, error) { return test.snapshot, nil }, func(value uint64) error { reserved = value; return nil }); err != nil {
				t.Fatal(err)
			}
			if reserved != test.reserved {
				t.Fatalf("reserved=%d want=%d", reserved, test.reserved)
			}
		})
	}
}

func TestDiskPressureWatermarks(t *testing.T) {
	for _, test := range []struct {
		used uint64
		want Pressure
	}{{24 << 30, PressureGC}, {28 << 30, PressureRejectCold}, {29 << 30, PressureCritical}} {
		if got := (Snapshot{CapacityBytes: 32 << 30, UsedBytes: test.used}).Pressure(); got != test.want {
			t.Fatalf("used=%d pressure=%d want=%d", test.used, got, test.want)
		}
	}
}

func TestInodePressureRejectsColdGrowthWithFreeBytes(t *testing.T) {
	snapshot := Snapshot{CapacityBytes: 64 << 30, Inodes: 10000, FreeInodes: 1500}
	if snapshot.Pressure() != PressureRejectCold {
		t.Fatal("inode pressure ignored")
	}
	snapshot.FreeInodes = 1000
	if snapshot.Pressure() != PressureCritical {
		t.Fatal("critical inode pressure ignored")
	}
}

func TestControlPartitionBudgetsLeaveFixedReserve(t *testing.T) {
	if ControlSystemBytes+ControlReserveBytes != ControlPartitionBytes {
		t.Fatal("control filesystem budgets do not cover the partition")
	}
	if ControlReserveBytes != 4<<30 {
		t.Fatalf("control filesystem reserve=%d", ControlReserveBytes)
	}
}
