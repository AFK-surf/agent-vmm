package guestdisk

import (
	"math"
	"sync"
	"testing"
)

func TestQuotaAdmissionPreservesPhysicalInvariant(t *testing.T) {
	snapshot := Snapshot{CapacityBytes: 32 << 30, UsedBytes: 13 << 30, WritableHeadroomBytes: 4 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}
	var committed bool
	load := func(*Transaction) (Snapshot, error) { return snapshot, nil }
	if err := new(Budget).AdmitQuota(load, 2<<30, func(*Transaction) error { committed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("safe quota was not committed")
	}
	if err := new(Budget).AdmitQuota(load, 11<<30, func(*Transaction) error { t.Fatal("unsafe quota commit ran"); return nil }); err == nil {
		t.Fatal("unsafe quota admitted")
	}
}

func TestConcurrentQuotaAdmissionsReloadFactsInsideSingleGlobalGate(t *testing.T) {
	budget := new(Budget)
	snapshot := Snapshot{CapacityBytes: 8, UsedBytes: 1, WritableHeadroomBytes: 2, EmergencyBytes: 1, MetadataGrowthBytes: 1}
	var facts sync.Mutex
	load := func(*Transaction) (Snapshot, error) { facts.Lock(); defer facts.Unlock(); return snapshot, nil }
	commit := func(*Transaction) error {
		facts.Lock()
		snapshot.WritableHeadroomBytes += 2
		facts.Unlock()
		return nil
	}
	errors := make(chan error, 2)
	for range 2 {
		go func() { errors <- budget.AdmitQuota(load, 2, commit) }()
	}
	var admitted int
	for range 2 {
		if err := <-errors; err == nil {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("concurrent admissions=%d want=1", admitted)
	}
}

func TestOverflowFailsClosed(t *testing.T) {
	snapshot := Snapshot{CapacityBytes: math.MaxUint64, UsedBytes: math.MaxUint64 - 3, WritableHeadroomBytes: 2, EmergencyBytes: 2}
	if snapshot.Safe() {
		t.Fatal("checked invariant accepted overflowing commitments")
	}
	if err := snapshot.WithQuotaIncrease(math.MaxUint64); err == nil {
		t.Fatal("quota overflow was accepted")
	}
	snapshot = Snapshot{CapacityBytes: math.MaxUint64, ImportReservationBytes: math.MaxUint64 - 1}
	if err := new(Budget).ReserveImportHeadroom(func(*Transaction) (Snapshot, error) { return snapshot, nil }, func(uint64) error { return nil }); err == nil {
		t.Fatal("import reservation overflow was accepted")
	}
}

func TestRetainedHeadroomAndCacheRemainInInvariant(t *testing.T) {
	base := Snapshot{CapacityBytes: 32 << 30, UsedBytes: 19 << 30, WritableHeadroomBytes: 6 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}
	if !base.Safe() {
		t.Fatal("baseline should be safe")
	}
	actual := base
	actual.UsedBytes += 4 << 30
	if actual.Safe() {
		t.Fatal("cache growth should violate retained headroom")
	}
	unsafeModel := actual
	unsafeModel.WritableHeadroomBytes = 4 << 30
	if !unsafeModel.Safe() {
		t.Fatal("counterexample did not show early retained release masking unsafe cache growth")
	}
}

func TestImportHasTwoGiBMinimumReservation(t *testing.T) {
	snapshot := Snapshot{CapacityBytes: 32 << 30, UsedBytes: 20 << 30, WritableHeadroomBytes: 4 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}
	var reserved uint64
	if err := new(Budget).ReserveImportHeadroom(func(*Transaction) (Snapshot, error) { return snapshot, nil }, func(value uint64) error { reserved = value; return nil }); err != nil {
		t.Fatal(err)
	}
	if reserved != 3<<30 {
		t.Fatalf("reserved = %d", reserved)
	}
}

func TestColdImportReservesAllUncommittedHeadroomWithoutCompressionGuess(t *testing.T) {
	snapshot := Snapshot{CapacityBytes: 32 << 30, UsedBytes: 13 << 30, WritableHeadroomBytes: 6 << 30, EmergencyBytes: 4 << 30, MetadataGrowthBytes: 1 << 30}
	var reserved uint64
	if err := new(Budget).ReserveImportHeadroom(func(*Transaction) (Snapshot, error) { return snapshot, nil }, func(value uint64) error { reserved = value; return nil }); err != nil {
		t.Fatal(err)
	}
	if reserved != 8<<30 {
		t.Fatalf("reserved=%d want=%d", reserved, uint64(8<<30))
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
