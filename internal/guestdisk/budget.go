package guestdisk

import (
	"errors"
	"math/bits"
	"sync"
)

const (
	DefaultEmergencyBytes = uint64(4 << 30)
	DefaultMetadataBytes  = uint64(1 << 30)
	MinimumImportBytes    = uint64(2 << 30)
	ControlPartitionBytes = uint64(8 << 30)
	ControlSystemBytes    = uint64(4 << 30)
	ControlReserveBytes   = ControlPartitionBytes - ControlSystemBytes
	// ImportExpansionFactor bounds the physical peak of one declared OCI
	// archive: the staged archive (1x), its content-store blobs (1x), and the
	// unpacked layer snapshots (up to 3x, above the usual 2-3x gzip or zstd
	// ratio of container layers). An import that writes more fails at its
	// project quota and takes the normal rollback and GC path.
	ImportExpansionFactor = uint64(5)
)

var (
	ErrCapacityUnavailable = errors.New("storage capacity unavailable")
	ErrGrowthSealed        = errors.New("storage growth is sealed after project quota settlement failed; restart the Agent VMM guest")
)

type Pressure uint8

const (
	PressureNormal Pressure = iota
	PressureGC
	PressureRejectCold
	PressureCritical
)

type Snapshot struct {
	Inodes                   uint64
	FreeInodes               uint64
	CapacityBytes            uint64
	UsedBytes                uint64
	ImportReservationBytes   uint64
	NamespaceCommitmentBytes uint64
	EmergencyBytes           uint64
	MetadataGrowthBytes      uint64
	AllocationUnitBytes      uint64
}

func (snapshot Snapshot) SystemReserveBytes() (uint64, bool) {
	value, carry := bits.Add64(snapshot.EmergencyBytes, snapshot.MetadataGrowthBytes, 0)
	return value, carry == 0
}

func (snapshot Snapshot) AvailableBytes() (uint64, bool) {
	committed := snapshot.UsedBytes
	for _, value := range []uint64{snapshot.ImportReservationBytes, snapshot.NamespaceCommitmentBytes, snapshot.EmergencyBytes, snapshot.MetadataGrowthBytes} {
		var carry uint64
		committed, carry = bits.Add64(committed, value, 0)
		if carry != 0 || committed > snapshot.CapacityBytes {
			return 0, false
		}
	}
	available := snapshot.CapacityBytes - committed
	unit := max(snapshot.AllocationUnitBytes, uint64(1))
	return available - available%unit, true
}

func (snapshot Snapshot) Safe() bool {
	_, ok := snapshot.AvailableBytes()
	return ok
}

func (snapshot Snapshot) Pressure() Pressure {
	if snapshot.CapacityBytes == 0 || snapshot.UsedBytes > snapshot.CapacityBytes || snapshot.ImportReservationBytes > snapshot.CapacityBytes-snapshot.UsedBytes || snapshot.NamespaceCommitmentBytes > snapshot.CapacityBytes-snapshot.UsedBytes-snapshot.ImportReservationBytes {
		return PressureCritical
	}
	return max(pressureFromFree(snapshot.CapacityBytes, snapshot.CapacityBytes-snapshot.UsedBytes-snapshot.ImportReservationBytes-snapshot.NamespaceCommitmentBytes), pressureFromFree(snapshot.Inodes, snapshot.FreeInodes))
}

func pressureFromFree(total, free uint64) Pressure {
	if total == 0 {
		return PressureNormal
	}
	if free <= total/10 {
		return PressureCritical
	}
	if free <= total/100*15 {
		return PressureRejectCold
	}
	if free <= total/4 {
		return PressureGC
	}
	return PressureNormal
}

// Budget serializes every physical commitment. Durable quota and import facts
// remain outside this lock and are supplied in Snapshot on each mutation.
// Model anchor: spec/tla/GuestDiskBudget.tla physical import expansion.
type Budget struct {
	mu     sync.Mutex
	sealed bool
}

// Transaction is the only authority allowed to settle a temporary physical
// growth allowance. It is valid only while Budget holds its mutation lock.
type Transaction struct{ budget *Budget }

func (transaction *Transaction) Settle(settle func() error) error {
	if transaction.budget.sealed {
		return ErrGrowthSealed
	}
	if err := settle(); err != nil {
		transaction.budget.sealed = true
		return errors.Join(ErrGrowthSealed, err)
	}
	return nil
}

func (budget *Budget) GrowthError() error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.sealed {
		return ErrGrowthSealed
	}
	return nil
}

// ReserveImportHeadroom gives a materialization without a declared archive
// size, such as a build output or an unpack of present content, every byte not
// already committed to physical usage, another import, or the fixed safety
// floors. The caller physically backs this reservation and converts it to U as
// containerd writes.
func (budget *Budget) ReserveImportHeadroom(load func(*Transaction) (Snapshot, error), commit func(uint64) error) error {
	return budget.Mutate(func(transaction *Transaction) error {
		return transaction.ReserveImportHeadroom(load, commit)
	})
}

// Local materialization keeps reservation, conversion and settlement inside
// the existing transaction; streamed imports may retain their physical hold.
func (transaction *Transaction) ReserveImportHeadroom(load func(*Transaction) (Snapshot, error), commit func(uint64) error) error {
	if transaction.budget.sealed {
		return ErrGrowthSealed
	}
	snapshot, err := load(transaction)
	if err != nil {
		return err
	}
	available, ok := snapshot.AvailableBytes()
	if !ok {
		return ErrCapacityUnavailable
	}
	if available < MinimumImportBytes {
		return ErrCapacityUnavailable
	}
	return commit(available)
}

// ImportReservationBytes is the physical reservation of a declared archive.
// Import validation bounds archive sizes far below the overflow range.
func ImportReservationBytes(archiveBytes uint64) uint64 {
	return archiveBytes * ImportExpansionFactor
}

// ReserveImport reserves only the bounded peak of a declared archive, so the
// rest of the headroom stays available to namespaces and other writers.
func (transaction *Transaction) ReserveImport(load func(*Transaction) (Snapshot, error), required uint64, commit func(uint64) error) error {
	if transaction.budget.sealed {
		return ErrGrowthSealed
	}
	snapshot, err := load(transaction)
	if err != nil {
		return err
	}
	available, ok := snapshot.AvailableBytes()
	if !ok || available < required {
		return ErrCapacityUnavailable
	}
	return commit(required)
}

func (budget *Budget) Mutate(fn func(*Transaction) error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.sealed {
		return ErrGrowthSealed
	}
	return fn(&Transaction{budget: budget})
}

func (budget *Budget) Settle(settle func() error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.sealed {
		return ErrGrowthSealed
	}
	return (&Transaction{budget: budget}).Settle(settle)
}

// MutateInventory preserves namespace/GC serialization after import growth is
// sealed. These control operations do not enlarge the shared import project.
func (budget *Budget) MutateInventory(fn func(*Transaction) error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return fn(&Transaction{budget: budget})
}
