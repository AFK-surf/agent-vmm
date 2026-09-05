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
	CapacityBytes          uint64
	UsedBytes              uint64
	WritableHeadroomBytes  uint64
	ImportReservationBytes uint64
	EmergencyBytes         uint64
	MetadataGrowthBytes    uint64
	AllocationUnitBytes    uint64
}

func (snapshot Snapshot) Safe() bool {
	committed := snapshot.UsedBytes
	for _, value := range []uint64{snapshot.WritableHeadroomBytes, snapshot.ImportReservationBytes, snapshot.EmergencyBytes, snapshot.MetadataGrowthBytes} {
		var carry uint64
		committed, carry = bits.Add64(committed, value, 0)
		if carry != 0 {
			return false
		}
	}
	return committed <= snapshot.CapacityBytes
}

func (snapshot Snapshot) Pressure() Pressure {
	if snapshot.CapacityBytes == 0 || snapshot.UsedBytes > snapshot.CapacityBytes || snapshot.ImportReservationBytes > snapshot.CapacityBytes-snapshot.UsedBytes {
		return PressureCritical
	}
	free := snapshot.CapacityBytes - snapshot.UsedBytes - snapshot.ImportReservationBytes
	if free <= snapshot.CapacityBytes/10 {
		return PressureCritical
	}
	if free <= snapshot.CapacityBytes*15/100 {
		return PressureRejectCold
	}
	if free <= snapshot.CapacityBytes/4 {
		return PressureGC
	}
	return PressureNormal
}

func (snapshot Snapshot) WithQuotaIncrease(increase uint64) error {
	var carry uint64
	snapshot.WritableHeadroomBytes, carry = bits.Add64(snapshot.WritableHeadroomBytes, increase, 0)
	if carry != 0 {
		return ErrCapacityUnavailable
	}
	if !snapshot.Safe() {
		return ErrCapacityUnavailable
	}
	return nil
}

// Budget serializes every physical commitment. Durable quota and import facts
// remain outside this lock and are supplied in Snapshot on each mutation.
// Model anchor: spec/tla/GuestDiskBudget.tla U+W+I+E+M<=D.
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

func (budget *Budget) AdmitQuota(load func(*Transaction) (Snapshot, error), increase uint64, commit func(*Transaction) error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.sealed {
		return ErrGrowthSealed
	}
	transaction := &Transaction{budget: budget}
	snapshot, err := load(transaction)
	if err != nil {
		return err
	}
	if err := snapshot.WithQuotaIncrease(increase); err != nil {
		return err
	}
	return commit(transaction)
}

// ReserveImportHeadroom gives a cold import every byte not already committed
// to physical usage, writable quotas, another import, or the fixed safety
// floors. The caller physically backs this reservation and converts it to U as
// containerd writes, so no archive compression ratio is assumed.
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
	committed := snapshot.UsedBytes
	for _, value := range []uint64{snapshot.WritableHeadroomBytes, snapshot.ImportReservationBytes, snapshot.EmergencyBytes, snapshot.MetadataGrowthBytes} {
		var carry uint64
		committed, carry = bits.Add64(committed, value, 0)
		if carry != 0 || committed > snapshot.CapacityBytes {
			return ErrCapacityUnavailable
		}
	}
	available := snapshot.CapacityBytes - committed
	unit := max(snapshot.AllocationUnitBytes, uint64(1))
	available -= available % unit
	if available < MinimumImportBytes {
		return ErrCapacityUnavailable
	}
	return commit(available)
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
