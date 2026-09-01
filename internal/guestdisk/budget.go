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

var ErrCapacityUnavailable = errors.New("storage capacity unavailable")

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
type Budget struct{ mu sync.Mutex }

func (budget *Budget) AdmitQuota(load func() (Snapshot, error), increase uint64, commit func() error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	snapshot, err := load()
	if err != nil {
		return err
	}
	if err := snapshot.WithQuotaIncrease(increase); err != nil {
		return err
	}
	return commit()
}

// ReserveImportHeadroom gives a cold import every byte not already committed
// to physical usage, writable quotas, another import, or the fixed safety
// floors. The caller physically backs this reservation and converts it to U as
// containerd writes, so no archive compression ratio is assumed.
func (budget *Budget) ReserveImportHeadroom(load func() (Snapshot, error), commit func(uint64) error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	snapshot, err := load()
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

func (budget *Budget) Mutate(fn func() error) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return fn()
}
