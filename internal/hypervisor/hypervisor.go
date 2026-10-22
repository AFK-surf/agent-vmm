package hypervisor

import (
	"context"
	"errors"
	"io"
	"math"
)

var ErrUnsupported = errors.New("hypervisor backend is not supported on this platform")

type MMIO interface {
	Read(offset uint64, data []byte) error
	Write(offset uint64, data []byte) error
}

type MMIODevice struct {
	Base   uint64
	Size   uint64
	IRQ    uint32
	Device MMIO
}

type Config struct {
	MemoryBytes  uint64
	CPUs         int
	EnableHyperV bool
	Kernel       []byte
	Initramfs    []byte
	CommandLine  string
	Serial       io.Writer
}

type VM struct{ machine *machine }

func New(cfg Config) (*VM, error) {
	if cfg.CPUs < 1 {
		return nil, errors.New("guest requires at least one vCPU")
	}
	if cfg.MemoryBytes < 64<<20 || cfg.MemoryBytes > uint64(math.MaxInt) || cfg.MemoryBytes%4096 != 0 {
		return nil, errors.New("guest memory must be page aligned and at least 64 MiB")
	}
	if len(cfg.Kernel) == 0 {
		return nil, errors.New("guest kernel is empty")
	}
	if cfg.Serial == nil {
		cfg.Serial = io.Discard
	}
	m, err := newMachine(cfg)
	if err != nil {
		return nil, err
	}
	return &VM{machine: m}, nil
}

func validateDevices(devices []MMIODevice) error {
	for i, d := range devices {
		for _, other := range devices[:i] {
			if d.Base < other.Base+other.Size && other.Base < d.Base+d.Size || d.IRQ == other.IRQ {
				return errors.New("MMIO devices must use disjoint ranges and interrupts")
			}
		}
	}
	return nil
}

// SetDevices binds transports after their constructors receive the mapped RAM.
// Call it before Run. Device workers must stop before Close unmaps their RAM.
func (v *VM) SetDevices(devices []MMIODevice) error { return v.machine.setDevices(devices) }

func (v *VM) Memory() []byte                      { return v.machine.memory() }
func (v *VM) MemoryBase() uint64                  { return v.machine.memoryBase() }
func (v *VM) SetIRQ(irq uint32, level bool) error { return v.machine.setIRQ(irq, level) }
func (v *VM) Run(ctx context.Context) error       { return v.machine.run(ctx) }
func (v *VM) Close() error                        { return v.machine.close() }

// SerialLoss reports console bytes dropped on a full buffer and bytes lost to host write errors.
func (v *VM) SerialLoss() (dropped, failed uint64) { return v.machine.serialLoss() }
