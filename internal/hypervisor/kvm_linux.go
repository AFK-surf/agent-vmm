//go:build linux && (arm64 || amd64)

package hypervisor

// Derived from tinyrange/cc eed53ffb414a8bebcbb1d7934fab00558d78df00.
// Modified for agent-vmm; see LICENSE-cc.txt.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	kvmCheckExtension      = 0xae03
	kvmGetVcpuMmapSize     = 0xae04
	kvmCreateVCPU          = 0xae41
	kvmSetUserMemoryRegion = 0x4020ae46
	kvmRun                 = 0xae80
	kvmIRQLine             = 0x4008ae61
)

type kvmUserspaceMemoryRegion struct {
	Slot, Flags                              uint32
	GuestPhysAddr, MemorySize, UserspaceAddr uint64
}

type machine struct {
	mu                       sync.Mutex
	cfg                      Config
	devices                  []MMIODevice
	kvmfd, vmfd              int
	mem                      []byte
	running, started, closed bool
	cancel                   context.CancelFunc
	done                     chan struct{}
	console                  *serialOutput
	serial                   uart16550
	rtc                      pl031
}

func ioctl(fd uintptr, request uint64, arg uintptr) (uintptr, error) {
	for {
		r, _, errno := unix.Syscall(unix.SYS_IOCTL, fd, uintptr(request), arg)
		if errno == unix.EINTR && request != kvmRun {
			continue
		}
		if errno != 0 {
			return 0, errno
		}
		return r, nil
	}
}

func newMachine(cfg Config) (m *machine, err error) {
	m = &machine{cfg: cfg, kvmfd: -1, vmfd: -1, done: make(chan struct{}), console: newSerialOutput(cfg.Serial)}
	m.serial.irq = func(level bool) error { return m.setIRQ(uartIRQ, level) }
	m.serial.out = m.console
	allocated := m
	defer func() {
		if err != nil {
			_ = allocated.close()
		}
	}()
	m.kvmfd, err = unix.Open("/dev/kvm", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open /dev/kvm: %w", err)
	}
	// immediate_exit closes the race between cancellation and entering KVM_RUN.
	supported, err := ioctl(uintptr(m.kvmfd), kvmCheckExtension, 136)
	if err != nil || supported == 0 {
		return nil, fmt.Errorf("KVM requires KVM_CAP_IMMEDIATE_EXIT: %v", err)
	}
	if err = m.createVM(); err != nil {
		return nil, fmt.Errorf("create KVM VM: %w", err)
	}
	limit, err := ioctl(uintptr(m.vmfd), kvmCheckExtension, 66)
	if err != nil {
		return nil, fmt.Errorf("KVM_CAP_MAX_VCPUS: %w", err)
	}
	if uint64(cfg.CPUs) > uint64(limit) {
		return nil, fmt.Errorf("guest requests %d vCPUs; KVM supports at most %d", cfg.CPUs, limit)
	}
	m.mem, err = unix.Mmap(-1, 0, int(cfg.MemoryBytes), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, fmt.Errorf("map guest memory: %w", err)
	}
	region := kvmUserspaceMemoryRegion{GuestPhysAddr: m.memoryBase(), MemorySize: cfg.MemoryBytes, UserspaceAddr: uint64(uintptr(unsafe.Pointer(&m.mem[0])))}
	if _, err = ioctl(uintptr(m.vmfd), kvmSetUserMemoryRegion, uintptr(unsafe.Pointer(&region))); err != nil {
		return nil, fmt.Errorf("register guest memory: %w", err)
	}
	return m, nil
}

func (m *machine) memory() []byte { return m.mem }
func (m *machine) setDevices(devices []MMIODevice) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started || m.closed {
		return errors.New("cannot attach devices after VM execution begins")
	}
	for _, d := range devices {
		if err := validatePlatformDevice(d); err != nil {
			return err
		}
	}
	if err := validateDevices(devices); err != nil {
		return err
	}
	m.devices = append([]MMIODevice(nil), devices...)
	return nil
}

func (m *machine) setIRQ(irq uint32, level bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return os.ErrClosed
	}
	line := struct{ IRQ, Level uint32 }{IRQ: encodeIRQ(irq)}
	if level {
		line.Level = 1
	}
	_, err := ioctl(uintptr(m.vmfd), kvmIRQLine, uintptr(unsafe.Pointer(&line)))
	return err
}

func (m *machine) serialLoss() (dropped, failed uint64) { return m.console.loss() }

// run creates every vCPU before the platform interrupt controller is finalized,
// then gives each vCPU its own OS thread. The first vCPU to stop ends the VM.
func (m *machine) run(ctx context.Context) error {
	m.mu.Lock()
	if m.started || m.closed {
		m.mu.Unlock()
		return errors.New("VM can run only once")
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.running = true
	m.started = true
	m.mu.Unlock()
	defer func() { cancel(); m.mu.Lock(); m.running = false; close(m.done); m.mu.Unlock() }()
	fds := make([]int, 0, m.cfg.CPUs)
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	for i := range m.cfg.CPUs {
		fd, err := ioctl(uintptr(m.vmfd), kvmCreateVCPU, uintptr(i))
		if err != nil {
			return fmt.Errorf("create vCPU %d: %w", i, err)
		}
		fds = append(fds, int(fd))
	}
	cleanup, err := m.prepareCPUs(fds)
	if err != nil {
		return err
	}
	defer cleanup()
	mmapSize, err := ioctl(uintptr(m.kvmfd), kvmGetVcpuMmapSize, 0)
	if err != nil {
		return fmt.Errorf("KVM run mmap size: %w", err)
	}

	// SIGWINCH has a Go-managed handler and is independent of runtime preemption.
	// KVM_RUN returns EINTR after this handler; no custom signal handler or mask is installed.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, unix.SIGWINCH)
	defer signal.Stop(signals)
	results := make(chan error, len(fds))
	for _, fd := range fds {
		go func() { results <- m.runCPU(runCtx, fd, int(mmapSize)) }()
	}
	err = <-results
	cancel()
	for range len(fds) - 1 {
		<-results
	}
	return err
}

func (m *machine) runCPU(ctx context.Context, vcpufd, mmapSize int) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	run, err := unix.Mmap(vcpufd, 0, mmapSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("map vCPU run state: %w", err)
	}
	defer unix.Munmap(run)
	stopKick := make(chan struct{})
	kickerDone := make(chan struct{})
	tid := unix.Gettid()
	go func() {
		defer close(kickerDone)
		select {
		case <-ctx.Done():
		case <-stopKick:
			return
		}
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			atomic.StoreUint32((*uint32)(unsafe.Pointer(&run[0])), 0x100)
			_ = unix.Tgkill(unix.Getpid(), tid, unix.SIGWINCH)
			select {
			case <-stopKick:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { close(stopKick); <-kickerDone }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err = ioctl(uintptr(vcpufd), kvmRun, 0)
		if err == unix.EINTR || err == unix.EAGAIN {
			continue
		}
		if err != nil {
			return fmt.Errorf("KVM_RUN: %w", err)
		}
		exit := binary.LittleEndian.Uint32(run[8:12])
		switch exit {
		case 6:
			if err = m.handleMMIO(run[32:]); err != nil {
				return err
			}
		case 24:
			event := binary.LittleEndian.Uint32(run[32:36])
			if event == 1 {
				return nil
			}
			return fmt.Errorf("guest system event %d", event)
		default:
			done, err := m.handleExit(exit, run)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}
func (m *machine) handleMMIO(exit []byte) error {
	addr := binary.LittleEndian.Uint64(exit[:8])
	n := binary.LittleEndian.Uint32(exit[16:20])
	if n == 0 || n > 8 {
		return fmt.Errorf("invalid KVM MMIO length %d", n)
	}
	data := exit[8 : 8+n]
	write := exit[20] != 0
	if addr >= uartBase && addr-uint64(uartBase) < 8 {
		return m.serial.access(addr-uint64(uartBase), data, write)
	}
	if addr >= arm64RTCBase && addr-arm64RTCBase < 0x1000 {
		return m.rtc.access(addr-arm64RTCBase, data, write)
	}
	for _, d := range m.devices {
		if addr >= d.Base && addr-d.Base < d.Size && uint64(n) <= d.Size-(addr-d.Base) {
			if write {
				return d.Device.Write(addr-d.Base, data)
			}
			return d.Device.Read(addr-d.Base, data)
		}
	}
	return fmt.Errorf("unmapped guest MMIO at %#x (%d bytes)", addr, n)
}

func (m *machine) close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	if m.running {
		m.cancel()
		done := m.done
		m.mu.Unlock()
		<-done
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil
		}
	}
	m.closed = true
	var errs []error
	// Release KVM's references before unmapping the backing memory.
	if m.vmfd >= 0 {
		errs = append(errs, unix.Close(m.vmfd))
		m.vmfd = -1
	}
	if m.kvmfd >= 0 {
		errs = append(errs, unix.Close(m.kvmfd))
		m.kvmfd = -1
	}
	if m.mem != nil {
		errs = append(errs, unix.Munmap(m.mem))
		m.mem = nil
	}
	m.mu.Unlock()
	m.console.close()
	return errors.Join(errs...)
}
