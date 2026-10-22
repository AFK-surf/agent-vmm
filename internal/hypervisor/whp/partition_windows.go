//go:build windows && (amd64 || arm64)

package whp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Partition owns one vCPU and its RAM. Run returns a raw platform exit for the
// caller's instruction/device emulator; it never treats an unhandled exit as success.
type Partition struct {
	mu                       sync.Mutex
	handle                   windows.Handle
	memory                   uintptr
	size                     uint64
	running, closed, closing bool
	done                     chan struct{}
	cancel                   context.CancelFunc
}

type Exit [512]byte

func New(memoryBytes, guestAddress uint64) (p *Partition, err error) {
	if memoryBytes == 0 || memoryBytes%4096 != 0 || guestAddress%4096 != 0 || guestAddress+memoryBytes < guestAddress || memoryBytes > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("WHP memory must use page-aligned, non-overflowing addresses")
	}
	present, err := Present()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, errors.New("Windows Hypervisor Platform is not present")
	}
	p = &Partition{size: memoryBytes}
	allocated := p
	defer func() {
		if err != nil {
			_ = allocated.Close()
		}
	}()
	if err = call(createPartition, uintptr(unsafe.Pointer(&p.handle))); err != nil {
		return nil, err
	}
	cpus := uint32(1)
	if err = call(setProperty, uintptr(p.handle), 0x1fff, uintptr(unsafe.Pointer(&cpus)), 4); err != nil {
		return nil, err
	}
	if err = configureInterruptController(p.handle); err != nil {
		return nil, err
	}
	if err = call(setupPartition, uintptr(p.handle)); err != nil {
		return nil, err
	}
	p.memory, err = windows.VirtualAlloc(0, uintptr(memoryBytes), windows.MEM_RESERVE|windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		return nil, err
	}
	if err = call(mapGPA, uintptr(p.handle), p.memory, uintptr(guestAddress), uintptr(memoryBytes), 7); err != nil {
		return nil, err
	}
	if err = call(createVP, uintptr(p.handle), 0, 0); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Partition) Memory() []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&p.memory)), int(p.size))
}
func (p *Partition) SetRegisters(names []uint32, values []RegisterValue) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return os.ErrClosed
	}
	if p.running {
		return errors.New("WHP CPU is running")
	}
	return registers(setRegisters, p.handle, names, values)
}
func (p *Partition) GetRegisters(names []uint32, values []RegisterValue) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return os.ErrClosed
	}
	if p.running {
		return errors.New("WHP CPU is running")
	}
	return registers(getRegisters, p.handle, names, values)
}

func (p *Partition) Run(ctx context.Context) (Exit, error) {
	p.mu.Lock()
	if p.closed || p.closing {
		p.mu.Unlock()
		return Exit{}, os.ErrClosed
	}
	if p.running {
		p.mu.Unlock()
		return Exit{}, errors.New("WHP CPU is already running")
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return Exit{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	p.cancel = cancel
	p.running = true
	p.done = make(chan struct{})
	handle := p.handle
	p.mu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	done := make(chan struct{})
	kickerDone := make(chan struct{})
	go func() {
		defer close(kickerDone)
		select {
		case <-runCtx.Done():
		case <-done:
			return
		}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			_ = call(cancelVP, uintptr(handle), 0, 0)
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	var exit Exit
	err := call(runVP, uintptr(handle), 0, uintptr(unsafe.Pointer(&exit[0])), uintptr(len(exit)))
	close(done)
	<-kickerDone
	p.mu.Lock()
	p.running = false
	close(p.done)
	p.mu.Unlock()
	if ctx.Err() != nil {
		return exit, ctx.Err()
	}
	return exit, err
}

func (p *Partition) Cancel() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return os.ErrClosed
	}
	if p.running {
		p.cancel()
		return nil
	}
	return call(cancelVP, uintptr(p.handle), 0, 0)
}
func (p *Partition) Interrupt(vector uint32, asserted bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return os.ErrClosed
	}
	return injectInterrupt(p.handle, vector, asserted)
}
func (p *Partition) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closing = true
	if p.running {
		p.cancel()
		done := p.done
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil
		}
	}
	p.closed = true
	var errs []error
	if p.handle != 0 {
		// Deleting the partition releases all GPA references before VirtualFree.
		_ = call(deleteVP, uintptr(p.handle), 0)
		if err := call(deletePartition, uintptr(p.handle)); err != nil {
			p.closed = false
			p.closing = false
			p.mu.Unlock()
			return err
		}
		p.handle = 0
	}
	if p.memory != 0 {
		errs = append(errs, windows.VirtualFree(p.memory, 0, windows.MEM_RELEASE))
		p.memory = 0
		p.size = 0
	}
	p.mu.Unlock()
	return errors.Join(errs...)
}
