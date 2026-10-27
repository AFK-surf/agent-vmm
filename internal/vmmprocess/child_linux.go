//go:build linux

package vmmprocess

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"github.com/AFK-surf/agent-vmm/internal/hypervisor"
	"github.com/AFK-surf/agent-vmm/internal/hypervisor/virtio"
	"golang.org/x/sys/unix"
)

// Virtio MMIO windows follow the arm64 platform's device range; IRQs are SPIs.
const mmioBase = 0x0a000000

var deviceIRQs = [...]uint32{32, 33, 34, 35, 36}

func isRefused(err error) bool { return errors.Is(err, unix.ECONNREFUSED) }

// Running probes the live VM's open file description lock without taking it,
// so a concurrent start never observes a lock held by the probe. flock(2)
// does not observe this lock.
func Running(directory string) (bool, error) {
	file, err := os.Open(filepath.Join(directory, "vmm.lock"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	lock := unix.Flock_t{Type: unix.F_WRLCK}
	if err = unix.FcntlFlock(file.Fd(), unix.F_OFD_GETLK, &lock); err != nil {
		return false, err
	}
	return lock.Type != unix.F_UNLCK, nil
}

func lockInstance(file *os.File) error {
	return unix.FcntlFlock(file.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{Type: unix.F_WRLCK})
}

type child struct {
	config   Config
	launch   string
	mu       sync.Mutex
	state    hostv1.VMState
	session  string
	streams  map[net.Conn]bool
	socket   *virtio.VSock
	cancel   context.CancelFunc
	done     chan struct{}
	observed chan struct{}
	once     sync.Once
	incoming chan net.Conn
}

func RunChild() error {
	file := os.NewFile(3, "VMM config")
	if file == nil {
		return errors.New("missing VMM config fd")
	}
	var spec launchSpec
	err := readMessage(bufio.NewReaderSize(file, 16<<10), &spec)
	config := spec.Config
	file.Close()
	if err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(config.InstanceDirectory, "vmm.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = lockInstance(lock); err != nil {
		return fmt.Errorf("VM instance already running: %w", err)
	}
	ready := os.NewFile(4, "VMM ready")
	paths := []string{config.KernelPath, config.InitramfsPath, config.RootFSPath, config.StatePath}
	files := make([]*os.File, len(paths))
	for i, path := range paths {
		files[i] = os.NewFile(uintptr(i+5), path)
		defer files[i].Close()
	}
	kernel, err := io.ReadAll(files[0])
	if err != nil {
		return err
	}
	initrd, err := io.ReadAll(files[1])
	if err != nil {
		return err
	}
	serial, err := os.OpenFile(config.SerialLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer serial.Close()
	vm, err := hypervisor.New(hypervisor.Config{MemoryBytes: config.MemoryBytes, CPUs: int(config.CPUCount), Kernel: kernel, Initramfs: initrd, CommandLine: config.CommandLine, Serial: serial})
	if err != nil {
		return err
	}
	defer vm.Close()
	mem := virtio.Memory{Base: vm.MemoryBase(), Data: vm.Memory()}
	devices := []hypervisor.MMIODevice{}
	closers := []io.Closer{}
	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()
	add := func(device hypervisor.MMIO) {
		devices = append(devices, hypervisor.MMIODevice{Base: mmioBase + uint64(len(devices))*0x1000, Size: 0x1000, IRQ: deviceIRQs[len(devices)], Device: device})
	}
	for i, disk := range files[2:] {
		irq := deviceIRQs[len(devices)]
		id := []string{"AGENT_ROOT", "AGENT_STATE", "AGENT_STATE_LEGACY"}[i]
		block, e := virtio.NewBlock(mem, func(level bool) error { return vm.SetIRQ(irq, level) }, disk, i != 1, id)
		if e != nil {
			return e
		}
		closers = append(closers, block)
		add(block)
	}
	rngIRQ := deviceIRQs[len(devices)]
	rng := virtio.NewRNG(mem, func(level bool) error { return vm.SetIRQ(rngIRQ, level) })
	closers = append(closers, rng)
	add(rng)
	sockIRQ := deviceIRQs[len(devices)]
	socket := virtio.NewVSock(mem, func(level bool) error { return vm.SetIRQ(sockIRQ, level) }, 3)
	closers = append(closers, socket)
	add(socket)
	if err = vm.SetDevices(devices); err != nil {
		return err
	}
	egress, err := socket.Listen(egresslink.EgressPort)
	if err != nil {
		return err
	}
	defer egress.Close()
	path := socketPath(config)
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(path, 0600); err != nil {
		return err
	}
	if _, err = ready.Write([]byte{1}); err != nil {
		return err
	}
	ready.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &child{config: config, launch: spec.Launch, state: hostv1.VMState_VM_STATE_RUNNING, streams: make(map[net.Conn]bool), socket: socket, cancel: cancel, done: make(chan struct{}), observed: make(chan struct{}), incoming: make(chan net.Conn)}
	go func() {
		for {
			conn, e := egress.Accept()
			if e != nil {
				return
			}
			select {
			case s.incoming <- conn:
			case <-s.done:
				conn.Close()
				return
			}
		}
	}()
	go func() {
		runErr := vm.Run(ctx)
		for _, closer := range closers {
			_ = closer.Close()
		}
		_ = vm.Close()
		for _, file := range files {
			_ = file.Close()
		}
		if dropped, failed := vm.SerialLoss(); dropped != 0 || failed != 0 {
			fmt.Fprintf(serial, "VMM serial output lost: dropped=%d failed=%d\n", dropped, failed)
		}
		s.mu.Lock()
		s.state = hostv1.VMState_VM_STATE_STOPPED
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			s.state = hostv1.VMState_VM_STATE_FAILED
			fmt.Fprintln(serial, runErr)
		}
		s.mu.Unlock()
		close(s.done)
	}()
	go func() { <-s.observed; listener.Close() }()
	var handlers sync.WaitGroup
	defer func() {
		s.cancel()
		s.mu.Lock()
		for conn, data := range s.streams {
			if data {
				conn.Close()
			}
		}
		s.mu.Unlock()
		handlers.Wait()
	}()
	return s.accept(listener, &handlers)
}

// accept ends only when the VM is done or the listener closes. A temporary
// failure such as descriptor exhaustion must not end a running VM.
func (s *child) accept(listener *net.UnixListener, handlers *sync.WaitGroup) error {
	delay := 5 * time.Millisecond
	for {
		conn, err := listener.AcceptUnix()
		if err == nil {
			delay = 5 * time.Millisecond
			handlers.Add(1)
			go func() { defer handlers.Done(); s.serve(conn) }()
			continue
		}
		select {
		case <-s.done:
			return nil
		default:
		}
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		select {
		case <-s.done:
			return nil
		case <-time.After(delay):
		}
		delay = min(2*delay, time.Second)
	}
}

func (s *child) serve(conn *net.UnixConn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReaderSize(conn, 16<<10)
	var req request
	if readMessage(reader, &req) != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	s.mu.Lock()
	if req.Operation == "claim" {
		if req.Config == nil || !sameConfig(*req.Config, s.config) {
			s.mu.Unlock()
			writeMessage(conn, requestError("running VMM configuration differs from requested instance"))
			return
		}
		for old := range s.streams {
			old.Close()
		}
		s.session = req.Session
	} else if req.Session == "" || req.Session != s.session {
		s.mu.Unlock()
		writeMessage(conn, requestError("VMM controller session replaced"))
		return
	}
	state := s.state
	if req.Operation == "stop" {
		s.cancel()
	}
	s.streams[conn] = req.Operation == "dial" || req.Operation == "accept"
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.streams, conn); s.mu.Unlock() }()
	switch req.Operation {
	case "claim", "status":
		writeMessage(conn, reply{State: state, Launch: s.launch, PID: os.Getpid()})
	case "wait":
		gone := make(chan struct{})
		go func() { var b [1]byte; conn.Read(b[:]); close(gone) }()
		select {
		case <-s.done:
		case <-gone:
			return
		}
		s.mu.Lock()
		state = s.state
		s.mu.Unlock()
		if writeMessage(conn, reply{State: state}) == nil {
			s.once.Do(func() { close(s.observed) })
		}
	case "stop":
		<-s.done
		s.mu.Lock()
		state = s.state
		s.mu.Unlock()
		if writeMessage(conn, reply{State: state}) == nil {
			s.once.Do(func() { close(s.observed) })
		}
	case "dial", "accept":
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Request streams have no payload until the response. A peer closing while
		// waiting cancels connection establishment without stopping the VM.
		ready := make(chan struct{})
		go func() {
			var b [1]byte
			_, err := conn.Read(b[:])
			if err != nil {
				cancel()
			}
			close(ready)
		}()
		var peer net.Conn
		var err error
		if req.Operation == "dial" {
			peer, err = s.socket.DialContext(ctx, req.Port)
		} else {
			select {
			case peer = <-s.incoming:
			case <-ctx.Done():
				err = ctx.Err()
			case <-s.done:
				err = net.ErrClosed
			}
		}
		// Wake the establishment-only reader before allowing stream payload.
		_ = conn.SetReadDeadline(time.Now())
		<-ready
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			writeMessage(conn, requestError("%v", err))
			return
		}
		if err = writeMessage(conn, reply{State: state}); err != nil {
			peer.Close()
			return
		}
		relay(&stream{conn, reader}, peer)
	default:
		writeMessage(conn, requestError("unknown VMM operation"))
	}
}

func sameConfig(a, b Config) bool { a.CommandLine = ""; b.CommandLine = ""; return a == b }
