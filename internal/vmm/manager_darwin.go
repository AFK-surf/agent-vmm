//go:build darwin

package vmm

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"github.com/Code-Hex/vz/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const VMMDPort = 4050

type Config struct {
	BundleDirectory   string
	InstanceDirectory string
	CPUCount          uint
	MemoryBytes       uint64
	StateBytes        int64
	SerialLog         string
	TrustRoot         ed25519.PublicKey
}

type ApplianceBlock struct {
	InstallationID, VolumeID, Identity, Path, DeviceIdentifier string
	Bytes                                                      int64
	Initializing                                               bool
}

type Manager struct {
	config Config

	mu              sync.Mutex
	state           hostv1.VMState
	bootID          string
	lastErr         string
	machine         *vz.VirtualMachine
	socket          *vz.VirtioSocketDevice
	egressListener  net.Listener
	verified        *bundle.Verified
	applianceBlocks []ApplianceBlock
	invalidate      func()
}

func (manager *Manager) SetInvalidationHandler(handler func()) {
	manager.mu.Lock()
	manager.invalidate = handler
	manager.mu.Unlock()
}

func NewManager(config Config) (*Manager, error) {
	if config.CPUCount == 0 || config.MemoryBytes < 512<<20 || config.StateBytes < 1<<30 {
		return nil, errors.New("invalid VM resources")
	}
	for _, path := range []string{config.BundleDirectory, config.InstanceDirectory, config.SerialLog} {
		if path == "" {
			return nil, errors.New("missing VM path")
		}
	}
	return &Manager{config: config, state: hostv1.VMState_VM_STATE_STOPPED}, nil
}

func (manager *Manager) Snapshot() *hostv1.VM {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.snapshotLocked()
}

func (manager *Manager) ConfigureApplianceBlocks(blocks []ApplianceBlock) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.state != hostv1.VMState_VM_STATE_STOPPED && manager.state != hostv1.VMState_VM_STATE_FAILED || manager.machine != nil {
		return errors.New("appliance block attachments can only change while the VM is stopped")
	}
	paths, identifiers, installations := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, block := range blocks {
		if block.InstallationID == "" || block.VolumeID == "" || block.Identity == "" || block.Path == "" || block.DeviceIdentifier == "" || block.Bytes < 1<<30 {
			return errors.New("appliance block attachment is incomplete")
		}
		if paths[block.Path] || identifiers[block.DeviceIdentifier] || installations[block.InstallationID] {
			return errors.New("appliance block attachment identity is not unique")
		}
		info, err := os.Stat(block.Path)
		if err != nil || !info.Mode().IsRegular() || info.Size() != block.Bytes {
			return errors.New("appliance block image has an unexpected size")
		}
		paths[block.Path], identifiers[block.DeviceIdentifier], installations[block.InstallationID] = true, true, true
	}
	manager.applianceBlocks = append([]ApplianceBlock(nil), blocks...)
	return nil
}

func (manager *Manager) HasApplianceBlock(installationID string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, block := range manager.applianceBlocks {
		if block.InstallationID == installationID {
			return true
		}
	}
	return false
}

func (manager *Manager) Start(ctx context.Context) (*hostv1.VM, error) {
	manager.mu.Lock()
	if manager.state == hostv1.VMState_VM_STATE_RUNNING {
		result := manager.snapshotLocked()
		manager.mu.Unlock()
		return result, nil
	}
	if manager.state != hostv1.VMState_VM_STATE_STOPPED && manager.state != hostv1.VMState_VM_STATE_FAILED {
		manager.mu.Unlock()
		return nil, errors.New("VM transition in progress")
	}
	manager.state = hostv1.VMState_VM_STATE_STARTING
	manager.lastErr = ""
	manager.mu.Unlock()

	if err := manager.start(ctx); err != nil {
		manager.fail(err)
		return nil, err
	}
	manager.mu.Lock()
	manager.state = hostv1.VMState_VM_STATE_RUNNING
	result := manager.snapshotLocked()
	manager.mu.Unlock()
	return result, nil
}

func (manager *Manager) start(ctx context.Context) error {
	verified, err := bundle.Verify(manager.config.BundleDirectory, manager.config.TrustRoot)
	if err != nil {
		return fmt.Errorf("verify bundle: %w", err)
	}
	if err := os.MkdirAll(manager.config.InstanceDirectory, 0o700); err != nil {
		return err
	}
	initializing, err := prepareState(manager.config.InstanceDirectory, manager.config.StateBytes)
	if err != nil {
		return err
	}
	serial, err := os.OpenFile(manager.config.SerialLog, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	configuration, err := manager.configuration(verified, serial, initializing)
	if err != nil {
		serial.Close()
		return err
	}
	valid, err := configuration.Validate()
	if !valid || err != nil {
		serial.Close()
		return fmt.Errorf("validate VM configuration: valid=%t: %w", valid, err)
	}
	machine, err := vz.NewVirtualMachine(configuration)
	if err != nil {
		serial.Close()
		return err
	}
	if err := machine.Start(); err != nil {
		serial.Close()
		return err
	}
	started := false
	defer func() {
		if started {
			return
		}
		if machine.CanStop() {
			_ = machine.Stop()
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = waitState(stopCtx, machine, vz.VirtualMachineStateStopped)
		cancel()
		_ = serial.Close()
		manager.mu.Lock()
		if manager.machine == machine {
			manager.closeEgressLocked()
			manager.machine = nil
			manager.socket = nil
			manager.bootID = ""
		}
		manager.mu.Unlock()
	}()
	if err := waitState(ctx, machine, vz.VirtualMachineStateRunning); err != nil {
		return err
	}
	manager.mu.Lock()
	manager.closeEgressLocked()
	manager.machine = machine
	manager.socket = machine.SocketDevices()[0]
	manager.verified = verified
	manager.bootID = ""
	manager.mu.Unlock()

	health, err := manager.waitHealth(ctx)
	if err != nil {
		return err
	}
	if !health.GetReady() || !health.GetRootReadOnly() || health.GetBootId() == "" || health.GetManifestDigest() != verified.ManifestDigest {
		return errors.New("guest health does not match verified bundle")
	}
	manager.mu.Lock()
	manager.bootID = health.GetBootId()
	manager.mu.Unlock()
	if initializing {
		if !health.GetStateInitialized() {
			return errors.New("guest did not initialize state disk")
		}
		if err := finishStateInitialization(manager.config.InstanceDirectory); err != nil {
			return err
		}
	}
	started = true
	go manager.observe(machine, serial)
	return nil
}

func (manager *Manager) Stop(ctx context.Context, force bool) (*hostv1.VM, error) {
	manager.mu.Lock()
	if manager.state == hostv1.VMState_VM_STATE_STOPPED {
		result := manager.snapshotLocked()
		manager.mu.Unlock()
		return result, nil
	}
	if manager.state != hostv1.VMState_VM_STATE_RUNNING && !(force && manager.state == hostv1.VMState_VM_STATE_FAILED && manager.machine != nil) {
		manager.mu.Unlock()
		return nil, errors.New("VM is not running")
	}
	manager.state = hostv1.VMState_VM_STATE_STOPPING
	machine := manager.machine
	manager.mu.Unlock()

	if !force {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_ = manager.prepareShutdown(shutdownCtx)
		if machine.CanRequestStop() {
			_, _ = machine.RequestStop()
		}
		if err := waitState(shutdownCtx, machine, vz.VirtualMachineStateStopped); err == nil {
			cancel()
			manager.mu.Lock()
			manager.state = hostv1.VMState_VM_STATE_STOPPED
			manager.machine = nil
			manager.socket = nil
			manager.closeEgressLocked()
			result := manager.snapshotLocked()
			manager.mu.Unlock()
			return result, nil
		}
		cancel()
		manager.mu.Lock()
		if machine.State() == vz.VirtualMachineStateStopped {
			manager.state = hostv1.VMState_VM_STATE_STOPPED
			manager.machine = nil
			manager.socket = nil
			manager.closeEgressLocked()
			result := manager.snapshotLocked()
			manager.mu.Unlock()
			return result, nil
		}
		if machine.State() == vz.VirtualMachineStateRunning {
			manager.state = hostv1.VMState_VM_STATE_RUNNING
		} else {
			manager.state = hostv1.VMState_VM_STATE_FAILED
			manager.lastErr = "graceful VM stop did not complete"
		}
		manager.mu.Unlock()
		return nil, errors.New("graceful VM stop failed; force stop requires explicit confirmation")
	}
	if machine.CanStop() {
		if err := machine.Stop(); err != nil {
			manager.fail(err)
			return nil, err
		}
	}
	if err := waitState(ctx, machine, vz.VirtualMachineStateStopped); err != nil {
		manager.fail(err)
		return nil, err
	}
	manager.mu.Lock()
	manager.state = hostv1.VMState_VM_STATE_STOPPED
	manager.machine = nil
	manager.socket = nil
	manager.closeEgressLocked()
	result := manager.snapshotLocked()
	manager.mu.Unlock()
	return result, nil
}

func (manager *Manager) ListenEgress() (net.Listener, string, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.state != hostv1.VMState_VM_STATE_RUNNING || manager.socket == nil || manager.bootID == "" {
		return nil, "", errors.New("VM is not running")
	}
	if manager.egressListener != nil {
		return nil, "", errors.New("egress listener already active")
	}
	listener, err := manager.socket.Listen(netprotocol.EgressPort)
	if err != nil {
		return nil, "", err
	}
	manager.egressListener = listener
	return listener, manager.bootID, nil
}

func (manager *Manager) CloseEgress() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.closeEgressLocked()
}

func (manager *Manager) closeEgressLocked() {
	if manager.egressListener != nil {
		_ = manager.egressListener.Close()
		manager.egressListener = nil
	}
}

func (manager *Manager) connectVMMD(ctx context.Context, allowStopping bool) (net.Conn, error) {
	manager.mu.Lock()
	socket := manager.socket
	running := manager.state == hostv1.VMState_VM_STATE_RUNNING || manager.state == hostv1.VMState_VM_STATE_STARTING || allowStopping && manager.state == hostv1.VMState_VM_STATE_STOPPING
	manager.mu.Unlock()
	if !running || socket == nil {
		return nil, errors.New("VM is not available")
	}
	type result struct {
		connection net.Conn
		err        error
	}
	ready := make(chan result, 1)
	go func() {
		connection, err := socket.Connect(VMMDPort)
		ready <- result{connection: connection, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ready:
		return result.connection, result.err
	}
}

func (manager *Manager) DialVMMD(ctx context.Context) (*grpc.ClientConn, error) {
	return manager.dialVMMD(ctx, false)
}

func waitState(ctx context.Context, machine *vz.VirtualMachine, wanted vz.VirtualMachineState) error {
	if machine.State() == wanted {
		return nil
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if machine.State() == wanted {
				return nil
			}
		case state := <-machine.StateChangedNotify():
			if state == wanted {
				return nil
			}
			if state == vz.VirtualMachineStateError {
				return errors.New("VM entered error state")
			}
		}
	}
}

func (manager *Manager) waitHealth(ctx context.Context) (*vmmdv1.HealthResponse, error) {
	for {
		connection, err := manager.dialVMMD(ctx, false)
		if err == nil {
			client := vmmdv1.NewVMMDServiceClient(connection)
			health, healthErr := client.Health(ctx, &vmmdv1.HealthRequest{})
			_ = connection.Close()
			if healthErr == nil && health.GetReady() {
				return health, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (manager *Manager) prepareShutdown(ctx context.Context) error {
	connection, err := manager.dialVMMD(ctx, true)
	if err != nil {
		return err
	}
	defer connection.Close()
	_, err = vmmdv1.NewVMMDServiceClient(connection).PrepareShutdown(ctx, &vmmdv1.PrepareShutdownRequest{RequestId: manager.bootID})
	return err
}

func (manager *Manager) dialVMMD(ctx context.Context, allowStopping bool) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///vmmd", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return manager.connectVMMD(ctx, allowStopping)
	}))
}

func (manager *Manager) observe(machine *vz.VirtualMachine, serial *os.File) {
	defer serial.Close()
	for state := range machine.StateChangedNotify() {
		if state != vz.VirtualMachineStateStopped && state != vz.VirtualMachineStateError {
			continue
		}
		manager.mu.Lock()
		if manager.machine == machine {
			manager.closeEgressLocked()
			manager.machine = nil
			manager.socket = nil
			if state == vz.VirtualMachineStateError {
				manager.state = hostv1.VMState_VM_STATE_FAILED
				manager.lastErr = "Virtualization.framework reported an error"
			} else {
				manager.state = hostv1.VMState_VM_STATE_STOPPED
			}
		}
		invalidate := manager.invalidate
		manager.mu.Unlock()
		if invalidate != nil {
			invalidate()
		}
		return
	}
}

func (manager *Manager) fail(err error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.state = hostv1.VMState_VM_STATE_FAILED
	manager.lastErr = err.Error()
	manager.closeEgressLocked()
}

func (manager *Manager) snapshotLocked() *hostv1.VM {
	return &hostv1.VM{State: manager.state, BootId: manager.bootID, Error: manager.lastErr}
}
