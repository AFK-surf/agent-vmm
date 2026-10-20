//go:build darwin

package vmm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"github.com/Code-Hex/vz/v3"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	VMMDPort          = 4050
	hostMemoryReserve = 4 << 30
	maxSerialLogBytes = 1 << 20
)

type startObservationKey struct{}

type startObservation struct {
	OperationID string `json:"operation_id,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
}

type startProgress struct {
	OperationID string `json:"operation_id,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	Stage       string `json:"stage"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
}

func WithStartObservation(ctx context.Context, operationID, requestID string) context.Context {
	return context.WithValue(ctx, startObservationKey{}, startObservation{OperationID: operationID, RequestID: requestID})
}

func startObservationFromContext(ctx context.Context) startObservation {
	value, _ := ctx.Value(startObservationKey{}).(startObservation)
	return value
}

func reportStartProgress(ctx context.Context, stage, state string, err error) {
	observation := startObservationFromContext(ctx)
	record := startProgress{OperationID: observation.OperationID, RequestID: observation.RequestID, Stage: stage, State: state}
	if err != nil {
		record.Error = err.Error()
		if len(record.Error) > 4096 {
			record.Error = record.Error[:4096]
		}
	}
	if data, marshalErr := json.Marshal(record); marshalErr == nil {
		log.Printf("vmm_start=%s", data)
	}
}

var physicalMemoryBytes = func() (uint64, error) { return unix.SysctlUint64("hw.memsize") }
var processMemoryBytes = currentProcessMemoryBytes

type Config struct {
	BundleDirectory     string
	InstanceDirectory   string
	LegacyStatePath     string
	CPUCount            uint
	MemoryBytes         uint64
	StateBytes          int64
	SerialLog           string
	GuestManifestDigest string
}

type Manager struct {
	config              Config
	hostDirectoryShares map[string]*vz.SharedDirectory

	mu             sync.Mutex
	state          hostv1.VMState
	bootID         string
	lastErr        string
	machine        *vz.VirtualMachine
	socket         *vz.VirtioSocketDevice
	egressListener net.Listener
	verified       *bundle.Verified
	invalidate     func()
	admissionFence func()
	recover        func(string)
	recoveryUsed   bool
}

func (manager *Manager) SetInvalidationHandler(handler func()) {
	manager.mu.Lock()
	manager.invalidate = handler
	manager.mu.Unlock()
}

// SetAdmissionFence installs a non-blocking callback that must not call back
// into Manager. terminalTransition invokes it while holding manager.mu.
func (manager *Manager) SetAdmissionFence(handler func()) {
	manager.mu.Lock()
	manager.admissionFence = handler
	manager.mu.Unlock()
}

func (manager *Manager) SetUnexpectedStopHandler(handler func(previousBootID string)) {
	manager.mu.Lock()
	manager.recover = handler
	manager.mu.Unlock()
}

func (manager *Manager) ClearLifecycleHandlers() {
	manager.mu.Lock()
	manager.invalidate = nil
	manager.admissionFence = nil
	manager.recover = nil
	manager.mu.Unlock()
}

func NewManager(config Config) (*Manager, error) {
	if config.CPUCount == 0 || config.MemoryBytes < 512<<20 || config.StateBytes < 1<<30 {
		return nil, errors.New("invalid VM resources")
	}
	physical, err := physicalMemoryBytes()
	if err != nil {
		return nil, fmt.Errorf("read host physical memory: %w", err)
	}
	if physical <= hostMemoryReserve || config.MemoryBytes > physical-hostMemoryReserve {
		return nil, fmt.Errorf("VM memory %d leaves less than 4 GiB for the host", config.MemoryBytes)
	}
	for _, path := range []string{config.BundleDirectory, config.InstanceDirectory, config.SerialLog} {
		if path == "" {
			return nil, errors.New("missing VM path")
		}
	}
	if config.LegacyStatePath != "" {
		info, err := os.Lstat(config.LegacyStatePath)
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("legacy state disk must be an existing regular file")
		}
		if info.Size() != config.StateBytes {
			return nil, errors.New("legacy and target state disk sizes must match")
		}
	}
	return &Manager{config: config, state: hostv1.VMState_VM_STATE_STOPPED}, nil
}

func (manager *Manager) Snapshot() *hostv1.VM {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.snapshotLocked()
}

// ResourceSnapshot reports facts owned by the Host process and its state disk.
// Missing measurements stay absent and carry a bounded reason.
func (manager *Manager) ResourceSnapshot() *hostv1.LayerResources {
	pids := uint32(1)
	result := &hostv1.LayerResources{Pids: &pids, SampledAt: timestamppb.Now()}
	var unavailable []string
	if memory, err := processMemoryBytes(); err == nil {
		result.MemoryBytes = &memory
	} else {
		unavailable = append(unavailable, err.Error())
	}
	if entries, err := os.ReadDir("/dev/fd"); err == nil && len(entries) <= int(^uint32(0)) {
		count := uint32(len(entries))
		result.OpenFds = &count
	} else {
		unavailable = append(unavailable, "host file descriptors")
	}
	statePath := filepath.Join(manager.config.InstanceDirectory, "state.raw")
	if info, err := os.Stat(statePath); err == nil && info.Mode().IsRegular() {
		used, allocatedErr := allocatedBytes(info)
		if allocatedErr == nil {
			committed := uint64(info.Size())
			reserve := uint64(hostEmergencyBytes)
			result.Disk = &hostv1.DiskAccounting{UsedBytes: &used, CommittedBytes: &committed, SystemReserveBytes: &reserve}
			var filesystem unix.Statfs_t
			if statErr := unix.Statfs(filepath.Dir(statePath), &filesystem); statErr == nil && filesystem.Bsize > 0 {
				capacity := filesystem.Blocks * uint64(filesystem.Bsize)
				result.Disk.CapacityBytes = &capacity
			} else {
				unavailable = append(unavailable, "host filesystem capacity")
			}
		} else {
			unavailable = append(unavailable, "host state disk allocation")
		}
	} else {
		unavailable = append(unavailable, "host state disk")
	}
	result.UnavailableReason = strings.Join(unavailable, ", ")
	return result
}

// HasMachineAttachment reports whether Virtualization.framework may still own
// the state disk. FAILED is not terminal for Host shutdown while this is true.
func (manager *Manager) HasMachineAttachment() bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.machine != nil
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

func (manager *Manager) start(ctx context.Context) (resultErr error) {
	stage := "verify_bundle"
	defer func() {
		if resultErr != nil {
			reportStartProgress(ctx, stage, "failed", resultErr)
		}
	}()
	reportStartProgress(ctx, stage, "started", nil)
	verified, err := bundle.Verify(manager.config.BundleDirectory, manager.config.GuestManifestDigest,
		"kernel", "initramfs", "rootfs")
	if err != nil {
		return fmt.Errorf("verify bundle: %w", err)
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	if err := os.MkdirAll(manager.config.InstanceDirectory, 0o700); err != nil {
		return err
	}
	stage = "prepare_state"
	reportStartProgress(ctx, stage, "started", nil)
	initializing, err := prepareState(manager.config.InstanceDirectory, manager.config.StateBytes)
	if err != nil {
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	if err := rotateSerialLog(manager.config.SerialLog); err != nil {
		return err
	}
	serial, err := os.OpenFile(manager.config.SerialLog, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	stage = "build_configuration"
	reportStartProgress(ctx, stage, "started", nil)
	configuration, err := manager.configuration(verified, serial, initializing, startObservationFromContext(ctx))
	if err != nil {
		serial.Close()
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	stage = "validate_configuration"
	reportStartProgress(ctx, stage, "started", nil)
	valid, err := configuration.Validate()
	if !valid || err != nil {
		serial.Close()
		return fmt.Errorf("validate VM configuration: valid=%t: %w", valid, err)
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	stage = "create_machine"
	reportStartProgress(ctx, stage, "started", nil)
	machine, err := vz.NewVirtualMachine(configuration)
	if err != nil {
		serial.Close()
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	stage = "start_machine"
	reportStartProgress(ctx, stage, "started", nil)
	if err := machine.Start(); err != nil {
		serial.Close()
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
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
	stage = "wait_running"
	reportStartProgress(ctx, stage, "started", nil)
	if err := waitState(ctx, machine, vz.VirtualMachineStateRunning); err != nil {
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	manager.mu.Lock()
	manager.closeEgressLocked()
	manager.machine = machine
	manager.socket = machine.SocketDevices()[0]
	manager.verified = verified
	manager.bootID = ""
	manager.mu.Unlock()

	stage = "wait_guest_health"
	reportStartProgress(ctx, stage, "started", nil)
	health, err := manager.waitHealth(ctx)
	if err != nil {
		return err
	}
	if !health.GetReady() || !health.GetRootReadOnly() || health.GetBootId() == "" {
		return errors.New("guest health does not match verified bundle")
	}
	manager.mu.Lock()
	manager.bootID = health.GetBootId()
	manager.mu.Unlock()
	reportStartProgress(ctx, stage, "succeeded", nil)
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

func rotateSerialLog(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return err
	}
	if info.Size() <= maxSerialLogBytes {
		return nil
	}
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()
	if _, err := input.Seek(-maxSerialLogBytes, 2); err != nil {
		return err
	}
	previous := path + ".previous"
	temporary, err := os.CreateTemp(filepath.Dir(path), ".serial-*.log")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(input, maxSerialLogBytes))
	if readErr != nil {
		temporary.Close()
		return readErr
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, previous); err != nil {
		return err
	}
	return os.Truncate(path, 0)
}

func (manager *Manager) Stop(ctx context.Context, force bool) (*hostv1.VM, error) {
	manager.mu.Lock()
	if manager.state == hostv1.VMState_VM_STATE_FAILED && manager.machine == nil {
		manager.state = hostv1.VMState_VM_STATE_STOPPED
		manager.socket = nil
		manager.bootID = ""
		manager.closeEgressLocked()
	}
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
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		prepareErr := manager.prepareShutdown(shutdownCtx)
		if prepareErr != nil {
			cancel()
			manager.mu.Lock()
			manager.state = hostv1.VMState_VM_STATE_FAILED
			manager.lastErr = fmt.Sprintf("guest shutdown preparation failed: %v", prepareErr)
			manager.mu.Unlock()
			return nil, prepareErr
		}
		var requestStopErr error
		if machine.CanRequestStop() {
			_, requestStopErr = machine.RequestStop()
		}
		if err := waitState(shutdownCtx, machine, vz.VirtualMachineStateStopped); err == nil {
			cancel()
			manager.mu.Lock()
			manager.state = hostv1.VMState_VM_STATE_STOPPED
			manager.machine = nil
			manager.socket = nil
			manager.bootID = ""
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
			manager.bootID = ""
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
		return nil, errors.Join(
			errors.New("graceful VM stop failed; force stop requires explicit confirmation"),
			prepareErr,
			requestStopErr,
		)
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
	manager.bootID = ""
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
	return awaitVMMDConnection(ctx, func() (net.Conn, error) {
		return socket.Connect(VMMDPort)
	})
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
	startedAt := time.Now()
	lastProgress := time.Time{}
	var lastErr error
	for {
		connection, err := manager.dialVMMD(ctx, false)
		lastErr = err
		if err == nil {
			client := vmmdv1.NewVMMDServiceClient(connection)
			health, healthErr := client.Health(ctx, &vmmdv1.HealthRequest{})
			_ = connection.Close()
			if healthErr == nil && health.GetReady() {
				return health, nil
			}
			lastErr = healthErr
		}
		if lastProgress.IsZero() || time.Since(lastProgress) >= 15*time.Second {
			log.Printf("vmm guest health waiting: elapsed=%s error=%v", time.Since(startedAt).Round(time.Millisecond), lastErr)
			lastProgress = time.Now()
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
	manager.observeStateChanges(machine, machine.StateChangedNotify())
}

func (manager *Manager) observeStateChanges(machine *vz.VirtualMachine, states <-chan vz.VirtualMachineState) {
	for state := range states {
		if state != vz.VirtualMachineStateStopped && state != vz.VirtualMachineStateError {
			continue
		}
		invalidate, recover, previousBootID := manager.terminalTransition(machine, state, true)
		if invalidate != nil {
			invalidate()
		}
		if recover != nil {
			recover(previousBootID)
		}
		return
	}
	invalidate, _, _ := manager.terminalTransition(machine, vz.VirtualMachineStateError, false)
	if invalidate != nil {
		invalidate()
	}
}

func (manager *Manager) terminalTransition(machine *vz.VirtualMachine, state vz.VirtualMachineState, recoverable bool) (func(), func(string), string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.machine != machine {
		return nil, nil, ""
	}
	if manager.admissionFence != nil {
		manager.admissionFence()
	}
	unexpected := false
	previousBootID := ""
	unexpected = manager.state == hostv1.VMState_VM_STATE_RUNNING
	previousBootID = manager.bootID
	manager.closeEgressLocked()
	manager.machine = nil
	manager.socket = nil
	manager.bootID = ""
	if unexpected && !recoverable {
		manager.recoveryUsed = true
		manager.state = hostv1.VMState_VM_STATE_FAILED
		manager.lastErr = "VM state notification channel closed; operator action required"
	} else if unexpected && manager.recoveryUsed {
		manager.state = hostv1.VMState_VM_STATE_FAILED
		manager.lastErr = "automatic VM recovery exhausted; operator action required"
	} else if state == vz.VirtualMachineStateError {
		manager.state = hostv1.VMState_VM_STATE_FAILED
		manager.lastErr = "Virtualization.framework reported an error"
	} else {
		manager.state = hostv1.VMState_VM_STATE_STOPPED
	}
	invalidate := manager.invalidate
	recover := manager.recover
	if unexpected && !manager.recoveryUsed {
		manager.recoveryUsed = true
	} else {
		recover = nil
	}
	return invalidate, recover, previousBootID
}

func (manager *Manager) FailRecovery(err error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.state = hostv1.VMState_VM_STATE_FAILED
	manager.lastErr = "automatic VM recovery failed; operator action required: " + err.Error()
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
