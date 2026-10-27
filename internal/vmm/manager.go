//go:build darwin || linux

package vmm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
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
		_, _ = fmt.Fprintf(os.Stderr, "vmm_start=%s\n", data)
	}
}

var physicalMemoryBytes = HostPhysicalMemoryBytes
var processMemoryBytes = currentProcessMemoryBytes

type Config struct {
	Logger              *slog.Logger
	BundleDirectory     string
	InstanceDirectory   string
	CPUCount            uint
	MemoryBytes         uint64
	StateBytes          int64
	SerialLog           string
	GuestManifestDigest string
}

type Manager struct {
	logger *slog.Logger
	config Config
	platformConfig

	mu             sync.Mutex
	state          hostv1.VMState
	bootID         string
	lastErr        string
	machine        machineBackend
	egressListener net.Listener
	verified       *bundle.Verified
	invalidate     func()
	admissionFence func()
	recover        func(string)
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
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{logger: logger.With("component", "vmm"), config: config, state: hostv1.VMState_VM_STATE_STOPPED}, nil
}

func (manager *Manager) Snapshot() *hostv1.VM {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.snapshotLocked()
}

// HasMachineAttachment reports whether a VM runtime may still own
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
	initializing, attached, err := manager.preparePlatformState()
	if err != nil {
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	if !attached {
		if err := rotateSerialLog(manager.config.SerialLog); err != nil {
			return err
		}
	}
	serial, err := os.OpenFile(manager.config.SerialLog, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	stage = "build_configuration"
	reportStartProgress(ctx, stage, "started", nil)
	machine, err := manager.startMachine(ctx, verified, serial, initializing, startObservationFromContext(ctx))
	if err != nil {
		serial.Close()
		return err
	}
	started := false
	defer func() {
		if started {
			return
		}
		retained := manager.abortMachineStart(machine)
		_ = serial.Close()
		manager.mu.Lock()
		manager.closeEgressLocked()
		manager.bootID = ""
		if retained {
			// An adopted VMM keeps running after a failed start; a forced Stop must
			// still reach it, and its end must release the attachment.
			manager.machine = machine
			go manager.observeStateChanges(machine, machine.StateChangedNotify())
		} else if manager.machine == machine {
			manager.machine = nil
		}
		manager.mu.Unlock()
	}()
	stage = "wait_running"
	reportStartProgress(ctx, stage, "started", nil)
	if err := waitState(ctx, machine, hostv1.VMState_VM_STATE_RUNNING); err != nil {
		return err
	}
	reportStartProgress(ctx, stage, "succeeded", nil)
	manager.mu.Lock()
	manager.closeEgressLocked()
	manager.machine = machine
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

func (manager *Manager) Stop(ctx context.Context, force bool) (*hostv1.VM, error) {
	manager.mu.Lock()
	if manager.state == hostv1.VMState_VM_STATE_FAILED && manager.machine == nil {
		manager.state = hostv1.VMState_VM_STATE_STOPPED
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
		if err := waitState(shutdownCtx, machine, hostv1.VMState_VM_STATE_STOPPED); err == nil {
			cancel()
			manager.mu.Lock()
			manager.state = hostv1.VMState_VM_STATE_STOPPED
			manager.machine = nil
			manager.bootID = ""
			manager.closeEgressLocked()
			result := manager.snapshotLocked()
			manager.mu.Unlock()
			return result, nil
		}
		cancel()
		manager.mu.Lock()
		if machine.State() == hostv1.VMState_VM_STATE_STOPPED {
			manager.state = hostv1.VMState_VM_STATE_STOPPED
			manager.machine = nil
			manager.bootID = ""
			manager.closeEgressLocked()
			result := manager.snapshotLocked()
			manager.mu.Unlock()
			return result, nil
		}
		if machine.State() == hostv1.VMState_VM_STATE_RUNNING {
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
	if err := waitState(ctx, machine, hostv1.VMState_VM_STATE_STOPPED); err != nil {
		manager.fail(err)
		return nil, err
	}
	manager.mu.Lock()
	manager.state = hostv1.VMState_VM_STATE_STOPPED
	manager.machine = nil
	manager.bootID = ""
	manager.closeEgressLocked()
	result := manager.snapshotLocked()
	manager.mu.Unlock()
	return result, nil
}

func (manager *Manager) observe(machine machineBackend, serial *os.File) {
	defer serial.Close()
	manager.observeStateChanges(machine, machine.StateChangedNotify())
}

func (manager *Manager) observeStateChanges(machine machineBackend, states <-chan hostv1.VMState) {
	for state := range states {
		if state != hostv1.VMState_VM_STATE_STOPPED && state != hostv1.VMState_VM_STATE_FAILED {
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
	invalidate, _, _ := manager.terminalTransition(machine, hostv1.VMState_VM_STATE_FAILED, false)
	if invalidate != nil {
		invalidate()
	}
}

func (manager *Manager) terminalTransition(machine machineBackend, state hostv1.VMState, recoverable bool) (func(), func(string), string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.machine != machine {
		return nil, nil, ""
	}
	if manager.state == hostv1.VMState_VM_STATE_STARTING {
		// A start adopts a retained VMM through a new session, which ends the
		// retained one; that end does not decide the new attempt.
		manager.machine = nil
		return nil, nil, ""
	}
	if manager.admissionFence != nil {
		manager.admissionFence()
	}
	unexpected := manager.state == hostv1.VMState_VM_STATE_RUNNING
	previousBootID := manager.bootID
	manager.closeEgressLocked()
	manager.machine = nil
	manager.bootID = ""
	switch {
	case manager.state == hostv1.VMState_VM_STATE_FAILED:
		// A retained machine that ends keeps the failure that retained it.
	case unexpected && !recoverable:
		manager.state = hostv1.VMState_VM_STATE_FAILED
		manager.lastErr = "VM state notification channel closed; operator action required"
	case state == hostv1.VMState_VM_STATE_FAILED:
		manager.state = hostv1.VMState_VM_STATE_FAILED
		manager.lastErr = "virtual machine reported an error"
	default:
		manager.state = hostv1.VMState_VM_STATE_STOPPED
	}
	// The recovery owner bounds its restart attempts for each unexpected stop.
	recover := manager.recover
	if !unexpected || !recoverable {
		recover = nil
	}
	return manager.invalidate, recover, previousBootID
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
