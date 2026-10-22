//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type vmLifecycleOperation uint8

const (
	vmLifecycleStart vmLifecycleOperation = iota + 1
	vmLifecycleStop
	vmLifecycleRecover
	vmLifecycleAppliance
	vmLifecycleShutdown
)

type vmLifecycleResult struct {
	vm  *hostv1.VM
	err error
}

type VMLifecycleOwner struct {
	controller *Controller
	manager    *vmm.Manager

	operationMu         sync.Mutex
	stateMu             sync.Mutex
	closing             bool
	recovery            context.CancelFunc
	recoveryWG          sync.WaitGroup
	shutdownDone        chan struct{}
	shutdownErr         error
	admission           atomic.Uint64
	snapshot            func() *hostv1.VM
	beforeAdmissionOpen func()
	shutdownCleanup     func(context.Context) error
	// commitErr is the last commit failure of commitBootID that left the VM
	// closed to admission.
	commitErr    error
	commitBootID string
	// recoveryBootID is the boot that the recovery goroutine replaces. It is
	// empty when that goroutine only retries the commit of a running VM.
	recoveryBootID string
	// recoveryPending records a recovery request that arrived after the
	// recovery goroutine last read recoveryBootID.
	recoveryPending bool
	// recoverAttempt, recoveryAttempts, and recoveredAt belong to the single
	// recovery goroutine.
	recoverAttempt   func(context.Context, string) error
	recoveryAttempts int
	recoveredAt      time.Time
}

func newVMLifecycleOwner(controller *Controller, manager *vmm.Manager) *VMLifecycleOwner {
	return &VMLifecycleOwner{controller: controller, manager: manager, shutdownDone: make(chan struct{}), snapshot: manager.Snapshot}
}

func (owner *VMLifecycleOwner) execute(ctx context.Context, operation vmLifecycleOperation, run func() vmLifecycleResult) vmLifecycleResult {
	startedAt := time.Now()
	log.Printf("vm lifecycle operation waiting: operation=%s", operation)
	release, err := owner.acquire(ctx, operation)
	if err != nil {
		log.Printf("vm lifecycle operation lock failed: operation=%s elapsed=%s error=%v", operation, time.Since(startedAt).Round(time.Millisecond), err)
		return vmLifecycleResult{err: err}
	}
	defer release()
	log.Printf("vm lifecycle operation acquired: operation=%s elapsed=%s", operation, time.Since(startedAt).Round(time.Millisecond))
	result := run()
	log.Printf("vm lifecycle operation completed: operation=%s elapsed=%s error=%v", operation, time.Since(startedAt).Round(time.Millisecond), result.err)
	return result
}

// acquire gives an operator mutation exclusive ownership before it writes a
// durable receipt. Once acquired, cancellation cannot strand accepted work
// behind another lifecycle owner.
func (owner *VMLifecycleOwner) acquire(ctx context.Context, operation vmLifecycleOperation) (func(), error) {
	if err := owner.lockOperation(ctx); err != nil {
		return nil, err
	}
	owner.stateMu.Lock()
	closing := owner.closing
	owner.stateMu.Unlock()
	if closing && operation != vmLifecycleShutdown {
		owner.operationMu.Unlock()
		return nil, errors.New("VM lifecycle is shutting down")
	}
	if owner.controller != nil {
		releaseInventory, err := owner.controller.lockInventoryTransition(ctx)
		if err != nil {
			owner.operationMu.Unlock()
			return nil, err
		}
		return func() {
			releaseInventory()
			owner.operationMu.Unlock()
		}, nil
	}
	return owner.operationMu.Unlock, nil
}

func (operation vmLifecycleOperation) String() string {
	switch operation {
	case vmLifecycleStart:
		return "start"
	case vmLifecycleStop:
		return "stop"
	case vmLifecycleRecover:
		return "recover"
	case vmLifecycleAppliance:
		return "appliance"
	case vmLifecycleShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

func (owner *VMLifecycleOwner) lockOperation(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if owner.operationMu.TryLock() {
			if err := ctx.Err(); err != nil {
				owner.operationMu.Unlock()
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (owner *VMLifecycleOwner) admitsRemoteWork() bool {
	if owner == nil {
		return false
	}
	_, open := owner.admissionToken()
	return open
}

func (owner *VMLifecycleOwner) admissionToken() (uint64, bool) {
	state := owner.admission.Load()
	return state >> 1, state&1 == 1
}

func (owner *VMLifecycleOwner) closeAdmission() {
	for {
		state := owner.admission.Load()
		if owner.admission.CompareAndSwap(state, ((state>>1)+1)<<1) {
			return
		}
	}
}

func (owner *VMLifecycleOwner) openAdmission(generation uint64, bootID string) bool {
	owner.stateMu.Lock()
	defer owner.stateMu.Unlock()
	if owner.closing {
		return false
	}
	vm := owner.snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING || bootID == "" || vm.GetBootId() != bootID {
		return false
	}
	if owner.beforeAdmissionOpen != nil {
		owner.beforeAdmissionOpen()
	}
	if !owner.admission.CompareAndSwap(generation<<1, generation<<1|1) {
		return false
	}
	owner.commitErr, owner.commitBootID = nil, ""
	return true
}

// admissionHealth reports whether a running VM admits remote work. Closed
// admission is a failure only after a commit of the current boot failed;
// otherwise the commit of that boot is still in progress.
func (owner *VMLifecycleOwner) admissionHealth(observedAt *timestamppb.Timestamp) *hostv1.ComponentHealth {
	health := &hostv1.ComponentHealth{Component: "host-admission", State: hostv1.HealthState_HEALTH_STATE_HEALTHY, ObservedAt: observedAt}
	if owner.admitsRemoteWork() {
		return health
	}
	bootID := owner.snapshot().GetBootId()
	owner.stateMu.Lock()
	defer owner.stateMu.Unlock()
	if owner.commitErr != nil && owner.commitBootID == bootID {
		health.State = hostv1.HealthState_HEALTH_STATE_UNAVAILABLE
		health.Reason = "VM admission is closed: last commit failed: " + owner.commitErr.Error()
		return health
	}
	health.State = hostv1.HealthState_HEALTH_STATE_DEGRADED
	health.Reason = "committing"
	return health
}

func (owner *VMLifecycleOwner) commit(ctx context.Context, excludedInstallationID string) error {
	vm := owner.manager.Snapshot()
	generation, _ := owner.admissionToken()
	return owner.controller.commitRunningVM(ctx, excludedInstallationID, generation, vm.GetBootId())
}

func (owner *VMLifecycleOwner) Start(ctx context.Context) (*hostv1.VM, error) {
	return owner.start(ctx, nil)
}

func (owner *VMLifecycleOwner) StartFenced(ctx context.Context, validate func() error) (*hostv1.VM, error) {
	return owner.start(ctx, validate)
}

func (owner *VMLifecycleOwner) start(ctx context.Context, validate func() error) (*hostv1.VM, error) {
	result := owner.execute(ctx, vmLifecycleStart, func() vmLifecycleResult {
		if validate != nil {
			if err := validate(); err != nil {
				return vmLifecycleResult{err: err}
			}
		}
		err := owner.startOwned(ctx)
		return vmLifecycleResult{vm: owner.manager.Snapshot(), err: err}
	})
	return result.vm, result.err
}

func (owner *VMLifecycleOwner) startVM(ctx context.Context) (*hostv1.VM, error) {
	state := owner.manager.Snapshot().GetState()
	if (state == hostv1.VMState_VM_STATE_STOPPED || state == hostv1.VMState_VM_STATE_FAILED) && !owner.manager.HasMachineAttachment() {
		if err := owner.controller.prepareHostShares(ctx); err != nil {
			return nil, err
		}
	}
	return owner.manager.Start(ctx)
}

func (owner *VMLifecycleOwner) startOwned(ctx context.Context) error {
	if owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING && owner.admitsRemoteWork() {
		return nil
	}
	log.Printf("vm lifecycle start stage: stage=prepare_transition")
	owner.controller.prepareVMTransition(ctx)
	owner.controller.stopEgress()
	if owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		err := owner.commit(ctx, "")
		if err != nil {
			owner.abandonCommit(err)
		}
		return err
	}
	log.Printf("vm lifecycle start stage: stage=machine_start")
	_, err := owner.startVM(ctx)
	if err == nil {
		log.Printf("vm lifecycle start stage: stage=commit")
		err = owner.commit(ctx, "")
	}
	if err != nil {
		owner.abandonCommit(err)
	}
	return err
}

func (owner *VMLifecycleOwner) Stop(ctx context.Context, force bool) (*hostv1.VM, error) {
	return owner.stop(ctx, force, nil)
}

func (owner *VMLifecycleOwner) StopFenced(ctx context.Context, force bool, validate func() error) (*hostv1.VM, error) {
	return owner.stop(ctx, force, validate)
}

func (owner *VMLifecycleOwner) stop(ctx context.Context, force bool, validate func() error) (*hostv1.VM, error) {
	result := owner.execute(ctx, vmLifecycleStop, func() vmLifecycleResult {
		if validate != nil {
			if err := validate(); err != nil {
				return vmLifecycleResult{err: err}
			}
		}
		err := owner.stopOwned(ctx, force)
		return vmLifecycleResult{vm: owner.manager.Snapshot(), err: err}
	})
	return result.vm, result.err
}

func (owner *VMLifecycleOwner) stopOwned(ctx context.Context, force bool) error {
	owner.controller.prepareVMTransition(ctx)
	owner.controller.stopEgress()
	_, err := owner.manager.Stop(ctx, force)
	if err != nil && owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if commitErr := owner.commit(context.Background(), ""); commitErr != nil {
			owner.abandonCommit(commitErr)
			err = errors.Join(err, commitErr)
		}
	}
	return err
}

func (owner *VMLifecycleOwner) ActivateAppliance(ctx context.Context, installationID string) error {
	return owner.execute(ctx, vmLifecycleAppliance, func() vmLifecycleResult {
		err := owner.activateApplianceOwned(ctx, installationID)
		return vmLifecycleResult{err: err}
	}).err
}

func (owner *VMLifecycleOwner) activateApplianceOwned(ctx context.Context, installationID string) error {
	controller := owner.controller
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING || !owner.admitsRemoteWork() {
		if _, err := owner.startVM(ctx); err != nil {
			return err
		}
		installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
		if err != nil {
			owner.abandonCommit(err)
			return err
		}
		if err := controller.startEgress(); err != nil {
			owner.abandonCommit(err)
			return err
		}
		client, connection, err := controller.guestClient(ctx)
		if err != nil {
			owner.abandonCommit(err)
			return err
		}
		if err := controller.startAppliance(ctx, client, installation); err != nil {
			connection.Close()
			owner.abandonCommit(err)
			return err
		}
		connection.Close()
		vm := owner.manager.Snapshot()
		generation, _ := owner.admissionToken()
		if err := controller.finishRunningVM(ctx, generation, vm.GetBootId()); err != nil {
			owner.abandonCommit(err)
			return err
		}
		return nil
	}
	installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	return controller.startAppliance(ctx, client, installation)
}

func (owner *VMLifecycleOwner) Shutdown(ctx context.Context) error {
	// Do not publish a terminal shutdown owner until this call owns the
	// lifecycle lock. A caller deadline while queued must leave a later caller
	// able to perform the cleanup.
	if err := owner.lockOperation(ctx); err != nil {
		return err
	}
	defer owner.operationMu.Unlock()
	if owner.controller != nil {
		owner.controller.inventoryTransition.Lock()
		defer owner.controller.inventoryTransition.Unlock()
	}
	owner.stateMu.Lock()
	if owner.closing {
		done := owner.shutdownDone
		owner.stateMu.Unlock()
		select {
		case <-done:
			owner.stateMu.Lock()
			err := owner.shutdownErr
			owner.stateMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	owner.closing = true
	owner.closeAdmission()
	if owner.recovery != nil {
		owner.recovery()
	}
	owner.manager.ClearLifecycleHandlers()
	owner.stateMu.Unlock()
	owner.recoveryWG.Wait()
	cleanup := owner.shutdownCleanup
	if cleanup == nil {
		cleanup = func(ctx context.Context) error {
			owner.controller.prepareVMTransition(ctx)
			owner.controller.stopEgress()
			state := owner.manager.Snapshot().GetState()
			if state == hostv1.VMState_VM_STATE_RUNNING {
				_, gracefulErr := owner.manager.Stop(ctx, false)
				if gracefulErr == nil {
					return nil
				}
				// A graceful-stop deadline must not consume the force-stop budget.
				// The force phase owns a fresh deadline so shutdown cannot abandon a
				// still-attached VM merely because its parent is expiring.
				forceCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_, forceErr := owner.manager.Stop(forceCtx, true)
				if forceErr != nil {
					return errors.Join(gracefulErr, forceErr)
				}
				return nil
			}
			if owner.manager.HasMachineAttachment() {
				forceCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_, err := owner.manager.Stop(forceCtx, true)
				return err
			}
			return nil
		}
	}
	err := cleanup(ctx)
	owner.stateMu.Lock()
	owner.shutdownErr = err
	close(owner.shutdownDone)
	owner.stateMu.Unlock()
	return err
}

// abandonCommit closes admission and the command paths of a failed commit.
// The Guest and its workloads keep running, and the recovery goroutine retries
// the commit against the same machine. Only an explicit stop ends the VM.
func (owner *VMLifecycleOwner) abandonCommit(err error) {
	owner.controller.prepareVMTransition(context.Background())
	owner.controller.stopEgress()
	vm := owner.snapshot()
	owner.stateMu.Lock()
	owner.commitErr, owner.commitBootID = err, vm.GetBootId()
	owner.stateMu.Unlock()
	if vm.GetState() == hostv1.VMState_VM_STATE_RUNNING {
		owner.RecoverAsync("")
	}
}
