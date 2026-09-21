//go:build darwin

package host

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
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
	return owner.admission.CompareAndSwap(generation<<1, generation<<1|1)
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
			owner.rollbackRunningVM()
		}
		return err
	}
	log.Printf("vm lifecycle start stage: stage=configure_appliance_blocks")
	if err := owner.controller.configureApplianceBlocks(ctx); err != nil {
		return err
	}
	log.Printf("vm lifecycle start stage: stage=vz_start")
	_, err := owner.manager.Start(ctx)
	if err == nil {
		log.Printf("vm lifecycle start stage: stage=commit")
		err = owner.commit(ctx, "")
	}
	if err != nil {
		owner.rollbackRunningVM()
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
		err = errors.Join(err, owner.commit(context.Background(), ""))
	}
	return err
}

func (owner *VMLifecycleOwner) RecoverAsync(previousBootID string) {
	owner.stateMu.Lock()
	if owner.closing {
		owner.stateMu.Unlock()
		return
	}
	owner.recoveryWG.Add(1)
	owner.stateMu.Unlock()
	go func() {
		defer owner.recoveryWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), unexpectedVMRecoveryTimeout)
		owner.stateMu.Lock()
		if owner.closing {
			owner.stateMu.Unlock()
			cancel()
			return
		}
		owner.recovery = cancel
		owner.stateMu.Unlock()
		defer func() {
			owner.stateMu.Lock()
			owner.recovery = nil
			owner.stateMu.Unlock()
			cancel()
		}()
		owner.recover(ctx, previousBootID)
	}()
}

func (owner *VMLifecycleOwner) recover(ctx context.Context, previousBootID string) {
	result := owner.execute(ctx, vmLifecycleRecover, func() vmLifecycleResult {
		log.Printf("vm lifecycle recover stage: stage=prepare_transition")
		owner.controller.prepareVMTransition(ctx)
		owner.controller.stopEgress()
		log.Printf("vm lifecycle recover stage: stage=configure_appliance_blocks")
		if err := owner.controller.configureApplianceBlocks(ctx); err != nil {
			return vmLifecycleResult{err: err}
		}
		log.Printf("vm lifecycle recover stage: stage=vz_start")
		vm, err := owner.manager.Start(ctx)
		if err == nil && (vm.GetBootId() == "" || vm.GetBootId() == previousBootID) {
			err = errors.New("recovered VM did not publish a fresh boot ID")
		}
		if err == nil {
			log.Printf("vm lifecycle recover stage: stage=commit")
			err = owner.commit(ctx, "")
		}
		if err != nil {
			owner.rollbackRunningVM()
			owner.manager.FailRecovery(err)
		}
		return vmLifecycleResult{vm: vm, err: err}
	})
	_ = result
}

func (owner *VMLifecycleOwner) ActivateAppliance(ctx context.Context, installationID string) error {
	return owner.execute(ctx, vmLifecycleAppliance, func() vmLifecycleResult {
		err := owner.activateApplianceOwned(ctx, installationID)
		return vmLifecycleResult{err: err}
	}).err
}

func (owner *VMLifecycleOwner) activateApplianceOwned(ctx context.Context, installationID string) error {
	controller := owner.controller
	volume, err := controller.store.GetApplianceVolume(ctx, installationID)
	if err != nil {
		return err
	}
	legacyConversion := volume.Layout == store.ApplianceVolumeLayoutHostBlock
	if volume.Layout != store.ApplianceVolumeLayoutHostBlock && volume.Layout != store.ApplianceVolumeLayoutGuestImage {
		return errors.New("appliance data volume layout is invalid")
	}
	if controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING && legacyConversion && !controller.manager.HasApplianceBlock(installationID) {
		return errors.New("legacy appliance data conversion requires the next shared VM restart")
	}
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		if err := controller.configureApplianceBlocks(ctx); err != nil {
			return err
		}
		if _, err := owner.manager.Start(ctx); err != nil {
			return err
		}
		installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
		if err != nil {
			owner.rollbackRunningVM()
			return err
		}
		if err := controller.startEgress(); err != nil {
			owner.rollbackRunningVM()
			return err
		}
		client, connection, err := controller.guestClient(ctx)
		if err != nil {
			owner.rollbackRunningVM()
			return err
		}
		if err := controller.startAppliance(ctx, client, installation); err != nil {
			connection.Close()
			owner.rollbackRunningVM()
			return err
		}
		connection.Close()
		vm := owner.manager.Snapshot()
		generation, _ := owner.admissionToken()
		if err := controller.finishRunningVM(ctx, generation, vm.GetBootId()); err != nil {
			owner.rollbackRunningVM()
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
				// still-attached VZ machine merely because its parent is expiring.
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

func (owner *VMLifecycleOwner) rollbackRunningVM() {
	owner.controller.stopEgress()
	if owner.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = owner.manager.Stop(ctx, true)
}
