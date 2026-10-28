//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/locks"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Operation uint8

const (
	Start Operation = iota + 1
	Stop
	Recover
	Appliance
	Shutdown
)

type Owner struct {
	manager          Manager
	store            *store.Store
	guest            *guestconn.Connection
	inventory        *InventoryFence
	workloads        Workloads
	services         Services
	remote           Remote
	sessions         Sessions
	appliances       Appliances
	hostShares       HostShares
	applianceStarter ApplianceStarter
	logger           *slog.Logger

	operationMu locks.RWMutex
	stateMu     sync.Mutex
	closing     bool
	recovery    context.CancelFunc
	recoveryWG  sync.WaitGroup
	shutdownErr error
	admission   atomic.Uint64
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
	// Recovery attempts belong to the recovery goroutine.
	recoveryAttempts int
	// admittedAt uses stateMu.
	admittedAt time.Time
}

type Manager interface {
	Snapshot() *hostv1.VM
	Start(context.Context) (*hostv1.VM, error)
	Stop(context.Context, bool) (*hostv1.VM, error)
	HasMachineAttachment() bool
	ClearLifecycleHandlers()
	FailRecovery(error)
	DialBoot(string) (*grpc.ClientConn, error)
}
type Workloads interface {
	RecoverWorkloadRuns(context.Context) error
	ResetWorkloadRecovery()
}
type Services interface {
	StartEgress() error
	StopEgress()
	ReconcilePolicies(context.Context) error
}
type Remote interface {
	ReconcileExternalConnectors(context.Context) error
	StopAllExternalConnectors()
}
type Sessions interface{ RevokeAllSessions(context.Context) }
type Appliances interface{ InvalidateAppliances() }
type HostShares interface{ PrepareHostShares(context.Context) error }
type ApplianceStarter interface {
	StartAppliance(context.Context, vmmdv1.VMMDServiceClient, store.ApplianceInstallation) error
}
type Config struct {
	Manager          Manager
	Store            *store.Store
	Guest            *guestconn.Connection
	Inventory        *InventoryFence
	Workloads        Workloads
	Services         Services
	Remote           Remote
	Sessions         Sessions
	Appliances       Appliances
	HostShares       HostShares
	ApplianceStarter ApplianceStarter
	Logger           *slog.Logger
}

func New(config Config) *Owner {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Owner{manager: config.Manager, store: config.Store, guest: config.Guest, inventory: config.Inventory,
		workloads: config.Workloads, services: config.Services, remote: config.Remote, sessions: config.Sessions,
		appliances: config.Appliances, hostShares: config.HostShares, applianceStarter: config.ApplianceStarter,
		logger: logger.With("component", "host.lifecycle")}
}

func (owner *Owner) Run(ctx context.Context, operation Operation, validate func() error, fn func(context.Context) error) error {
	started := time.Now()
	owner.logger.Info("vm lifecycle operation waiting", "kind", operation)
	if err := owner.operationMu.Lock(ctx); err != nil {
		return err
	}
	defer owner.operationMu.Unlock()
	owner.stateMu.Lock()
	closing := owner.closing
	owner.stateMu.Unlock()
	if closing && operation != Shutdown {
		return errors.New("VM lifecycle is shutting down")
	}
	owner.logger.Info("vm lifecycle operation acquired", "kind", operation, "elapsed", time.Since(started))
	if validate != nil {
		if err := validate(); err != nil {
			return err
		}
	}
	err := fn(ctx)
	owner.logger.Info("vm lifecycle operation completed", "kind", operation, "elapsed", time.Since(started), "error", err)
	return err
}

func (operation Operation) String() string {
	switch operation {
	case Start:
		return "start"
	case Stop:
		return "stop"
	case Recover:
		return "recover"
	case Appliance:
		return "appliance"
	case Shutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

func (owner *Owner) AdmitsRemoteWork() bool {
	if owner == nil {
		return false
	}
	_, open := owner.admissionToken()
	return open
}

func (owner *Owner) admissionToken() (uint64, bool) {
	state := owner.admission.Load()
	return state >> 1, state&1 == 1
}

func (owner *Owner) CloseAdmission() {
	for {
		state := owner.admission.Load()
		if owner.admission.CompareAndSwap(state, ((state>>1)+1)<<1) {
			return
		}
	}
}

func (owner *Owner) openAdmission(generation uint64, bootID string) bool {
	owner.stateMu.Lock()
	defer owner.stateMu.Unlock()
	if owner.closing {
		return false
	}
	vm := owner.manager.Snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING || bootID == "" || vm.GetBootId() != bootID {
		return false
	}
	if !owner.admission.CompareAndSwap(generation<<1, generation<<1|1) {
		return false
	}
	owner.commitErr, owner.commitBootID = nil, ""
	owner.admittedAt = time.Now()
	return true
}

// AdmissionHealth reports whether a running VM admits remote work. Closed
// admission is a failure only after a commit of the current boot failed;
// otherwise the commit of that boot is still in progress.
func (owner *Owner) AdmissionHealth(observedAt *timestamppb.Timestamp) *hostv1.ComponentHealth {
	health := &hostv1.ComponentHealth{Component: "host-admission", State: hostv1.HealthState_HEALTH_STATE_HEALTHY, ObservedAt: observedAt}
	if owner.AdmitsRemoteWork() {
		return health
	}
	bootID := owner.manager.Snapshot().GetBootId()
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

func (owner *Owner) commit(ctx context.Context, activate string) (err error) {
	defer func() {
		if err != nil {
			owner.abandonCommit(ctx, err)
		}
	}()
	owner.stateMu.Lock()
	generation, _ := owner.admissionToken()
	bootID := owner.manager.Snapshot().GetBootId()
	owner.stateMu.Unlock()
	if err := owner.guest.Connect(owner.manager, bootID); err != nil {
		return err
	}
	if err := owner.services.StartEgress(); err != nil {
		return err
	}
	if err := owner.workloads.RecoverWorkloadRuns(ctx); err != nil {
		return err
	}
	if err := owner.services.ReconcilePolicies(ctx); err != nil {
		return err
	}
	if err := owner.remote.ReconcileExternalConnectors(ctx); err != nil {
		return err
	}
	if activate != "" {
		installation, err := owner.store.GetApplianceInstallation(ctx, activate)
		if err != nil {
			return err
		}
		client, err := owner.guest.Client()
		if err != nil {
			return err
		}
		if err := owner.applianceStarter.StartAppliance(ctx, client, installation); err != nil {
			return err
		}
	}
	if !owner.openAdmission(generation, bootID) {
		return errors.New("VM lifecycle admission changed")
	}
	return nil
}

func (owner *Owner) startVM(ctx context.Context) (*hostv1.VM, error) {
	state := owner.manager.Snapshot().GetState()
	if (state == hostv1.VMState_VM_STATE_STOPPED || state == hostv1.VMState_VM_STATE_FAILED) && !owner.manager.HasMachineAttachment() {
		if err := owner.hostShares.PrepareHostShares(ctx); err != nil {
			return nil, err
		}
	}
	return owner.manager.Start(ctx)
}

func (owner *Owner) StartOwned(ctx context.Context) error {
	if owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING && owner.AdmitsRemoteWork() {
		return nil
	}
	return owner.startAndCommit(ctx, "", "", true)
}

func (owner *Owner) startAndCommit(ctx context.Context, activate, previousBootID string, recoverFailure bool) (err error) {
	release, err := owner.inventory.Transition(ctx)
	if err != nil {
		return err
	}
	defer release()
	owner.logger.Info("vm lifecycle start stage", "stage", "prepare_transition")
	owner.prepareVMTransition(ctx)
	if owner.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		owner.logger.Info("vm lifecycle start stage", "stage", "machine_start")
		var vm *hostv1.VM
		vm, err = owner.startVM(ctx)
		if err == nil && previousBootID != "" && (vm.GetBootId() == "" || vm.GetBootId() == previousBootID) {
			err = errors.New("recovered VM did not publish a fresh boot ID")
		}
	}
	if err == nil {
		owner.logger.Info("vm lifecycle start stage", "stage", "commit")
		err = owner.commit(ctx, activate)
	} else {
		owner.abandonCommit(ctx, err)
	}
	if err != nil && recoverFailure && owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		owner.RecoverAsync("")
	}
	return err
}

func (owner *Owner) StopOwned(ctx context.Context, force bool) error {
	release, transitionErr := owner.inventory.Transition(ctx)
	if transitionErr != nil {
		return transitionErr
	}
	defer release()
	owner.prepareVMTransition(ctx)
	_, err := owner.manager.Stop(ctx, force)
	if err != nil && owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if commitErr := owner.commit(ctx, ""); commitErr != nil {
			owner.RecoverAsync("")
			err = errors.Join(err, commitErr)
		}
	}
	return err
}

func (owner *Owner) ActivateApplianceOwned(ctx context.Context, installationID string) error {
	if owner.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING || !owner.AdmitsRemoteWork() {
		return owner.startAndCommit(ctx, installationID, "", true)
	}
	installation, err := owner.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	client, err := owner.guest.Client()
	if err != nil {
		return err
	}
	return owner.applianceStarter.StartAppliance(ctx, client, installation)
}

func (owner *Owner) Shutdown(ctx context.Context) error {
	return owner.Run(ctx, Shutdown, nil, owner.shutdownOwned)
}

func (owner *Owner) shutdownOwned(ctx context.Context) error {
	release, err := owner.inventory.Transition(ctx)
	if err != nil {
		return err
	}
	defer release()
	owner.stateMu.Lock()
	if owner.closing {
		err := owner.shutdownErr
		owner.stateMu.Unlock()
		return err
	}
	owner.closing = true
	owner.CloseAdmission()
	if owner.recovery != nil {
		owner.recovery()
	}
	owner.manager.ClearLifecycleHandlers()
	owner.stateMu.Unlock()
	owner.recoveryWG.Wait()
	err = owner.stopForShutdown(ctx)
	owner.stateMu.Lock()
	owner.shutdownErr = err
	owner.stateMu.Unlock()
	return err
}

func (owner *Owner) stopForShutdown(ctx context.Context) error {
	owner.prepareVMTransition(ctx)
	// The Linux VMM owns its process and disks across Host restarts.
	if runtime.GOOS == "linux" {
		return nil
	}
	if owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		_, gracefulErr := owner.manager.Stop(ctx, false)
		if gracefulErr == nil {
			return nil
		}
		// Force stop owns a fresh deadline after a graceful-stop timeout.
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

// abandonCommit closes admission and the command paths of a failed commit.
// The Guest and its workloads keep running, and the recovery goroutine retries
// the commit against the same machine. Only an explicit stop ends the VM.
func (owner *Owner) abandonCommit(ctx context.Context, err error) {
	owner.CloseAdmission()
	owner.remote.StopAllExternalConnectors()
	owner.invalidateRuntimeCapabilities(ctx)
	owner.services.StopEgress()
	vm := owner.manager.Snapshot()
	owner.stateMu.Lock()
	owner.commitErr, owner.commitBootID = err, vm.GetBootId()
	owner.stateMu.Unlock()
}

func (owner *Owner) Snapshot() *hostv1.VM { return owner.manager.Snapshot() }
func (owner *Owner) prepareVMTransition(ctx context.Context) {
	owner.guest.Disconnect()
	owner.workloads.ResetWorkloadRecovery()
	owner.CloseAdmission()
	owner.remote.StopAllExternalConnectors()
	owner.invalidateRuntimeCapabilities(ctx)
	owner.services.StopEgress()
}
func (owner *Owner) invalidateRuntimeCapabilities(ctx context.Context) {
	owner.appliances.InvalidateAppliances()
	owner.sessions.RevokeAllSessions(ctx)
	if err := owner.store.DisconnectRemoteConnections(ctx, time.Now()); err != nil {
		owner.stateMu.Lock()
		owner.commitErr, owner.commitBootID = err, owner.manager.Snapshot().GetBootId()
		owner.stateMu.Unlock()
	}
}
