//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
)

func TestVMLifecycleOwnerSerializesOperations(t *testing.T) {
	owner := &Owner{logger: slog.Default()}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	Run := func(context.Context) error {
		current := active.Add(1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	}
	done := make(chan struct{}, 2)
	go func() { owner.Run(context.Background(), Start, nil, Run); done <- struct{}{} }()
	<-entered
	go func() { owner.Run(context.Background(), Stop, nil, Run); done <- struct{}{} }()
	select {
	case <-entered:
		t.Fatal("a second lifecycle operation executed concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	<-entered
	release <- struct{}{}
	<-done
	<-done
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent lifecycle operations = %d, want 1", maximum.Load())
	}
}

func TestVMRecoveryBacksOffAndStopsAfterExhaustion(t *testing.T) {
	originalDelay := vmRecoveryInitialDelay
	vmRecoveryInitialDelay = time.Millisecond
	t.Cleanup(func() { vmRecoveryInitialDelay = originalDelay })
	owner := lifecycleTestOwner(t)
	owner.manager = &lifecycleTestManager{snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "boot"} }}
	started := time.Now()
	owner.RecoverAsync("")
	owner.recoveryWG.Wait()
	if owner.recoveryAttempts != vmRecoveryAttempts || owner.recoveryPending || owner.recovery != nil || owner.commitErr == nil {
		t.Fatalf("recovery attempts=%d pending=%v error=%v", owner.recoveryAttempts, owner.recoveryPending, owner.commitErr)
	}
	if time.Since(started) < 15*time.Millisecond {
		t.Fatal("recovery did not back off")
	}
	owner.recover(context.Background())
	if owner.recoveryAttempts != vmRecoveryAttempts {
		t.Fatal("exhausted recovery started another round")
	}
	generation, _ := owner.admissionToken()
	if !owner.openAdmission(generation, "boot") || owner.admittedAt.IsZero() {
		t.Fatal("successful activation did not start the stable period")
	}
	owner.admittedAt = owner.admittedAt.Add(-vmRecoveryStableAfter)
	owner.CloseAdmission()
	owner.recover(context.Background())
	if owner.recoveryAttempts != vmRecoveryAttempts || !owner.admittedAt.IsZero() || owner.commitErr == nil {
		t.Fatal("stable-period recovery did not receive a new bounded round")
	}
}

func TestAdmissionHealthReportsCommitFailureOfCurrentBoot(t *testing.T) {
	bootID := "boot"
	owner := &Owner{logger: slog.Default(),
		manager: &lifecycleTestManager{snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: bootID} }},
	}
	if health := owner.AdmissionHealth(nil); health.GetState() != hostv1.HealthState_HEALTH_STATE_DEGRADED || health.GetReason() != "committing" {
		t.Fatalf("committing admission health = %+v", health)
	}
	owner.commitErr, owner.commitBootID = errors.New("policy replay failed"), bootID
	health := owner.AdmissionHealth(nil)
	if health.GetState() != hostv1.HealthState_HEALTH_STATE_UNAVAILABLE || !strings.Contains(health.GetReason(), "policy replay failed") {
		t.Fatalf("failed commit admission health = %+v", health)
	}
	bootID = "next-boot"
	if health := owner.AdmissionHealth(nil); health.GetState() != hostv1.HealthState_HEALTH_STATE_DEGRADED {
		t.Fatalf("previous boot commit failure reported for a new boot: %+v", health)
	}
	generation, _ := owner.admissionToken()
	if !owner.openAdmission(generation, bootID) {
		t.Fatal("admission did not open")
	}
	if health := owner.AdmissionHealth(nil); health.GetState() != hostv1.HealthState_HEALTH_STATE_HEALTHY || owner.commitErr != nil {
		t.Fatalf("open admission health = %+v commitErr=%v", health, owner.commitErr)
	}
}

func TestAbandonCommitKeepsGuestRunningWithoutSchedulingRecovery(t *testing.T) {
	originalDelay := vmRecoveryInitialDelay
	vmRecoveryInitialDelay = time.Millisecond
	t.Cleanup(func() { vmRecoveryInitialDelay = originalDelay })
	owner := lifecycleTestOwner(t)
	owner.manager = &lifecycleTestManager{snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "boot"} }}
	generation, _ := owner.admissionToken()
	if !owner.openAdmission(generation, "boot") {
		t.Fatal("admission did not open")
	}
	// The caller owns the lifecycle, so a VM stop through the owner would block here.
	owner.operationMu.Lock(context.Background())
	owner.abandonCommit(context.Background(), errors.New("policy replay failed"))
	owner.operationMu.Unlock()
	if owner.AdmitsRemoteWork() || owner.commitBootID != "boot" {
		t.Fatalf("abandoned commit: admits=%t commitBoot=%q", owner.AdmitsRemoteWork(), owner.commitBootID)
	}
	if owner.recovery != nil || owner.recoveryPending {
		t.Fatal("abandonCommit scheduled recovery")
	}
}

func TestCommitRetryDoesNotStartStoppedVM(t *testing.T) {
	owner := &Owner{logger: slog.Default(),
		manager: &lifecycleTestManager{snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED} }},
	}
	generation, _ := owner.admissionToken()
	// A nil controller fails any transition that the retry would start.
	if err := owner.recoverOnce(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if current, _ := owner.admissionToken(); current != generation {
		t.Fatal("commit retry on a stopped VM closed admission")
	}
}

func TestVMLifecycleOwnerRejectsOperationsAfterShutdownBegins(t *testing.T) {
	owner := &Owner{logger: slog.Default(), closing: true}
	ran := false
	result := owner.Run(context.Background(), Start, nil, func(context.Context) error {
		ran = true
		return nil
	})
	if result == nil || ran {
		t.Fatal("lifecycle operation entered execute after shutdown began")
	}
}

func TestVMLifecycleOwnerDoesNotRunExpiredQueuedOperation(t *testing.T) {
	owner := &Owner{logger: slog.Default()}
	entered := make(chan struct{})
	release := make(chan struct{})
	go owner.Run(context.Background(), Recover, nil, func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	ran := false
	result := owner.Run(ctx, Start, nil, func(context.Context) error {
		ran = true
		return nil
	})
	close(release)

	if result == nil || ran {
		t.Fatal("expired queued lifecycle operation was executed")
	}
}

func TestVMLifecycleOwnerDoesNotRunExpiredOperationWhenLockIsFree(t *testing.T) {
	owner := &Owner{logger: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	result := owner.Run(ctx, Start, nil, func(context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(result, context.Canceled) || ran {
		t.Fatalf("expired operation result=%v ran=%t", result, ran)
	}
}

func TestVMLifecycleOwnerTimedOutShutdownCanBeRetried(t *testing.T) {
	owner := lifecycleTestOwner(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	go owner.Run(context.Background(), Recover, nil, func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := owner.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued shutdown error = %v, want deadline exceeded", err)
	}
	owner.stateMu.Lock()
	closing := owner.closing
	owner.stateMu.Unlock()
	if closing {
		t.Fatal("timed-out queued shutdown permanently claimed shutdown ownership")
	}

	close(release)
	cleaned := false
	owner.manager = &lifecycleTestManager{snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING} }, stop: func(context.Context, bool) (*hostv1.VM, error) { cleaned = true; return &hostv1.VM{}, nil }}
	if err := owner.Shutdown(context.Background()); err != nil || !cleaned {
		t.Fatalf("retried shutdown error = %v, cleaned = %t", err, cleaned)
	}
}

func TestVMLifecycleOwnerAdmissionStartsAndTransitionsClosed(t *testing.T) {
	owner := &Owner{logger: slog.Default()}
	if owner.AdmitsRemoteWork() {
		t.Fatal("new lifecycle owner admitted remote work before a healthy commit")
	}
	owner.admission.Store(1)
	owner.CloseAdmission()
	if owner.AdmitsRemoteWork() {
		t.Fatal("lifecycle transition did not synchronously close admission")
	}
}

func TestTerminalTransitionBetweenCommitAndOpenCannotReopenAdmission(t *testing.T) {
	owner := &Owner{logger: slog.Default(),
		manager: &lifecycleTestManager{snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "fresh-boot"} }},
	}
	generation, _ := owner.admissionToken()
	owner.manager = &lifecycleTestManager{snapshot: func() *hostv1.VM {
		owner.CloseAdmission()
		return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "fresh-boot"}
	}}
	if owner.openAdmission(generation, "fresh-boot") {
		t.Fatal("stale commit reopened admission after terminal transition")
	}
	if owner.AdmitsRemoteWork() {
		t.Fatal("terminal transition left admission open")
	}
}

type lifecycleTestRuntime struct{}

func (lifecycleTestRuntime) RecoverWorkloadRuns(context.Context) error         { return nil }
func (lifecycleTestRuntime) ResetWorkloadRecovery()                            {}
func (lifecycleTestRuntime) StartEgress() error                                { return nil }
func (lifecycleTestRuntime) StopEgress()                                       {}
func (lifecycleTestRuntime) ReconcilePolicies(context.Context) error           { return nil }
func (lifecycleTestRuntime) ReconcileExternalConnectors(context.Context) error { return nil }
func (lifecycleTestRuntime) StopAllExternalConnectors()                        {}
func (lifecycleTestRuntime) RevokeAllSessions(context.Context)                 {}
func (lifecycleTestRuntime) InvalidateAppliances()                             {}
func lifecycleTestOwner(t *testing.T) *Owner {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	runtime := lifecycleTestRuntime{}
	return New(Config{Manager: &vmm.Manager{}, Store: database, Guest: &guestconn.Connection{}, Inventory: &InventoryFence{}, Workloads: runtime, Services: runtime, Remote: runtime, Sessions: runtime, Appliances: runtime})
}

type lifecycleTestManager struct {
	Manager
	snapshot func() *hostv1.VM
	stop     func(context.Context, bool) (*hostv1.VM, error)
}

func (manager *lifecycleTestManager) Snapshot() *hostv1.VM { return manager.snapshot() }
func (manager *lifecycleTestManager) Stop(ctx context.Context, force bool) (*hostv1.VM, error) {
	return manager.stop(ctx, force)
}
func (*lifecycleTestManager) ClearLifecycleHandlers() {}
func (*lifecycleTestManager) DialBoot(string) (*grpc.ClientConn, error) {
	return nil, errors.New("Guest unavailable")
}
