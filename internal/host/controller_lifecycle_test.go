//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVMLifecycleOwnerSerializesOperations(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	run := func() vmLifecycleResult {
		current := active.Add(1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return vmLifecycleResult{}
	}
	done := make(chan struct{}, 2)
	go func() { owner.execute(context.Background(), vmLifecycleStart, run); done <- struct{}{} }()
	<-entered
	go func() { owner.execute(context.Background(), vmLifecycleStop, run); done <- struct{}{} }()
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

func TestVMRecoveryBacksOffAndBoundsAttemptsAcrossStops(t *testing.T) {
	originalDelay := vmRecoveryInitialDelay
	vmRecoveryInitialDelay = time.Millisecond
	t.Cleanup(func() { vmRecoveryInitialDelay = originalDelay })
	manager := &vmm.Manager{}
	owner := &VMLifecycleOwner{manager: manager, shutdownDone: make(chan struct{}), recoveryBootID: "boot"}
	attempts := 0
	succeedAt := 3
	owner.recoverAttempt = func(_ context.Context, previousBootID string) error {
		attempts++
		if previousBootID != "boot" {
			t.Fatalf("previous boot = %q", previousBootID)
		}
		if attempts == succeedAt {
			return nil
		}
		return errors.New("start failed")
	}
	owner.recover(context.Background())
	if attempts != 3 || manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_FAILED {
		t.Fatalf("first stop: attempts=%d vm=%+v", attempts, manager.Snapshot())
	}
	// The recovered VM stops again before it is stable; the attempts continue.
	succeedAt = 0
	owner.recover(context.Background())
	if attempts != vmRecoveryAttempts || manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_FAILED || !strings.Contains(manager.Snapshot().GetError(), "operator action required") {
		t.Fatalf("second stop: attempts=%d vm=%+v", attempts, manager.Snapshot())
	}
	owner.recoveredAt = time.Now().Add(-vmRecoveryStableAfter)
	owner.recover(context.Background())
	if attempts != 2*vmRecoveryAttempts {
		t.Fatalf("stable VM did not get a fresh recovery budget: attempts=%d", attempts)
	}
}

func TestUnexpectedStopTurnsCommitRetryIntoRestart(t *testing.T) {
	originalDelay := vmRecoveryInitialDelay
	vmRecoveryInitialDelay = time.Millisecond
	t.Cleanup(func() { vmRecoveryInitialDelay = originalDelay })
	owner := &VMLifecycleOwner{manager: &vmm.Manager{}, shutdownDone: make(chan struct{}), recovery: func() {}}
	var targets []string
	owner.recoverAttempt = func(_ context.Context, previousBootID string) error {
		targets = append(targets, previousBootID)
		if len(targets) == 1 {
			owner.RecoverAsync("running-boot")
			return errors.New("commit failed")
		}
		return nil
	}
	owner.recover(context.Background())
	if len(targets) != 2 || targets[0] != "" || targets[1] != "running-boot" {
		t.Fatalf("recovery targets = %q", targets)
	}
}

func TestRecoveryRequestAfterLastAttemptRunsAnotherPass(t *testing.T) {
	originalDelay := vmRecoveryInitialDelay
	vmRecoveryInitialDelay = time.Millisecond
	t.Cleanup(func() { vmRecoveryInitialDelay = originalDelay })
	owner := &VMLifecycleOwner{manager: &vmm.Manager{}, shutdownDone: make(chan struct{})}
	var targets []string
	owner.recoverAttempt = func(_ context.Context, previousBootID string) error {
		targets = append(targets, previousBootID)
		if len(targets) == 1 {
			owner.RecoverAsync("boot-two")
		}
		return nil
	}
	owner.RecoverAsync("boot-one")
	owner.recoveryWG.Wait()
	if len(targets) != 2 || targets[0] != "boot-one" || targets[1] != "boot-two" || owner.recovery != nil {
		t.Fatalf("recovery targets = %q", targets)
	}
}

func TestAdmissionHealthReportsCommitFailureOfCurrentBoot(t *testing.T) {
	bootID := "boot"
	owner := &VMLifecycleOwner{
		shutdownDone: make(chan struct{}),
		snapshot:     func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: bootID} },
	}
	if health := owner.admissionHealth(nil); health.GetState() != hostv1.HealthState_HEALTH_STATE_DEGRADED || health.GetReason() != "committing" {
		t.Fatalf("committing admission health = %+v", health)
	}
	owner.commitErr, owner.commitBootID = errors.New("policy replay failed"), bootID
	health := owner.admissionHealth(nil)
	if health.GetState() != hostv1.HealthState_HEALTH_STATE_UNAVAILABLE || !strings.Contains(health.GetReason(), "policy replay failed") {
		t.Fatalf("failed commit admission health = %+v", health)
	}
	bootID = "next-boot"
	if health := owner.admissionHealth(nil); health.GetState() != hostv1.HealthState_HEALTH_STATE_DEGRADED {
		t.Fatalf("previous boot commit failure reported for a new boot: %+v", health)
	}
	generation, _ := owner.admissionToken()
	if !owner.openAdmission(generation, bootID) {
		t.Fatal("admission did not open")
	}
	if health := owner.admissionHealth(nil); health.GetState() != hostv1.HealthState_HEALTH_STATE_HEALTHY || owner.commitErr != nil {
		t.Fatalf("open admission health = %+v commitErr=%v", health, owner.commitErr)
	}
}

func TestAbandonCommitKeepsGuestRunningAndRetriesCommit(t *testing.T) {
	originalDelay := vmRecoveryInitialDelay
	vmRecoveryInitialDelay = time.Millisecond
	t.Cleanup(func() { vmRecoveryInitialDelay = originalDelay })
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	controller := &Controller{manager: &vmm.Manager{}, store: database, egress: egress.New(), sessions: make(map[string]*sessionServer)}
	owner := newVMLifecycleOwner(controller, controller.manager)
	controller.vmLifecycle = owner
	owner.snapshot = func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "boot"} }
	retried := make(chan string, 1)
	owner.recoverAttempt = func(_ context.Context, previousBootID string) error {
		retried <- previousBootID
		return nil
	}
	generation, _ := owner.admissionToken()
	if !owner.openAdmission(generation, "boot") {
		t.Fatal("admission did not open")
	}
	// The caller owns the lifecycle, so a VM stop through the owner would block here.
	owner.operationMu.Lock()
	owner.abandonCommit(errors.New("policy replay failed"))
	owner.operationMu.Unlock()
	if owner.admitsRemoteWork() || owner.commitBootID != "boot" {
		t.Fatalf("abandoned commit: admits=%t commitBoot=%q", owner.admitsRemoteWork(), owner.commitBootID)
	}
	select {
	case target := <-retried:
		if target != "" {
			t.Fatalf("commit retry restarts boot %q", target)
		}
	case <-time.After(time.Second):
		t.Fatal("abandoned commit was not retried")
	}
	owner.recoveryWG.Wait()
}

func TestCommitRetryDoesNotStartStoppedVM(t *testing.T) {
	owner := &VMLifecycleOwner{
		manager: &vmm.Manager{}, shutdownDone: make(chan struct{}),
		snapshot: func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED} },
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
	owner := &VMLifecycleOwner{closing: true, shutdownDone: make(chan struct{})}
	ran := false
	result := owner.execute(context.Background(), vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	if result.err == nil || ran {
		t.Fatal("lifecycle operation entered execute after shutdown began")
	}
}

func TestVMLifecycleOwnerDoesNotRunExpiredQueuedOperation(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	go owner.execute(context.Background(), vmLifecycleRecover, func() vmLifecycleResult {
		close(entered)
		<-release
		return vmLifecycleResult{}
	})
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	ran := false
	result := owner.execute(ctx, vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	close(release)

	if result.err == nil || ran {
		t.Fatal("expired queued lifecycle operation was executed")
	}
}

func TestVMLifecycleOwnerDoesNotRunExpiredOperationWhenLockIsFree(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	result := owner.execute(ctx, vmLifecycleStart, func() vmLifecycleResult {
		ran = true
		return vmLifecycleResult{}
	})
	if !errors.Is(result.err, context.Canceled) || ran {
		t.Fatalf("expired operation result=%v ran=%t", result.err, ran)
	}
}

func TestVMLifecycleOwnerTimedOutShutdownCanBeRetried(t *testing.T) {
	owner := &VMLifecycleOwner{manager: &vmm.Manager{}, shutdownDone: make(chan struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	go owner.execute(context.Background(), vmLifecycleRecover, func() vmLifecycleResult {
		close(entered)
		<-release
		return vmLifecycleResult{}
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
	owner.shutdownCleanup = func(context.Context) error {
		cleaned = true
		return nil
	}
	if err := owner.Shutdown(context.Background()); err != nil || !cleaned {
		t.Fatalf("retried shutdown error = %v, cleaned = %t", err, cleaned)
	}
}

func TestVMLifecycleOwnerAdmissionStartsAndTransitionsClosed(t *testing.T) {
	owner := &VMLifecycleOwner{shutdownDone: make(chan struct{})}
	if owner.admitsRemoteWork() {
		t.Fatal("new lifecycle owner admitted remote work before a healthy commit")
	}
	owner.admission.Store(1)
	owner.closeAdmission()
	if owner.admitsRemoteWork() {
		t.Fatal("lifecycle transition did not synchronously close admission")
	}
}

func TestTerminalTransitionBetweenCommitAndOpenCannotReopenAdmission(t *testing.T) {
	owner := &VMLifecycleOwner{
		shutdownDone: make(chan struct{}),
		snapshot:     func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "fresh-boot"} },
	}
	generation, _ := owner.admissionToken()
	owner.beforeAdmissionOpen = owner.closeAdmission
	if owner.openAdmission(generation, "fresh-boot") {
		t.Fatal("stale commit reopened admission after terminal transition")
	}
	if owner.admitsRemoteWork() {
		t.Fatal("terminal transition left admission open")
	}
}

func TestVMStartDoesNotReenterLifecycleOwnerForPendingApplianceRemoval(t *testing.T) {
	if os.Getenv("AGENT_VMM_TEST_START_PENDING_REMOVE") != "1" {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestVMStartDoesNotReenterLifecycleOwnerForPendingApplianceRemoval$")
		command.Env = append(os.Environ(), "AGENT_VMM_TEST_START_PENDING_REMOVE=1", "TMPDIR="+t.TempDir())
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("VM Start subprocess: %v\n%s", err, output)
		}
		return
	}
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.SetRemoteEnrollment(ctx, registration.ID, registration.Revision, registration.EnrollmentState,
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL), "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "pending-remove", InstallationID: installation.ID,
		RequestID: "original-remove", Kind: 7, State: 1, Phase: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "active-appliance", InstallationID: installation.ID,
		RequestID: "active-appliance", Kind: 3, State: 1, Phase: 2}); err != nil {
		t.Fatal(err)
	}
	controller.manager = &vmm.Manager{}
	controller.egress = egress.New()
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	defer hostLockWatchdog(t)()
	_, err = controller.startVM(ctx, "start-with-pending-remove", &hostv1.VMMutationFence{})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("VM Start did not reach its own fence without consuming Remove: %v", err)
	}
	operation, err := controller.store.ApplianceOperationByRequest(ctx, "original-remove")
	if err != nil || operation.State != 1 || operation.Phase != 4 {
		t.Fatalf("VM Start consumed another operation's pending removal: %+v %v", operation, err)
	}
	operation, err = controller.store.ApplianceOperationByRequest(ctx, "active-appliance")
	if err != nil || operation.State != 1 || operation.Phase != 2 {
		t.Fatalf("VM Start classified an active appliance command as Host restart: %+v %v", operation, err)
	}
}
