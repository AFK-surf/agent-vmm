//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"log"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

// An unexpected stop or a failed commit gets up to five recovery attempts,
// spaced 5, 10, 20, and 40 seconds apart. The 75 seconds of backoff outlast a
// transient hypervisor or Host resource failure inside the ten-minute recovery
// budget. Attempts count across stops and commits until a recovered VM runs for
// vmRecoveryStableAfter, so a Guest that crashes after each boot reaches FAILED
// instead of restarting forever, and a commit that keeps failing leaves the
// running VM closed to admission.
const (
	vmRecoveryAttempts    = 5
	vmRecoveryStableAfter = 10 * time.Minute
)

var vmRecoveryInitialDelay = 5 * time.Second

// RecoverAsync starts the single recovery goroutine. previousBootID names the
// boot that stopped unexpectedly; an empty ID retries the commit of the running
// VM. An unexpected stop during a commit retry turns that retry into a restart.
// A request that arrives after the last attempt read its target runs another
// recovery pass inside the same budget.
func (owner *VMLifecycleOwner) RecoverAsync(previousBootID string) {
	owner.stateMu.Lock()
	if owner.closing || owner.recovery != nil {
		if !owner.closing {
			owner.recoveryPending = true
			if previousBootID != "" {
				owner.recoveryBootID = previousBootID
			}
		}
		owner.stateMu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), unexpectedVMRecoveryTimeout)
	owner.recovery = cancel
	owner.recoveryBootID = previousBootID
	owner.recoveryWG.Add(1)
	owner.stateMu.Unlock()
	go func() {
		defer owner.recoveryWG.Done()
		defer cancel()
		for {
			owner.stateMu.Lock()
			owner.recoveryPending = false
			owner.stateMu.Unlock()
			owner.recover(ctx)
			owner.stateMu.Lock()
			if !owner.recoveryPending || owner.closing || ctx.Err() != nil {
				owner.recovery = nil
				owner.stateMu.Unlock()
				return
			}
			owner.stateMu.Unlock()
		}
	}()
}

func (owner *VMLifecycleOwner) recover(ctx context.Context) {
	attempt := owner.recoverAttempt
	if attempt == nil {
		attempt = owner.recoverOnce
	}
	if time.Since(owner.recoveredAt) >= vmRecoveryStableAfter {
		owner.recoveryAttempts = 0
	}
	err := errors.New("automatic VM recovery attempts are exhausted")
attempts:
	for owner.recoveryAttempts < vmRecoveryAttempts {
		if owner.recoveryAttempts > 0 {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break attempts
			case <-time.After(vmRecoveryInitialDelay << (owner.recoveryAttempts - 1)):
			}
		}
		owner.recoveryAttempts++
		owner.stateMu.Lock()
		previousBootID := owner.recoveryBootID
		owner.recoveryPending = false
		owner.stateMu.Unlock()
		if err = attempt(ctx, previousBootID); err == nil {
			owner.recoveredAt = time.Now()
			return
		}
		log.Printf("vm lifecycle recover attempt failed: attempt=%d error=%v", owner.recoveryAttempts, err)
	}
	if owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		log.Printf("vm lifecycle recover left the running VM closed to admission: error=%v", err)
		return
	}
	owner.manager.FailRecovery(err)
}

func (owner *VMLifecycleOwner) recoverOnce(ctx context.Context, previousBootID string) error {
	return owner.execute(ctx, vmLifecycleRecover, func() vmLifecycleResult {
		state := owner.snapshot().GetState()
		// An operator start can recover the VM between attempts.
		if state == hostv1.VMState_VM_STATE_RUNNING && owner.admitsRemoteWork() {
			return vmLifecycleResult{}
		}
		// A commit retry never starts a VM that another owner stopped.
		if previousBootID == "" && state != hostv1.VMState_VM_STATE_RUNNING {
			return vmLifecycleResult{}
		}
		log.Printf("vm lifecycle recover stage: stage=prepare_transition")
		owner.controller.prepareVMTransition(ctx)
		owner.controller.stopEgress()
		log.Printf("vm lifecycle recover stage: stage=machine_start")
		vm, err := owner.startVM(ctx)
		if err == nil && (vm.GetBootId() == "" || vm.GetBootId() == previousBootID) {
			err = errors.New("recovered VM did not publish a fresh boot ID")
		}
		if err == nil {
			log.Printf("vm lifecycle recover stage: stage=commit")
			err = owner.commit(ctx, "")
		}
		if err != nil {
			owner.abandonCommit(err)
		}
		return vmLifecycleResult{vm: vm, err: err}
	}).err
}
