//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
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
	RecoveryTimeout       = 10 * time.Minute
	vmRecoveryAttempts    = 5
	vmRecoveryStableAfter = 10 * time.Minute
)

var vmRecoveryInitialDelay = 5 * time.Second

// RecoverAsync starts the single recovery goroutine. previousBootID names the
// boot that stopped unexpectedly; an empty ID retries the commit of the running
// VM. An unexpected stop during a commit retry turns that retry into a restart.
// A request that arrives after the last attempt read its target runs another
// recovery pass inside the same budget.
func (owner *Owner) RecoverAsync(previousBootID string) {
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
	ctx, cancel := context.WithTimeout(context.Background(), RecoveryTimeout)
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
			if !owner.recoveryPending || owner.closing || ctx.Err() != nil || owner.recoveryAttempts >= vmRecoveryAttempts {
				owner.recoveryPending = false
				owner.recovery = nil
				owner.stateMu.Unlock()
				return
			}
			owner.stateMu.Unlock()
		}
	}()
}

func (owner *Owner) recover(ctx context.Context) {
	owner.stateMu.Lock()
	if !owner.admittedAt.IsZero() && time.Since(owner.admittedAt) >= vmRecoveryStableAfter {
		owner.recoveryAttempts = 0
		owner.admittedAt = time.Time{}
	}
	owner.stateMu.Unlock()
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
		if err = owner.recoverOnce(ctx, previousBootID); err == nil {
			return
		}
		owner.logger.Warn("vm lifecycle recover attempt failed", "attempt", owner.recoveryAttempts, "error", err)
	}
	if owner.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		owner.logger.Warn("vm lifecycle recover left the running VM closed to admission", "error", err)
		return
	}
	owner.manager.FailRecovery(err)
}

func (owner *Owner) recoverOnce(ctx context.Context, previousBootID string) error {
	return owner.Run(ctx, Recover, nil, func(ctx context.Context) error {
		state := owner.manager.Snapshot().GetState()
		// An operator start can recover the VM between attempts.
		if state == hostv1.VMState_VM_STATE_RUNNING && owner.AdmitsRemoteWork() {
			return nil
		}
		// A commit retry never starts a VM that another owner stopped.
		if previousBootID == "" && state != hostv1.VMState_VM_STATE_RUNNING {
			return nil
		}
		return owner.startAndCommit(ctx, "", previousBootID, false)
	})
}
