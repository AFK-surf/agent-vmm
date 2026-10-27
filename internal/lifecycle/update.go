//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type updateApplicationActions struct {
	releaseID func(string) (string, error)
	drain     func(paths) error
	replace   func(string, string) error
	repair    func(paths) error
	startVM   func(context.Context, paths, string) error
	remove    func(string) error
}

func updateApplicationWith(value paths, sourceApp, targetReleaseID, requestID string, actions updateApplicationActions) error {
	if err := validatePreparedUpdate(value, sourceApp); err != nil {
		return err
	}
	operation := readLifecycleRequest(value, requestID)
	if operation == nil {
		return errors.New("lifecycle update is not owned by this request")
	}
	progress := operation.Cutover
	if progress == nil {
		progress = &cutoverProgress{}
	}
	targetRelease, targetErr := actions.releaseID(value.app)
	if targetErr == nil && targetRelease == targetReleaseID {
		progress.SoftwareInstalled = true
	}
	save := func(stage string) error {
		current := readLifecycleRequest(value, requestID)
		if current == nil || current.TargetReleaseID != targetReleaseID {
			return errors.New("lifecycle update target is not owned by this request")
		}
		if stage == "restore" && current.Stage != stage {
			current.Attempt = 1
		}
		current.Stage, current.Cutover = stage, progress
		current.UpdatedAt = time.Now().UnixMilli()
		if err := persistOperationOwned(value, *current, lifecycleOperationOwner(value)); err != nil {
			return err
		}
		operation = current
		return nil
	}
	verify := func() error {
		if err := actions.remove(sourceApp); err != nil {
			return fmt.Errorf("remove retired Host bundle: %w", err)
		}
		return nil
	}
	if operation.Stage == "settled" {
		if err := verify(); err != nil {
			return err
		}
		progress.EffectsVerified = true
		return save("settled")
	}
	return cutover(progress, false, false, save, cutoverActions{
		stop:    func() error { return actions.drain(value) },
		replace: func() error { return actions.replace(sourceApp, value.app) },
		restore: func() error {
			return restoreHost(value, maintenanceRestoreRequestID(requestID, uint64(operation.Attempt)), actions.repair, actions.startVM)
		},
		verify: verify,
	})
}
