//go:build darwin || linux

package lifecycle

import (
	"errors"
	"fmt"
)

type updateApplicationActions struct {
	releaseID func(string, bool) (string, error)
	drain     func(paths) error
	replace   func(string, string) error
	repair    func(paths) error
	startVM   func(paths, string) error
	remove    func(string) error
}

func updateApplicationWith(value paths, sourceApp, targetReleaseID, requestID string, actions updateApplicationActions) error {
	if err := validatePreparedUpdate(value, sourceApp); err != nil {
		return err
	}
	targetRelease, targetErr := actions.releaseID(value.app, false)
	sourceRelease, sourceErr := actions.releaseID(sourceApp, true)
	if targetErr == nil && targetRelease == targetReleaseID {
		if err := persistUpdateStage(value, requestID, targetReleaseID, "recovery"); err != nil {
			return err
		}
		if err := actions.repair(value); err != nil {
			return fmt.Errorf("restore updated Host service: %w", err)
		}
		if err := actions.startVM(value, requestID+"-restore"); err != nil {
			return fmt.Errorf("restore updated VM: %w", err)
		}
		if err := actions.remove(sourceApp); err != nil {
			return errors.New("retired Host bundle could not be removed")
		}
		return persistUpdateStage(value, requestID, targetReleaseID, "complete")
	}
	if targetErr != nil {
		return fmt.Errorf("current Host release is unreadable: %w", targetErr)
	}
	if sourceErr != nil || sourceRelease != targetReleaseID {
		return errors.New("prepared Host release does not match the selected target")
	}
	if err := persistUpdateStage(value, requestID, targetReleaseID, "stop"); err != nil {
		return err
	}
	if err := actions.drain(value); err != nil {
		return fmt.Errorf("stop current Host release: %w", err)
	}
	if err := persistUpdateStage(value, requestID, targetReleaseID, "replace"); err != nil {
		return err
	}
	if err := actions.replace(sourceApp, value.app); err != nil {
		return err
	}
	if err := persistUpdateStage(value, requestID, targetReleaseID, "recovery"); err != nil {
		return err
	}
	if err := actions.repair(value); err != nil {
		return fmt.Errorf("restore updated Host service: %w", err)
	}
	if err := actions.startVM(value, requestID+"-restore"); err != nil {
		return fmt.Errorf("restore updated VM: %w", err)
	}
	if err := actions.remove(sourceApp); err != nil {
		return errors.New("retired Host bundle could not be removed")
	}
	return persistUpdateStage(value, requestID, targetReleaseID, "complete")
}
