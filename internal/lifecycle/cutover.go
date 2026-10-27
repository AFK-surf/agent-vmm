//go:build darwin || linux

package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

type cutoverProgress struct {
	DataCleared       bool `json:"dataCleared,omitempty"`
	SoftwareRemoved   bool `json:"softwareRemoved,omitempty"`
	SoftwareInstalled bool `json:"softwareInstalled,omitempty"`
	EffectsVerified   bool `json:"effectsVerified,omitempty"`
}

type cutoverActions struct {
	stop, reset, replace, remove, verify func() error
	restore                              func() error
}

// cutover keeps installation, update, and removal on one recoverable step sequence.
func cutover(progress *cutoverProgress, reset, remove bool, save func(string) error, actions cutoverActions) error {
	step := func(stage string, action func() error) error {
		if err := save(stage); err != nil {
			return err
		}
		return action()
	}
	if progress.EffectsVerified {
		return nil
	}
	if !progress.SoftwareInstalled && !progress.SoftwareRemoved {
		if err := step("stop", actions.stop); err != nil {
			return err
		}
	}
	if reset && !progress.DataCleared {
		if err := step("reset", actions.reset); err != nil {
			return err
		}
		progress.DataCleared = true
		if err := save("reset"); err != nil {
			return err
		}
	}
	if remove {
		if !progress.SoftwareRemoved {
			if err := step("replace", actions.remove); err != nil {
				return err
			}
			progress.SoftwareRemoved = true
			if err := save("replace"); err != nil {
				return err
			}
		}
	} else {
		if !progress.SoftwareInstalled {
			if err := step("replace", actions.replace); err != nil {
				return err
			}
			progress.SoftwareInstalled = true
			if err := save("replace"); err != nil {
				return err
			}
		}
		if err := step("restore", actions.restore); err != nil {
			return err
		}

	}
	if err := step("settled", actions.verify); err != nil {
		return err
	}
	progress.EffectsVerified = true
	return save("settled")
}

const (
	maintenanceTargetOlderThanData = "target_older_than_data"
	maintenanceHostStartFailed     = "host_start_failed"
)

type maintenanceFailure struct {
	code string
	err  error
}

func (failure maintenanceFailure) Error() string { return failure.err.Error() }
func (failure maintenanceFailure) Unwrap() error { return failure.err }

// restoreHost stops observation as soon as the new Host reports schema refusal.
func restoreHost(value paths, id string, repair func(paths) error, ensureVM func(context.Context, paths, string) error) error {
	if err := os.Remove(filepath.Join(value.applicationData, store.StartFailureFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := repair(value); err != nil {
		return err
	}
	if failure := readHostStartFailure(value); failure != nil {
		return hostSchemaFailure(failure)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultCoordinatorInitTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if readHostStartFailure(value) != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := ensureVM(ctx, value, id)
	cancel()
	<-done
	if failure := readHostStartFailure(value); failure != nil {
		return hostSchemaFailure(failure)
	}
	return err
}
func hostSchemaFailure(failure *store.StartFailure) error {
	return maintenanceFailure{code: maintenanceHostStartFailed, err: fmt.Errorf("Host refused its database: %s (data schema %d, current schema %d)", failure.Code, failure.DataSchema, failure.CurrentSchema)}
}

func maintenanceRestoreRequestID(id string, attempt uint64) string {
	hash := sha256.Sum256([]byte(id))
	return fmt.Sprintf("maintenance-restore-%x-%d", hash[:16], attempt)
}
