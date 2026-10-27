package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

type targetSchemaRange struct {
	Current int `json:"currentSchema"`
	Minimum int `json:"minimumSchema"`
}

func targetCurrentSchema(request maintenanceRequest) (targetSchemaRange, error) {
	output, err := exec.Command(filepath.Join(request.SourceApp, "Contents", "Helpers", "agent-vmm-lifecycle"), "version", "--json").Output()
	if err != nil {
		return targetSchemaRange{}, fmt.Errorf("target Host version is unreadable: %w", err)
	}
	var version targetSchemaRange
	if err := json.Unmarshal(output, &version); err != nil {
		return version, errors.New("target Host version is unreadable")
	}
	return version, nil
}

func defaultMaintenanceActions() maintenanceActions {
	return maintenanceActions{
		commitState: writeOwnerState, commitAdmission: writeOwnerState,

		validateSource: func(request maintenanceRequest) error {
			canonical, err := filepath.EvalSymlinks(request.SourceApp)
			if err != nil || canonical != request.SourceApp {
				return errors.New("maintenance source must be a canonical bundle path without symlinks")
			}
			return nil
		},
		targetSchema: targetCurrentSchema,
		dataSchema: func(value paths) (int, error) {
			return store.ReadSchemaVersion(filepath.Join(value.applicationData, "host.sqlite"))
		},
		stop: stopForMaintenance,
		removeSoftware: func(value paths) error {
			for _, path := range []string{value.agentPlist, value.daemonPlist} {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			return os.RemoveAll(value.app)
		},
		resetData: func(value paths) error {
			if err := removeHostData(value); err != nil {
				return err
			}
			return os.RemoveAll(value.logs)
		},
		replace: func(value paths, request maintenanceRequest) error {
			stage := filepath.Join(filepath.Dir(value.app), ".Agent VMM Host.maintenance-"+request.RequestID+".app")
			if err := os.RemoveAll(stage); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(value.app), 0o700); err != nil {
				return err
			}
			if err := exec.Command("/usr/bin/ditto", request.SourceApp, stage).Run(); err != nil {
				return err
			}
			// Validate the final copy before publication.
			id, err := bundle.HostReleaseID(stage)
			if err != nil || id != request.TargetReleaseID {
				return errors.New("prepared replacement does not match selected release")
			}
			if err := exec.Command("/usr/bin/codesign", "--verify", "--deep", "--strict", stage).Run(); err != nil {
				return err
			}
			if err := atomicReplaceApp(stage, value.app); err != nil {
				return err
			}
			return os.RemoveAll(stage)
		},
		restore: func(value paths, id string) error {
			value.resources = filepath.Join(value.app, "Contents", "Resources")
			return restoreHost(value, id, repair, ensureVMStartedContext)
		},
		verify: func(value paths, request maintenanceRequest) error {
			if request.Action == "uninstall" {
				for _, path := range []string{value.app, value.agentPlist, value.daemonPlist} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						return errors.New("Host software removal is not confirmed")
					}
				}
				if request.DataPolicy == "reset" {
					for _, path := range []string{value.logs} {
						if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
							return errors.New("Host data deletion is not confirmed")
						}
					}
				}
				return nil
			}
			facts := readStatus(value)
			if !facts.HostRunning || !facts.HostHealthy {
				return errors.New("installed Host readiness is not confirmed")
			}
			return nil
		},
	}
}

func maintenanceSchemaRefusal(value paths, request maintenanceRequest, actions maintenanceActions) (string, error) {
	data, err := actions.dataSchema(value)
	if err != nil {
		return "", fmt.Errorf("local Host database schema is unreadable: %w", err)
	}
	if data == 0 {
		return "", nil
	}
	target, err := actions.targetSchema(request)
	if err != nil {
		return "", err
	}
	if err := store.CheckSchema(data, target.Minimum, target.Current); err != nil {
		return err.Error(), nil
	}
	return "", nil
}

// refuseMaintenance records a terminal refusal that made no effects. It does
// not change the owner state, so the current Host, an active request it named,
// and a new request stay as they were.
func validateMaintenancePath(home, target string) error {
	relative, err := filepath.Rel(home, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, "../") {
		return errors.New("maintenance target is outside the service home")
	}
	current := home
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("maintenance target contains a symlink")
		}
	}
	return nil
}

// Called inside the outer lifecycle lock. A different retry request ID must
// not turn an old managed installation into a new enrollment.
