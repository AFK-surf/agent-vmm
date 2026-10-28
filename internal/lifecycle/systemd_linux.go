package lifecycle

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"golang.org/x/sys/unix"
)

//go:embed agent-vmm-host.service
var unitTemplate string

var runCommand = func(ctx context.Context, name string, arguments ...string) error {
	_, err := commandOutput(ctx, name, arguments...)
	return err
}

var commandOutput = func(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, name, arguments...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", name, strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func checkMutationUser(euid int) error {
	if euid != 0 {
		return errors.New("Linux lifecycle mutations install or change the system service; run them as root with sudo")
	}
	return nil
}

// Linux has no shared-Host maintenance journal or publication command.
func executePlatformCommand(string, serviceRequest, []string) bool { return false }

func checkMaintenanceInstallTarget(paths, string) error { return nil }

func (value paths) hostJob() string { return linuxUnitName }

func (value paths) hostInstalled() bool {
	return regular(filepath.Join(value.app, "agent-vmm")) && regular(value.unit)
}

func renderUnit(value paths) ([]byte, error) {
	parsed, err := template.New(linuxUnitName).Parse(unitTemplate)
	if err != nil {
		return nil, err
	}
	var rendered bytes.Buffer
	err = parsed.Execute(&rendered, map[string]string{
		"User": value.service.User, "Group": value.service.Group,
		"Current": value.app, "State": value.applicationData, "ServiceLogs": value.serviceLogs,
	})
	return rendered.Bytes(), err
}

func observeHostService(ctx context.Context, value paths) (hostServiceFacts, error) {
	output, err := commandOutput(ctx, "systemctl", "show", value.hostJob(), "--property=LoadState,ActiveState,MainPID,ExecMainCode,ExecMainStatus")
	if err != nil {
		return hostServiceFacts{}, err
	}
	return parseSystemdServiceObservation(output)
}

func parseSystemdServiceObservation(output []byte) (hostServiceFacts, error) {
	properties := map[string]string{}
	for _, line := range strings.Split(string(output), "\n") {
		if key, raw, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			properties[key] = raw
		}
	}
	if properties["LoadState"] == "not-found" {
		return hostServiceFacts{}, errHostServiceNotFound
	}
	pid, err := strconv.Atoi(properties["MainPID"])
	if properties["LoadState"] != "loaded" || err != nil || pid < 0 {
		return hostServiceFacts{}, errors.New("systemctl show reported an invalid Host service state")
	}
	var result hostServiceFacts
	switch properties["ActiveState"] {
	case "active", "activating", "deactivating", "reloading":
		result.Loaded = true
	}
	result.PID = pid
	result.Running = pid > 0
	if status, err := strconv.Atoi(properties["ExecMainStatus"]); err == nil && properties["ExecMainCode"] != "0" {
		result.LastExitStatus = &status
	}
	return result, nil
}

// reconcileService publishes the source release, makes it current, and
// starts the unit. The system service keeps the fixed daemon policy, so
// personal mesh pairing stays disabled.
func reconcileService(ctx context.Context, value paths, _ bool) error {
	contentID, err := installRelease(value, value.source)
	if err != nil {
		return err
	}
	desired, err := renderUnit(value)
	if err != nil {
		return err
	}
	installed, _ := os.ReadFile(value.unit)
	facts, observeErr := observeHostService(ctx, value)
	if observeErr != nil && !errors.Is(observeErr, errHostServiceNotFound) {
		return fmt.Errorf("host service status could not be read: %w", observeErr)
	}
	if bytes.Equal(installed, desired) && currentContentID(value) == contentID && facts.Running {
		return persistServiceConfig(value, true)
	}
	releaseChanged := currentContentID(value) != contentID
	if releaseChanged && facts.Loaded {
		if err := drainService(ctx, value); err != nil {
			return err
		}
	} else if facts.Loaded {
		if err := runCommand(ctx, "systemctl", "stop", value.hostJob()); err != nil {
			return fmt.Errorf("host service could not be stopped before reload: %w", err)
		}
	}
	if err := switchCurrent(value, contentID); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(value.unit), 0o755); err != nil {
		return err
	}
	if err := atomicWrite(value.unit, desired, 0o644); err != nil {
		return errors.New("service definition could not be installed")
	}
	for _, arguments := range [][]string{{"daemon-reload"}, {"enable", value.hostJob()}, {"start", value.hostJob()}} {
		if err := runCommand(ctx, "systemctl", arguments...); err != nil {
			return fmt.Errorf("host service could not be started: %w", err)
		}
	}
	if err := persistServiceConfig(value, true); err != nil {
		return errors.New("service configuration could not be persisted")
	}
	return nil
}

// drainService stops the VM through the Host, then stops the unit and waits
// for the VMM child to release its instance lock.
func drainService(ctx context.Context, value paths) error {
	facts, err := observeHostService(ctx, value)
	if err != nil && !errors.Is(err, errHostServiceNotFound) {
		return fmt.Errorf("host service status could not be read before drain: %w", err)
	}
	if facts.Running {
		if err := value.serviceOwner().drainVM(ctx, value, newOperationID("update-drain")); err != nil {
			return err
		}
	}
	if facts.Loaded {
		if err := runCommand(ctx, "systemctl", "stop", value.hostJob()); err != nil {
			return fmt.Errorf("host service could not be drained: %w", err)
		}
	}
	return waitForVMMExit(ctx, value)
}

func removeService(ctx context.Context, value paths, purge bool) error {
	if err := drainService(ctx, value); err != nil {
		return err
	}
	_, observeErr := observeHostService(ctx, value)
	if observeErr != nil && !errors.Is(observeErr, errHostServiceNotFound) {
		return fmt.Errorf("host service status could not be read before uninstall: %w", observeErr)
	}
	if observeErr == nil {
		if err := runCommand(ctx, "systemctl", "disable", "--now", value.hostJob()); err != nil {
			return fmt.Errorf("host service could not be stopped before uninstall: %w", err)
		}
	}
	if err := os.Remove(value.unit); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("host service definition could not be removed")
	}
	if err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	_ = os.Remove(value.serviceConfigFile)
	if purge {
		if err := removeHostData(value); err != nil {
			return errors.New("application data could not be removed")
		}
	}
	if err := os.RemoveAll(value.installRoot); err != nil {
		return errors.New("installed releases could not be removed")
	}
	return ctx.Err()
}

// waitForVMMExit uses the VMM instance lock as the authority for a running
// VM, because the VMM child outlives the Host process.
func waitForVMMExit(ctx context.Context, value paths) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		running, err := vmmRunning(value)
		if err != nil {
			return fmt.Errorf("VMM instance lock could not be read: %w", err)
		}
		if !running {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("the VMM process still holds the instance lock; stop it before retrying: %w", waitCtx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func vmmRunning(value paths) (bool, error) {
	file, err := os.Open(filepath.Join(value.applicationData, "instance", "vmm.lock"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	lock := unix.Flock_t{Type: unix.F_WRLCK}
	if err := unix.FcntlFlock(file.Fd(), unix.F_OFD_GETLK, &lock); err != nil {
		return false, err
	}
	return lock.Type != unix.F_UNLCK, nil
}

func validatePreparedUpdate(value paths, sourceApp string) error {
	if filepath.Dir(sourceApp) != value.installRoot || !strings.HasPrefix(filepath.Base(sourceApp), ".") {
		return errors.New("prepared update must be a hidden directory in " + value.installRoot)
	}
	return nil
}

// updateApplication installs the prepared release and switches the current
// link. The state directory is never part of the release.
func updateApplication(value paths, sourceApp, targetReleaseID, requestID string) error {
	return updateApplicationWith(value, sourceApp, targetReleaseID, requestID, linuxUpdateActions(value, targetReleaseID))
}

func linuxUpdateActions(value paths, targetReleaseID string) updateApplicationActions {
	current := value
	current.source = value.app
	return updateApplicationActions{
		releaseID: func(directory string) (string, error) { return ReleaseID(directory) },
		drain: func(value paths) error {
			return withHostStopTimeout(func(ctx context.Context) error { return drainService(ctx, value) })
		},
		replace: func(source, _ string) error {
			contentID, err := installRelease(value, source)
			if err != nil {
				return err
			}
			if release, err := ReleaseID(filepath.Join(value.installRoot, contentID)); err != nil || release != targetReleaseID {
				return errors.New("copied Host release does not match the selected target")
			}
			return switchCurrent(value, contentID)
		},
		repair: func(paths) error {
			return withHostStopTimeout(func(ctx context.Context) error { return reconcileService(ctx, current, true) })
		},
		startVM: ensureVMStartedContext,
		remove: func(source string) error {
			if err := os.RemoveAll(source); err != nil {
				return err
			}
			return pruneReleases(value)
		},
	}
}
