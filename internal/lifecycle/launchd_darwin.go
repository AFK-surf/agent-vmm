package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

func reconcileService(ctx context.Context, value paths, disablePairing bool) error {
	return repairWithOptionsContext(ctx, value, launchctlContext, disablePairing)
}

func drainService(ctx context.Context, value paths) error {
	return drainWithContext(ctx, value, launchctlContext)
}

func removeService(ctx context.Context, value paths, purge bool) error {
	return uninstallWithContext(ctx, value, purge, launchctlContext)
}

func checkMutationUser(euid int) error {
	if euid == 0 {
		return errors.New("lifecycle business mutations must run as the configured non-root service user; use agent-vmm-service-executor only for fixed system jobs")
	}
	return nil
}

func executePlatformCommand(command string, request serviceRequest, args []string) bool {
	switch command {
	case "maintenance", "maintenance-status":
		executeMaintenanceCommand(command, request, args)
	case "publish-host":
		executePublishHost(request, args)
	default:
		return false
	}
	return true
}

func repair(value paths) error { return repairWith(value, launchctl) }

func repairWithPolicy(value paths, disablePairing bool) error {
	return repairWithOptions(value, launchctl, disablePairing)
}

func repairWith(value paths, run func(...string) error) error {
	return repairWithOptions(value, run, false)
}

func repairWithOptions(value paths, run func(...string) error, disablePairing bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return repairWithOptionsContext(ctx, value, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	}, disablePairing)
}

func repairWithOptionsContext(ctx context.Context, value paths, run func(context.Context, ...string) error, disablePairing bool) error {
	return repairWithOptionsContextObserved(ctx, value, run, disablePairing, os.Geteuid(), launchctlServiceObservation)
}

func repairWithOptionsContextObserved(ctx context.Context, value paths, run func(context.Context, ...string) error, disablePairing bool, euid int,
	observe func(context.Context, string) (hostServiceFacts, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Omission preserves the administrator-owned restrictive policy across a
	// bundle replacement. An explicit disable request can only tighten it.
	disablePairing = disablePairing || value.service.DisablePairing || value.service.Mode == serviceModeDaemon
	if !directory(value.app) {
		return errors.New("Agent VMM application bundle is missing")
	}
	if err := validateAlternateDefinitionPermission(value, euid); err != nil {
		return err
	}
	if value.service.Mode == serviceModeDaemon && euid != 0 {
		desired, renderErr := renderServiceDefinition(value, disablePairing)
		if renderErr != nil || !serviceDefinitionCurrent(ctx, value, desired, disablePairing, run, observe) {
			return errors.New("installing or updating the Agent VMM daemon requires administrator access")
		}
		if err := prepareServiceStateDirectories(value, euid); err != nil {
			return err
		}
		if err := persistServiceConfig(value, disablePairing); err != nil {
			return errors.New("service configuration could not be persisted")
		}
		return nil
	}
	definitionMode := os.FileMode(0o700)
	if value.service.Mode == serviceModeDaemon {
		definitionMode = 0o755
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), definitionMode); err != nil {
		return errors.New("launchd service directory could not be created")
	}
	if err := prepareServiceStateDirectories(value, euid); err != nil {
		return err
	}
	desired, err := renderServiceDefinition(value, disablePairing)
	if err != nil {
		return err
	}
	if serviceDefinitionCurrent(ctx, value, desired, disablePairing, run, observe) {
		return nil
	}
	if err := stopHostServiceAt(ctx, run, value.serviceDomain()); err != nil {
		return fmt.Errorf("host service could not be stopped before reload: %w", err)
	}
	if err := stopHostServiceAt(ctx, run, value.alternateServiceDomain()); err != nil {
		return fmt.Errorf("alternate host service could not be stopped before reload: %w", err)
	}
	if err := atomicWrite(value.hostPlist, desired, map[serviceMode]os.FileMode{serviceModeAgent: 0o600, serviceModeDaemon: 0o644}[value.service.Mode]); err != nil {
		return errors.New("service definition could not be installed")
	}
	if value.service.Mode == serviceModeDaemon {
		_ = os.Remove(value.agentPlist)
	} else if os.Geteuid() == 0 {
		_ = os.Remove(value.daemonPlist)
	}
	if err := run(ctx, "bootstrap", value.serviceDomain(), value.hostPlist); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("host service load was interrupted: %w", contextErr)
		}
		return fmt.Errorf("host service could not be loaded: %w", err)
	}
	if err := run(ctx, "enable", value.serviceDomain()+"/"+hostLabel); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("host service enable was interrupted: %w", contextErr)
		}
		return fmt.Errorf("host service could not be enabled: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := persistServiceConfig(value, disablePairing); err != nil {
		return errors.New("service configuration could not be persisted")
	}
	return nil
}

func prepareServiceStateDirectories(value paths, euid int) error {
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		return errors.New("application data directory could not be created")
	}
	if err := os.MkdirAll(value.logs, 0o700); err != nil {
		return errors.New("log directory could not be created")
	}
	if euid == 0 {
		for _, directory := range []string{value.applicationData, value.logs} {
			if err := os.Chown(directory, value.service.UID, value.service.GID); err != nil {
				return errors.New("service user directory ownership could not be applied")
			}
		}
	}
	return nil
}

func addProgramArgument(plist, argument string) string {
	marker := "</array>"
	index := strings.Index(plist, marker)
	if index < 0 || strings.Contains(plist, "<string>"+argument+"</string>") {
		return plist
	}
	return plist[:index] + "\t\t<string>" + xmlEscape(argument) + "</string>\n\t" + plist[index:]
}

func uninstall(value paths, purge bool) error { return uninstallWith(value, purge, launchctl) }

func drain(value paths) error { return drainWith(value, launchctl) }

// launchctl bootout waits for the host and its supervised workers to terminate.
func drainWith(value paths, run func(...string) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return drainWithContext(ctx, value, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	})
}

func drainWithContext(ctx context.Context, value paths, run func(context.Context, ...string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.service.Mode == serviceModeDaemon && os.Geteuid() != 0 {
		err := run(ctx, "print", value.serviceDomain()+"/"+hostLabel)
		if errors.Is(err, errHostServiceNotFound) {
			return waitForStateDiskRelease(ctx, filepath.Join(value.applicationData, "instance", "state.raw"))
		}
		if err != nil {
			return fmt.Errorf("host service status could not be read before drain: %w", err)
		}
		if err := drainVMForUpdate(ctx, value, newOperationID("service-drain")); err != nil {
			return err
		}
		return errors.New("VM is stopped; administrator must run agent-vmm-service-executor stop --job host")
	}
	domains := []string{value.serviceDomain(), value.alternateServiceDomain()}
	loadedDomains := make(map[string]bool, len(domains))
	for _, domain := range domains {
		err := run(ctx, "print", domain+"/"+hostLabel)
		if err == nil {
			loadedDomains[domain] = true
			continue
		}
		if !errors.Is(err, errHostServiceNotFound) {
			return fmt.Errorf("host service status could not be read before drain: %w", err)
		}
	}
	pids := make([]int, 0, len(domains))
	for _, domain := range domains {
		if !loadedDomains[domain] {
			continue
		}
		pid, err := launchctlPID(ctx, domain+"/"+hostLabel)
		if err == nil && pid > 0 {
			pids = append(pids, pid)
		} else if err != nil {
			return fmt.Errorf("host process identity could not be read before drain: %w", err)
		}
	}
	needsVMStop := len(pids) > 0
	if !needsVMStop {
		owners, err := stateDiskOpenOwners(ctx, filepath.Join(value.applicationData, "instance", "state.raw"))
		if err != nil {
			return err
		}
		needsVMStop = len(owners) > 0
	}
	if needsVMStop {
		if err := drainVMForUpdate(ctx, value, newOperationID("update-drain")); err != nil {
			if len(pids) > 0 {
				return err
			}
		}
	}
	for _, domain := range domains {
		if err := stopHostServiceAt(ctx, run, domain); err != nil {
			return fmt.Errorf("host service could not be drained: %w", err)
		}
	}
	for _, pid := range pids {
		if err := waitForProcessExit(ctx, pid); err != nil {
			return fmt.Errorf("host process did not exit after launchd removed the service: %w", err)
		}
	}
	stateDisk := filepath.Join(value.applicationData, "instance", "state.raw")
	if err := waitForStateDiskRelease(ctx, stateDisk); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// A loaded job without a live PID has no Host process to wait for, whatever
// launchd state it reports, for example "spawn scheduled" in a crash loop.
var launchctlPID = func(ctx context.Context, target string) (int, error) {
	service, err := launchctlServiceObserver(ctx, target)
	return service.PID, err
}
var processRunning = func(pid int) bool {
	err := unix.Kill(pid, 0)
	return err == nil || errors.Is(err, unix.EPERM)
}

var stateDiskOpenOwners = fuserOpenOwners

func fuserOpenOwners(ctx context.Context, path string) ([]int, error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read VM state disk: %w", err)
	}
	command := exec.CommandContext(ctx, "/usr/bin/fuser", path)
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("inspect VM state disk owner: %w", err)
	}
	return parseFuserOwners(output), nil
}

func parseFuserOwners(output []byte) []int {
	owners := make([]int, 0)
	for _, field := range strings.Fields(string(output)) {
		pid, parseErr := strconv.Atoi(field)
		if parseErr == nil && pid > 0 {
			owners = append(owners, pid)
		}
	}
	return owners
}

func waitForStateDiskRelease(ctx context.Context, path string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		owners, err := stateDiskOpenOwners(waitCtx, path)
		if err != nil {
			return fmt.Errorf("VM state disk ownership could not be verified: %w", err)
		}
		if len(owners) == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("VM state disk is still open by process IDs %v; stop those exact owners before retrying the update: %w", owners, waitCtx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitForProcessExit(ctx context.Context, pid int) error {
	for processRunning(pid) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return nil
}

func uninstallWith(value paths, purge bool, run func(...string) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), hostStopTimeout)
	defer cancel()
	return uninstallWithContext(ctx, value, purge, func(_ context.Context, arguments ...string) error {
		return run(arguments...)
	})
}

func uninstallWithContext(ctx context.Context, value paths, purge bool, run func(context.Context, ...string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.service.Mode == serviceModeDaemon && os.Geteuid() != 0 {
		return errors.New("uninstalling the Agent VMM daemon requires administrator access")
	}
	if err := stopHostServiceAt(ctx, run, value.serviceDomain()); err != nil {
		return fmt.Errorf("host service could not be stopped before uninstall: %w", err)
	}
	if err := os.Remove(value.hostPlist); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("host service definition could not be removed")
	}
	_ = os.Remove(value.serviceConfigFile)
	if purge {
		if err := os.RemoveAll(value.applicationData); err != nil {
			return errors.New("application data could not be removed")
		}
		if err := os.RemoveAll(value.logs); err != nil {
			return errors.New("logs could not be removed")
		}
	}
	if err := os.RemoveAll(value.app); err != nil {
		return errors.New("application bundle could not be removed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func launchctl(arguments ...string) error {
	return launchctlContext(context.Background(), arguments...)
}

func launchctlContext(ctx context.Context, arguments ...string) error {
	command := exec.CommandContext(ctx, "/bin/launchctl", arguments...)
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	message := strings.TrimSpace(string(output))
	if contextErr := ctx.Err(); contextErr != nil {
		return fmt.Errorf("launchctl %s was interrupted: %w", arguments[0], contextErr)
	}
	if launchctlServiceAbsent(arguments[0], message) {
		return fmt.Errorf("%w: %s", errHostServiceNotFound, message)
	}
	if message == "" {
		return fmt.Errorf("launchctl %s failed: %w", arguments[0], err)
	}
	return fmt.Errorf("launchctl %s failed: %w: %s", arguments[0], err, message)
}

func launchctlServiceAbsent(action, message string) bool {
	return action == "print" && (strings.Contains(message, "Could not find service") ||
		strings.Contains(message, "Could not find domain") ||
		strings.Contains(message, "Domain does not support specified action"))
}

func stopHostService(ctx context.Context, run func(context.Context, ...string) error) error {
	return stopHostServiceAt(ctx, run, "")
}

func stopHostServiceAt(ctx context.Context, run func(context.Context, ...string) error, serviceDomain string) error {
	if serviceDomain == "" {
		serviceDomain = fmt.Sprintf("gui/%d", os.Getuid())
	}
	target := serviceDomain + "/" + hostLabel
	bootoutErr := run(ctx, "bootout", target)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := run(ctx, "print", target); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if errors.Is(err, errHostServiceNotFound) {
			return nil
		}
		return fmt.Errorf("host service status could not be read: %w", err)
	}
	if bootoutErr != nil {
		return fmt.Errorf("launchctl bootout failed while the host service remains loaded: %w", bootoutErr)
	}
	return waitForHostServiceRemovalAt(ctx, run, serviceDomain)
}

// waitForHostServiceRemovalAt waits within the caller deadline. The Host
// shutdown after SIGTERM stops the VM and the data worker first.
func waitForHostServiceRemovalAt(ctx context.Context, run func(context.Context, ...string) error, serviceDomain string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("host service did not finish stopping: %w", err)
		}
		if err := run(ctx, "print", serviceDomain+"/"+hostLabel); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("host service did not finish stopping: %w", ctx.Err())
			}
			if errors.Is(err, errHostServiceNotFound) {
				return nil
			}
			return fmt.Errorf("host service status could not be read: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("host service did not finish stopping: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func xmlEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}
