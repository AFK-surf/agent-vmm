package main

import (
	"path/filepath"
)

// managedServiceSource names the service manager in managed observations.
const managedServiceSource = "systemd"

func platformDataDirectory(home string) string {
	return filepath.Join(home, ".local", "state", "agent-vmm")
}

// The managed Linux Host is one system service with a fixed state directory.
func serviceDataDirectory(string) string { return "/var/lib/agent-vmm" }
func serviceLogDirectory(string) string  { return "/var/lib/agent-vmm/logs" }

// The Linux lifecycle is the "agent-vmm lifecycle" command of this executable.
func defaultLifecycleHelper(executable string) string { return executable }
func lifecycleHelperArguments(arguments []string) []string {
	return append([]string{"lifecycle"}, arguments...)
}

// managedPrivilegeIssue reports a lifecycle mutation that the current user
// cannot run. Linux lifecycle mutations change the system service as root.
func managedPrivilegeIssue(command string, euid int) (managedIssue, string, bool) {
	if euid == 0 {
		return managedIssue{}, "", false
	}
	return managedIssue{Code: "needs_administrator", Message: command + " changes the Linux system service; run it as root", Layer: "service", Retryable: managedBool(true)}, "administrator", true
}
