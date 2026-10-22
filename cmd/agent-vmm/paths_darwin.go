package main

import "path/filepath"

// managedServiceSource names the service manager in managed observations.
const managedServiceSource = "launchd"

func platformDataDirectory(home string) string {
	return filepath.Join(home, "Library", "Application Support", "AgentVMM")
}
func serviceDataDirectory(home string) string { return platformDataDirectory(home) }
func serviceLogDirectory(home string) string {
	return filepath.Join(home, "Library", "Logs", "AgentVMM")
}

// The macOS lifecycle is the agent-vmm-lifecycle helper of the Host bundle.
func defaultLifecycleHelper(executable string) string {
	return filepath.Join(filepath.Dir(executable), "agent-vmm-lifecycle")
}
func lifecycleHelperArguments(arguments []string) []string { return arguments }

// managedPrivilegeIssue reports a lifecycle mutation that the current user
// cannot run. Lifecycle mutations run as the non-root service user.
func managedPrivilegeIssue(command string, euid int) (managedIssue, string, bool) {
	if command != "repair" || euid != 0 {
		return managedIssue{}, "", false
	}
	return managedIssue{Code: "needs_service_user", Message: "repair must run as the configured non-root service user", Layer: "lifecycle", Retryable: managedBool(true)}, "service_user", true
}
