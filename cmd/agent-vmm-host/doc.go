//go:build darwin

// Command agent-vmm-host runs the non-root macOS Host service and owns its private listeners.
// Launchd starts it from the installed Host application.
package main
