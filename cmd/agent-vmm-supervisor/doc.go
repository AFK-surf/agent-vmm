//go:build linux

// Command agent-vmm-supervisor is the fixed sandbox init for workloads and appliances.
// VMMD starts the user process separately and retains its execution identity.
package main
