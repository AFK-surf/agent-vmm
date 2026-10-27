//go:build linux

// Command agent-vmmd runs the privileged Guest runtime owner on the private Host vsock channel.
// It mediates containerd, quotas, network policy, and storage without exposing their control sockets to workloads.
package main
