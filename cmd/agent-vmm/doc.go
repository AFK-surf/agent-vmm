//go:build darwin || linux

// Command agent-vmm lets the local operator inspect and change the Host through its private API.
// Its mcp subcommand bridges client stdio to the private appliance Unix frontend.
// On Linux it also dispatches the non-root Host and administrator lifecycle service modes.
package main
