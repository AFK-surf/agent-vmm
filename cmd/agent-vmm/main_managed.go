//go:build (darwin || linux) && managedonly

package main

import (
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if len(os.Args) > 1 && os.Args[1] == "mcp" {
		os.Exit(runMCP(os.Args[2:]))
	}
	if runPlatformMode() {
		return
	}
	if dispatchApplianceCLI(os.Args[1:]) {
		return
	}
	os.Exit(runManagedCLI(os.Args[1:], os.Stdout, os.Stderr, managedRuntime{}))
}
