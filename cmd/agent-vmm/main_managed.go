//go:build (darwin || linux) && managedonly

package main

import (
	"os"
)

func main() {
	if runPlatformMode() {
		return
	}
	if dispatchApplianceCLI(os.Args[1:]) {
		return
	}
	os.Exit(runManagedCLI(os.Args[1:], os.Stdout, os.Stderr, managedRuntime{}))
}
