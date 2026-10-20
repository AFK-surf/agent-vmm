//go:build darwin && managedonly

package main

import (
	"os"
)

func main() {
	if dispatchApplianceCLI(os.Args[1:]) {
		return
	}
	os.Exit(runManagedCLI(os.Args[1:], os.Stdout, os.Stderr, managedRuntime{}))
}
