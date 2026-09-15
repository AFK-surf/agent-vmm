//go:build darwin && managedonly

package main

import (
	"os"
)

func main() {
	os.Exit(runManagedCLI(os.Args[1:], os.Stdout, os.Stderr, managedRuntime{}))
}
