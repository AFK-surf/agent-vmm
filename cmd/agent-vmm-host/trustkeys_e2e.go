//go:build darwin && (development || e2e)

package main

import (
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
)

// This provider choice is compile-time only. Release hosts have no runtime
// switch to the development device identity.
func newTrustKeyProvider(instanceDirectory string) trustkeys.Provider {
	return trustkeys.NewFileProvider(filepath.Join(instanceDirectory, "device-root.key"))
}
