//go:build darwin && !development && !e2e

package main

import (
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
)

func newTrustKeyProvider(instanceDirectory string) trustkeys.Provider {
	return trustkeys.NewFileProvider(filepath.Join(instanceDirectory, "device-root.key"))
}
