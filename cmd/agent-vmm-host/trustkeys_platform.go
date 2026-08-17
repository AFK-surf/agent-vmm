//go:build darwin && !development && !e2e

package main

import (
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
)

func newTrustKeyProvider(string) trustkeys.Provider { return trustkeys.NewPlatformProvider() }
