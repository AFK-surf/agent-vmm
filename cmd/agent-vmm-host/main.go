//go:build darwin

package main

import "github.com/AFK-surf/agent-vmm/internal/hostapp"

func main() { hostapp.Run(guestManifestDigest) }
