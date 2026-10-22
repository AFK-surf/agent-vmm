package main

import (
	"log"
	"os"

	"github.com/AFK-surf/agent-vmm/internal/hostapp"
	"github.com/AFK-surf/agent-vmm/internal/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/vmmprocess"
)

var guestManifestDigest string

// runPlatformMode runs the host, VMM, and lifecycle commands that the Linux
// executable links in addition to the client.
func runPlatformMode() bool {
	if len(os.Args) < 2 {
		return false
	}
	switch os.Args[1] {
	case "__vmm":
		if err := vmmprocess.RunChild(); err != nil {
			log.Fatal(err)
		}
		return true
	case "host":
		os.Args = append(os.Args[:1], os.Args[2:]...)
		hostapp.Run(guestManifestDigest)
		return true
	case "lifecycle":
		os.Args = append(os.Args[:1], os.Args[2:]...)
		lifecycle.Main()
		return true
	}
	return false
}
