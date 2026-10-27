package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
			slog.Error("VMM stopped", "error", err)
			os.Exit(1)
		}
		return true
	case "host":
		os.Args = append(os.Args[:1], os.Args[2:]...)
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := hostapp.Run(ctx, guestManifestDigest); err != nil {
			slog.Error("Host stopped", "error", err)
			if !errors.Is(err, hostapp.ErrSchemaRejected) {
				os.Exit(1)
			}
		}
		return true
	case "lifecycle":
		os.Args = append(os.Args[:1], os.Args[2:]...)
		lifecycle.Main()
		return true
	}
	return false
}
