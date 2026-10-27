//go:build darwin

package main

import (
	"context"
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/hostapp"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := hostapp.Run(ctx, guestManifestDigest); err != nil {
		slog.Error("Host stopped", "error", err)
		if !errors.Is(err, hostapp.ErrSchemaRejected) {
			os.Exit(1)
		}
	}
}
