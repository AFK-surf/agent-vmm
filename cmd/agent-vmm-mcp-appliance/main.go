package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/mcpappliance"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := mcpappliance.Run(ctx, mcpappliance.Config{RuntimeDirectory: "/run/agent-vmm", StateDirectory: "/var/lib/agent-vmm", Logger: logger}); err != nil {
		logger.Error("bundled MCP appliance stopped", "error", err)
		os.Exit(1)
	}
}
