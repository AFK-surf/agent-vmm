package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/mcpserver"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "agent-vmm-mcp: operation failed")
		os.Exit(1)
	}
}

func run() error {
	defaultSocket, err := mcpserver.DefaultOperatorSocket()
	if err != nil {
		return err
	}
	var operatorSocket, environmentID, displayName string
	flag.StringVar(&operatorSocket, "operator-socket", defaultSocket, "absolute Agent VMM operator socket")
	flag.StringVar(&environmentID, "environment", "", "local environment ID")
	flag.StringVar(&displayName, "display-name", "", "local environment display name")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	bootstrapContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	session, err := mcpserver.Bootstrap(bootstrapContext, operatorSocket, environmentID, displayName)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		_ = session.Close(shutdownContext)
	}()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return mcpserver.New(session.Runtime, logger).Run(ctx, &mcp.StdioTransport{})
}
