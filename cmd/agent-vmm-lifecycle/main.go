//go:build darwin

package main

import (
	"github.com/AFK-surf/agent-vmm/internal/lifecycle"
	"log/slog"
	"os"
)

func main() { slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil))); lifecycle.Main() }
