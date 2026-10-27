package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/AFK-surf/agent-vmm/test/e2e/reporting"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	repository := flag.String("repository", ".", "repository root")
	bundle := flag.String("bundle", "", "guest bundle directory")
	jsonPath := flag.String("json", "", "go test JSON path")
	report := flag.String("report", ".build/test-results/e2e/report.md", "report path")
	status := flag.Int("status", 0, "test exit status")
	flag.Parse()
	if *bundle == "" {
		slog.Error("bundle is required")
		os.Exit(1)
	}
	if err := reporting.Render(*repository, *bundle, *jsonPath, *report, *status); err != nil {
		slog.Error("render E2E report", "error", err)
		os.Exit(1)
	}
}
