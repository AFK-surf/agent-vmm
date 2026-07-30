package main

import (
	"flag"
	"log"

	"github.com/AFK-surf/agent-vmm/test/e2e/reporting"
)

func main() {
	repository := flag.String("repository", ".", "repository root")
	bundle := flag.String("bundle", "", "guest bundle directory")
	jsonPath := flag.String("json", "", "go test JSON path")
	report := flag.String("report", ".context/e2e/report.md", "report path")
	status := flag.Int("status", 0, "test exit status")
	flag.Parse()
	if *bundle == "" {
		log.Fatal("bundle is required")
	}
	if err := reporting.Render(*repository, *bundle, *jsonPath, *report, *status); err != nil {
		log.Fatal(err)
	}
}
