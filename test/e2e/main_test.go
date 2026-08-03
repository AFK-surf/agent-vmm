//go:build darwin && e2e

package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/test/e2e/reporting"
)

func TestMain(m *testing.M) {
	status := m.Run()
	repository, err := filepath.Abs("../..")
	if err == nil {
		bundleDirectory := os.Getenv("AGENT_VMM_E2E_BUNDLE")
		if bundleDirectory == "" {
			bundleDirectory = "/tmp/agent-vmm-e2e-bundle"
		}
		reportPath := filepath.Join(repository, ".context/e2e/report.md")
		if reportErr := reporting.Render(repository, bundleDirectory, "", reportPath, status); reportErr != nil {
			_ = os.WriteFile(reportPath, []byte("# Agent VMM product E2E report\n\nReport metadata unavailable: "+reportErr.Error()+"\n"), 0o644)
		}
	}
	os.Exit(status)
}
