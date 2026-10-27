package vmm

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartProgressProducer(t *testing.T) {
	if os.Getenv("AGENT_VMM_EMIT_START_PROGRESS") == "1" {
		reportStartProgress(WithStartObservation(context.Background(), "operation-producer", "request-producer"), "wait_guest_health", "started", nil)
		return
	}
	t.Parallel()
	command := exec.Command(os.Args[0], "-test.run=^TestStartProgressProducer$")
	command.Env = append(os.Environ(), "AGENT_VMM_EMIT_START_PROGRESS=1")
	var output bytes.Buffer
	command.Stderr = &output
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "vmm_start={") || !strings.HasSuffix(output.String(), "}\n") {
		t.Fatalf("unexpected producer output: %q", output.String())
	}
	if directory := os.Getenv("AGENT_VMM_STARTUP_FIXTURE_DIRECTORY"); directory != "" {
		if err := os.WriteFile(filepath.Join(directory, "vmm-start.log"), output.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
