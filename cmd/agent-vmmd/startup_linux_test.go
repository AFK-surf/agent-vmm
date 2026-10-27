//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGuestStartupWritesParseableMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "guest-startup.log")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = output
	defer func() { os.Stderr = original }()
	reportGuestStartup(startupReference{OperationID: "operation-producer"}, "containerd", "failed", errors.New("producer failure"))
	os.Stderr = original
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	line := string(data)
	if !strings.HasPrefix(line, "guest_startup=") || strings.Count(line, "\n") != 1 {
		t.Fatalf("startup marker = %q", line)
	}
	var record guestStartupProgress
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "guest_startup=")), &record); err != nil {
		t.Fatal(err)
	}
	if record.OperationID != "operation-producer" || record.Stage != "containerd" || record.State != "failed" || record.Error != "producer failure" {
		t.Fatalf("startup record = %+v", record)
	}
	if directory := os.Getenv("AGENT_VMM_STARTUP_FIXTURE_DIRECTORY"); directory != "" {
		if err := os.WriteFile(filepath.Join(directory, "guest-startup.log"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
