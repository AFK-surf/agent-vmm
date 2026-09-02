//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContainerdLauncherSetsMaximumKillableOOMScoreBeforeCapabilityDrop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oom_score_adj")
	if err := setContainerdOOMScore(path); err != nil {
		t.Fatal(err)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "1000" {
		t.Fatalf("oom_score_adj = %q, want 1000", value)
	}
}
