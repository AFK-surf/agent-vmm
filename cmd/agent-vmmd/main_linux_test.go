//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContainerdLauncherSetsMinimumOOMScoreBeforeCapabilityDrop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oom_score_adj")
	if err := setContainerdOOMScore(path); err != nil {
		t.Fatal(err)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "-1000" {
		t.Fatalf("oom_score_adj = %q, want -1000", value)
	}
}

func TestContainerdConfigPreservesInheritedOOMScore(t *testing.T) {
	contents, err := os.ReadFile("../../build/guest/root/etc/containerd/config.toml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "\noom_score =") {
		t.Fatalf("containerd config must preserve the launcher-inherited OOM score:\n%s", contents)
	}
}
