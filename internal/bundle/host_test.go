package bundle

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostReleaseIDReadsInventory(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, HostInventoryRelativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"release_id":"release-test"}`), 0644); err != nil {
		t.Fatal(err)
	}
	if release, err := HostReleaseID(directory); err != nil || release != "release-test" {
		t.Fatalf("release=%q err=%v", release, err)
	}
	if err := os.WriteFile(path, []byte(`{"release_id":"../invalid"}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := HostReleaseID(directory); err == nil {
		t.Fatal("invalid release identity accepted")
	}
}
