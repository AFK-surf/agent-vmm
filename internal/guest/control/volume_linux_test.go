//go:build linux

package control

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInitializeVolumeDirectoryAppliesOwnerAndMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if err := initializeVolumeDirectory(path, 0o750, &uid, &gid, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o750 {
		t.Fatalf("mode = %o", got)
	}
}

func TestInitializeVolumeDirectoryRollsBack(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "volume")
	want := errors.New("assign project failed")
	if err := initializeVolumeDirectory(path, 0o700, nil, nil, func(string) error { return want }); !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("volume path remained after failure: %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging path remained after failure: %v, %v", entries, err)
	}
}
