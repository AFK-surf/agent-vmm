//go:build linux

package control

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInitializeVolumeDirectoryAppliesOwnerAndMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "volumes", "namespace-a", "volume")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
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
	parent := filepath.Join(t.TempDir(), "state", "volumes", "namespace-a")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
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

func TestVolumeGenerationFenceRejectsConcurrentSameNameRecreation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "volumes", "namespace-a", "volume-a")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	staleGeneration, err := replaceVolumeGenerationID(path)
	if err != nil {
		t.Fatal(err)
	}

	runtime := &Runtime{}
	recreateDone := make(chan struct{})
	recreateResult := make(chan error, 1)
	unlockRecreation := runtime.lockNamespace("namespace-a")
	deleteResult := make(chan error, 1)
	go func() {
		unlock, err := runtime.lockVolumeGeneration("namespace-a", path, staleGeneration)
		if unlock != nil {
			unlock()
		}
		deleteResult <- err
	}()
	go func() {
		defer close(recreateDone)
		_, err := replaceVolumeGenerationID(path)
		recreateResult <- err
		unlockRecreation()
	}()

	select {
	case <-recreateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("same-name recreation did not complete")
	}
	if err := <-recreateResult; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-deleteResult:
		if err == nil {
			t.Fatal("stale delete acquired the recreated volume generation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale delete did not complete")
	}
}
