//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestInitializeVolumeDirectoryAppliesOwnerAndMode(t *testing.T) {
	for _, mode := range []uint32{0o750, 0o1777} {
		path := filepath.Join(t.TempDir(), "state", "volumes", "namespace-a", "volume")
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
		if err := initializeVolumeDirectory(path, mode, &uid, &gid, func(string) error { return nil }); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != os.FileMode(mode&0o777) {
			t.Fatalf("mode = %o", got)
		}
		if (info.Mode()&os.ModeSticky != 0) != (mode&0o1000 != 0) {
			t.Fatalf("sticky bit was lost: %v", info.Mode())
		}
		stat := info.Sys().(*syscall.Stat_t)
		if stat.Uid != uid || stat.Gid != gid {
			t.Fatalf("owner = %d:%d", stat.Uid, stat.Gid)
		}
	}
	if os.Getuid() == 0 {
		root := t.TempDir()
		if err := os.Chmod(filepath.Dir(root), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(root, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "job")
		uid, gid := uint32(0), uint32(0)
		if err := initializeVolumeDirectory(path, 0o1777, &uid, &gid, func(string) error { return nil }); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(path, ".registration")
		if err := os.WriteFile(marker, []byte("protected"), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", `set -e; printf job > "$1/job"; if rm "$1/.registration"; then exit 99; fi`, "sh", path)
		command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000}}
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("job ownership isolation: %v: %s", err, output)
		}
		if _, err := os.Stat(marker); err != nil {
			t.Fatalf("registration marker was deleted: %v", err)
		}
		if info, err := os.Stat(filepath.Join(path, "job")); err != nil || info.Sys().(*syscall.Stat_t).Uid != 1000 {
			t.Fatalf("job user cannot create its own file: %v", err)
		}
	}
	for _, mode := range []uint32{0o2777, 0o4777, 0o7777, 0o10000} {
		if _, err := (&Runtime{}).CreateVolume(context.Background(), "namespace-a", "volume", nil, nil, &mode); err == nil {
			t.Fatalf("accepted privileged mode %o", mode)
		}
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
