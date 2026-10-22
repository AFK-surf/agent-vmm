//go:build linux

package vmmprocess

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestHeldInstanceLockBlocksRunningAndSpawn(t *testing.T) {
	directory := t.TempDir()
	if running, err := Running(directory); err != nil || running {
		t.Fatalf("missing instance: %t, %v", running, err)
	}
	file, err := os.OpenFile(filepath.Join(directory, "vmm.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.WriteString("stale process metadata"); err != nil {
		t.Fatal(err)
	}
	if running, err := Running(directory); err != nil || running {
		t.Fatalf("unheld lock: %t, %v", running, err)
	}
	if err := lockInstance(file); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if running, err := Running(directory); err != nil || !running {
			t.Fatalf("held lock: %t, %v", running, err)
		}
	}
	// An exiting VMM has closed its endpoint but still holds the lock.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := Start(ctx, Config{InstanceDirectory: directory, KernelPath: filepath.Join(directory, "missing-kernel")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("start before retirement = %v", err)
	}
	if err := unix.FcntlFlock(file.Fd(), unix.F_OFD_SETLK, &unix.Flock_t{Type: unix.F_UNLCK}); err != nil {
		t.Fatal(err)
	}
	if running, err := Running(directory); err != nil || running {
		t.Fatalf("released lock: %t, %v", running, err)
	}
}
