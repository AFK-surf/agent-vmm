//go:build linux

package bootstrap

import (
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestStateFilesystemFormatDisablesDiscard(t *testing.T) {
	want := []string{"-F", "-q", "-O", "project,quota", "-E", "nodiscard,lazy_itable_init=0,lazy_journal_init=0", "/dev/vdb"}
	if got := stateFilesystemFormatArgs("/dev/vdb"); !reflect.DeepEqual(got, want) {
		t.Fatalf("mkfs.ext4 arguments = %q, want %q", got, want)
	}
}

func TestStateFilesystemFormatPreservesFullyAllocatedBackingFile(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("loop-device format regression requires root")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 is unavailable")
	}
	if _, err := exec.LookPath("losetup"); err != nil {
		t.Skip("losetup is unavailable")
	}
	backing := t.TempDir() + "/state.raw"
	file, err := os.OpenFile(backing, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Fallocate(int(file.Fd()), 0, 0, 128<<20); err != nil {
		file.Close()
		t.Skipf("filesystem cannot fully allocate a loop backing file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before := allocatedBlocks(t, backing)
	output, err := exec.Command("losetup", "--find", "--show", backing).CombinedOutput()
	if err != nil {
		t.Skipf("loop devices are unavailable: %v: %s", err, output)
	}
	device := strings.TrimSpace(string(output))
	attached := true
	defer func() {
		if attached {
			_ = exec.Command("losetup", "--detach", device).Run()
		}
	}()
	if output, err = exec.Command("mkfs.ext4", stateFilesystemFormatArgs(device)...).CombinedOutput(); err != nil {
		t.Fatalf("mkfs.ext4: %v: %s", err, output)
	}
	if output, err = exec.Command("losetup", "--detach", device).CombinedOutput(); err != nil {
		t.Fatalf("detach loop: %v: %s", err, output)
	}
	attached = false
	after := allocatedBlocks(t, backing)
	if after < before {
		t.Fatalf("mkfs.ext4 punched holes in preallocated state: before_blocks=%d after_blocks=%d", before, after)
	}
}

func allocatedBlocks(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Sys().(*syscall.Stat_t).Blocks
}
