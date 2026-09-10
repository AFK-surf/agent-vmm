//go:build darwin

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func TestHostOwnerLockFailsClosedForSecondProcessOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host-owner.lock")
	first, err := acquireHostOwnerLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := acquireHostOwnerLock(path); err == nil {
		second.Close()
		t.Fatal("second Host acquired the same VM and database owner lock")
	}
}

func TestInstalledVMResumeTimeoutAllowsBoundedGuestRecovery(t *testing.T) {
	if installedVMResumeTimeout != 10*time.Minute {
		t.Fatalf("installed VM resume timeout = %s, want 10m", installedVMResumeTimeout)
	}
}

func TestNewStateDiskDefaultIs64GiB(t *testing.T) {
	if defaultNewStateBytes != 64<<30 {
		t.Fatalf("new state disk default = %d, want %d", defaultNewStateBytes, int64(64<<30))
	}
}

func TestNewStateDiskRequiresControlAndWorkloadFilesystems(t *testing.T) {
	directory := t.TempDir()
	if _, err := resolveStateDiskBytes(filepath.Join(directory, "state.raw"), minimumNewStateBytes-1); err == nil {
		t.Fatal("undersized new state disk was accepted")
	}
}

func TestStateMigrationRequiresSeparateNewTarget(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "legacy.raw")
	file, err := os.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(minimumNewStateBytes); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	err = runStateMigration(vmm.Config{BundleDirectory: "bundle", InstanceDirectory: directory, SerialLog: filepath.Join(directory, "serial.log")}, source)
	if err == nil || !strings.Contains(err.Error(), "separate instance directory") {
		t.Fatalf("same-directory migration error = %v", err)
	}
}

func TestRuntimeRejectsRoot(t *testing.T) {
	if err := validateRuntimeUID(0); err == nil {
		t.Fatal("root runtime was accepted")
	}
	if err := validateRuntimeUID(502); err != nil {
		t.Fatalf("ordinary user runtime was rejected: %v", err)
	}
}

func TestResumeInstalledVMUsesExistingStateDiskWithoutReformatting(t *testing.T) {
	directory := t.TempDir()
	stateDisk := filepath.Join(directory, "state.raw")
	calls := 0
	start := func(_ context.Context, requestID string) (*hostv1.OperationResponse, error) {
		calls++
		if requestID != "host-start-resume-test" {
			t.Fatalf("unexpected request id %q", requestID)
		}
		return &hostv1.OperationResponse{}, nil
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, "host-start-resume-test", start); err != nil || calls != 0 {
		t.Fatalf("fresh install must not auto-start: calls=%d err=%v", calls, err)
	}
	if err := os.WriteFile(stateDisk, []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, "host-start-resume-test", start); err == nil || calls != 0 {
		t.Fatalf("invalid state must fail closed: calls=%d err=%v", calls, err)
	}
	if err := os.Truncate(stateDisk, 1<<30); err != nil {
		t.Fatal(err)
	}
	initializing := filepath.Join(directory, "state.initializing")
	if err := os.WriteFile(initializing, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, "host-start-resume-incomplete", start); err == nil || calls != 0 {
		t.Fatalf("incomplete initialization must fail closed: calls=%d err=%v", calls, err)
	}
	if err := os.Remove(initializing); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, "host-start-resume-test", start); err != nil || calls != 1 {
		t.Fatalf("installed VM was not resumed once: calls=%d err=%v", calls, err)
	}
	symlink := filepath.Join(directory, "linked-state.raw")
	if err := os.Symlink(stateDisk, symlink); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), symlink, "host-start-resume-symlink", start); err == nil || calls != 1 {
		t.Fatalf("symlink state must fail closed: calls=%d err=%v", calls, err)
	}
	want := errors.New("start failed")
	if err := resumeInstalledVM(context.Background(), stateDisk, "host-start-resume-failure", func(context.Context, string) (*hostv1.OperationResponse, error) {
		return nil, want
	}); !errors.Is(err, want) {
		t.Fatalf("start failure was not reported: %v", err)
	}
}

func TestResolveStateDiskBytesUsesActualExistingLength(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.raw")
	if got, err := resolveStateDiskBytes(path, 64<<30); err != nil || got != 64<<30 {
		t.Fatalf("new state bytes = %d, %v", got, err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	sentinel := []byte("existing-state")
	for _, size := range []int64{32 << 30, 48 << 30, 64 << 30, 96 << 30} {
		if err := file.Truncate(size); err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt(sentinel, 4096); err != nil {
			t.Fatal(err)
		}
		got, err := resolveStateDiskBytes(path, 64<<30)
		if err != nil || got != size {
			t.Fatalf("existing state bytes = %d, %v; want %d", got, err, size)
		}
		contents := make([]byte, len(sentinel))
		if _, err := file.ReadAt(contents, 4096); err != nil || string(contents) != string(sentinel) {
			t.Fatalf("existing state changed at size %d: %q, %v", size, contents, err)
		}
	}
}

func TestDisableCoreDumps(t *testing.T) {
	if err := disableCoreDumps(); err != nil {
		t.Fatal(err)
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &limit); err != nil {
		t.Fatal(err)
	}
	if limit.Cur != 0 || limit.Max != 0 {
		t.Fatalf("core limit = %+v", limit)
	}
}

func TestEnsureConnectorCapabilityIsStablePrivateAndRejectsSymlinks(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "connector.cap")
	first, err := ensureConnectorCapability(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ensureConnectorCapability(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || len(first) < 32 {
		t.Fatal("install-scoped connector capability was not stable")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("capability mode=%v", info.Mode().Perm())
	}
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("not-a-capability"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, "symlink.cap")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureConnectorCapability(symlink); err == nil {
		t.Fatal("symlink capability path was accepted")
	}
}
