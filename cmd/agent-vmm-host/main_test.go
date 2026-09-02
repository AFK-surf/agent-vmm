//go:build darwin

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func TestInstalledVMResumeTimeoutAllowsBoundedGuestRecovery(t *testing.T) {
	if installedVMResumeTimeout != 10*time.Minute {
		t.Fatalf("installed VM resume timeout = %s, want 10m", installedVMResumeTimeout)
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

func TestResumeInstalledVMIsBoundedByExactStateDisk(t *testing.T) {
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
	if err := resumeInstalledVM(context.Background(), stateDisk, 1<<30, "host-start-resume-test", start); err != nil || calls != 0 {
		t.Fatalf("fresh install must not auto-start: calls=%d err=%v", calls, err)
	}
	if err := os.WriteFile(stateDisk, []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, 1<<30, "host-start-resume-test", start); err == nil || calls != 0 {
		t.Fatalf("invalid state must fail closed: calls=%d err=%v", calls, err)
	}
	if err := os.Truncate(stateDisk, 1<<30); err != nil {
		t.Fatal(err)
	}
	initializing := filepath.Join(directory, "state.initializing")
	if err := os.WriteFile(initializing, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, 1<<30, "host-start-resume-incomplete", start); err == nil || calls != 0 {
		t.Fatalf("incomplete initialization must fail closed: calls=%d err=%v", calls, err)
	}
	if err := os.Remove(initializing); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), stateDisk, 1<<30, "host-start-resume-test", start); err != nil || calls != 1 {
		t.Fatalf("installed VM was not resumed once: calls=%d err=%v", calls, err)
	}
	symlink := filepath.Join(directory, "linked-state.raw")
	if err := os.Symlink(stateDisk, symlink); err != nil {
		t.Fatal(err)
	}
	if err := resumeInstalledVM(context.Background(), symlink, 1<<30, "host-start-resume-symlink", start); err == nil || calls != 1 {
		t.Fatalf("symlink state must fail closed: calls=%d err=%v", calls, err)
	}
	want := errors.New("start failed")
	if err := resumeInstalledVM(context.Background(), stateDisk, 1<<30, "host-start-resume-failure", func(context.Context, string) (*hostv1.OperationResponse, error) {
		return nil, want
	}); !errors.Is(err, want) {
		t.Fatalf("start failure was not reported: %v", err)
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
