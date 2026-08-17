//go:build darwin

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

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
