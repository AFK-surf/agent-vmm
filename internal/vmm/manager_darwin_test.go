//go:build darwin

package vmm

import (
	"context"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
)

func TestStateInitializationMarker(t *testing.T) {
	directory := t.TempDir()
	const size = int64(1 << 20)

	initializing, err := prepareState(directory, size)
	if err != nil || !initializing {
		t.Fatalf("prepare new state = %t, %v", initializing, err)
	}
	info, err := os.Stat(filepath.Join(directory, "state.raw"))
	if err != nil || info.Size() != size {
		t.Fatalf("state disk = %v, %v", info, err)
	}
	initializing, err = prepareState(directory, size)
	if err != nil || !initializing {
		t.Fatalf("prepare interrupted state = %t, %v", initializing, err)
	}
	if err := finishStateInitialization(directory); err != nil {
		t.Fatal(err)
	}
	initializing, err = prepareState(directory, size)
	if err != nil || initializing {
		t.Fatalf("prepare initialized state = %t, %v", initializing, err)
	}
}

func TestBundleFailurePrecedesInstanceCreation(t *testing.T) {
	root := t.TempDir()
	instance := filepath.Join(root, "instance")
	manager, err := NewManager(Config{
		BundleDirectory: filepath.Join(root, "invalid-bundle"), InstanceDirectory: instance,
		CPUCount: 2, MemoryBytes: 1 << 30, StateBytes: 1 << 30,
		SerialLog: filepath.Join(root, "serial.log"), TrustRoot: make(ed25519.PublicKey, ed25519.PublicKeySize),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with invalid bundle")
	}
	if _, err := os.Stat(instance); !os.IsNotExist(err) {
		t.Fatalf("instance created before verification: %v", err)
	}
}
