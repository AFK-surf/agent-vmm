//go:build darwin

package vmm

import (
	"context"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
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

func TestConfigurationHasNoNetworkDevice(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"kernel", "initramfs", "rootfs", "state.raw"} {
		if err := os.WriteFile(filepath.Join(directory, name), make([]byte, 4096), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	serial, err := os.OpenFile(filepath.Join(directory, "serial.log"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	manager := &Manager{config: Config{CPUCount: 2, MemoryBytes: 1 << 30, InstanceDirectory: directory}}
	verified := &bundle.Verified{Directory: directory, Manifest: bundle.Manifest{Artifacts: map[string]bundle.Artifact{
		"kernel": {Path: "kernel"}, "initramfs": {Path: "initramfs"}, "rootfs": {Path: "rootfs"},
	}}}
	configuration, err := manager.configuration(verified, serial, false)
	if err != nil {
		t.Fatal(err)
	}
	if devices := configuration.NetworkDevices(); len(devices) != 0 {
		t.Fatalf("configuration has %d network devices", len(devices))
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
