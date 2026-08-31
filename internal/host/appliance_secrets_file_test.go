//go:build darwin

package host

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFileApplianceSecretsPersistAndDelete(t *testing.T) {
	store, err := newApplianceSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.EnsureVolumeKey(context.Background(), "installation-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.EnsureVolumeKey(context.Background(), "installation-a")
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("volume key did not persist: %v", err)
	}
	replacement := bytes.Repeat([]byte{0x5a}, 32)
	if err := store.ReplaceVolumeKey(context.Background(), "installation-a", replacement); err != nil {
		t.Fatal(err)
	}
	second, err = store.EnsureVolumeKey(context.Background(), "installation-a")
	if err != nil || !bytes.Equal(second, replacement) {
		t.Fatalf("replacement volume key did not persist: %v", err)
	}
	seed := []byte("bootstrap-seed")
	if err := store.StoreBootstrap(context.Background(), "installation-a", "bootstrap-a", seed); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadBootstrap(context.Background(), "installation-a", "bootstrap-a")
	if err != nil || !bytes.Equal(loaded, seed) {
		t.Fatalf("bootstrap seed = %q, %v", loaded, err)
	}
	if err := store.DeleteBootstrap(context.Background(), "installation-a", "bootstrap-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadBootstrap(context.Background(), "installation-a", "bootstrap-a"); err == nil {
		t.Fatal("deleted bootstrap seed remained available")
	}
	keyPath := filepath.Join(store.(*fileApplianceSecrets).directory, applianceSecretID("installation-a")+"-volume.key")
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("volume key mode = %v", info.Mode().Perm())
	}
}

func TestFileApplianceSecretsDoNotReplaceUnreadableVolumeKey(t *testing.T) {
	store, err := newApplianceSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureVolumeKey(context.Background(), "installation-a"); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(store.(*fileApplianceSecrets).directory, applianceSecretID("installation-a")+"-volume.key")
	if err := os.Chmod(keyPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureVolumeKey(context.Background(), "installation-a"); err == nil {
		t.Fatal("insecure volume key mode was accepted")
	}
	info, err := os.Stat(keyPath)
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("invalid key was silently replaced: mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestFileApplianceSecretsDoNotReplaceTruncatedVolumeKey(t *testing.T) {
	store, err := newApplianceSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureVolumeKey(context.Background(), "installation-a"); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(store.(*fileApplianceSecrets).directory, applianceSecretID("installation-a")+"-volume.key")
	if err := os.WriteFile(keyPath, []byte("truncated"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureVolumeKey(context.Background(), "installation-a"); err == nil {
		t.Fatal("truncated volume key was accepted")
	}
	value, err := os.ReadFile(keyPath)
	if err != nil || string(value) != "truncated" {
		t.Fatalf("truncated key was silently replaced: value=%q err=%v", value, err)
	}
}

func TestFileApplianceSecretsRejectInsecureDirectory(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "appliance-secrets")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := newApplianceSecretStore(root); err == nil {
		t.Fatal("insecure appliance secret directory was accepted")
	}
}

func TestWriteSecretOncePublishesOneLinkAndDoesNotReplace(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "secret")
	first := []byte("first")
	if err := writeSecretOnce(path, first); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		t.Fatalf("published secret link count = %v", stat)
	}
	if err := writeSecretOnce(path, []byte("second")); !os.IsExist(err) {
		t.Fatalf("exclusive publish error = %v", err)
	}
	value, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(value, first) {
		t.Fatalf("existing secret changed: %q, %v", value, err)
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".appliance-secret-*.tmp"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary links remained after publish: %v, %v", matches, err)
	}
}
