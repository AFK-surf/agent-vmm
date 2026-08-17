//go:build darwin && (development || e2e)

package host

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDevelopmentApplianceSecretsPersistAndDelete(t *testing.T) {
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
	seed := []byte("development-bootstrap-seed")
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
