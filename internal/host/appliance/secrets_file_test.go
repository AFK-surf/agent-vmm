//go:build darwin || linux

package appliance

import (
	"bytes"
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/keychain/testfixture"
	"os"
	"path/filepath"
	"testing"
)

func TestApplianceSecretsPersistRotateAndDelete(t *testing.T) {
	for _, backend := range []string{"file", "keychain-memory"} {
		t.Run(backend, func(t *testing.T) {
			var secrets SecretStore
			if backend == "file" {
				var err error
				secrets, err = NewSecretStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			} else {
				secrets = NewKeychainSecretStore(testfixture.NewPasswords())
			}
			ctx := t.Context()
			seed := []byte("bootstrap-seed")
			if err := secrets.StoreBootstrap(ctx, "installation-a", "bootstrap-a", seed); err != nil {
				t.Fatal(err)
			}
			loaded, err := secrets.LoadBootstrap(ctx, "installation-a", "bootstrap-a")
			if err != nil || !bytes.Equal(loaded, seed) {
				t.Fatalf("bootstrap seed = %q, %v", loaded, err)
			}
			if err := secrets.DeleteBootstrap(ctx, "installation-a", "bootstrap-a"); err != nil {
				t.Fatal(err)
			}
			if _, err := secrets.LoadBootstrap(ctx, "installation-a", "bootstrap-a"); !errors.Is(err, errBootstrapNotFound) {
				t.Fatalf("deleted bootstrap: %v", err)
			}
			for _, key := range [][]byte{bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)} {
				if err := secrets.StoreMCPTokenKey(ctx, "installation-a", key); err != nil {
					t.Fatal(err)
				}
				loaded, err := secrets.LoadMCPTokenKey(ctx, "installation-a")
				if err != nil || !bytes.Equal(key, loaded) {
					t.Fatalf("rotated MCP key: %v", err)
				}
			}
			if err := secrets.DeleteInstallation(ctx, "installation-a"); err != nil {
				t.Fatal(err)
			}
			if _, err := secrets.LoadMCPTokenKey(ctx, "installation-a"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("deleted MCP key: %v", err)
			}
		})
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
	if _, err := NewSecretStore(root); err == nil {
		t.Fatal("insecure appliance secret directory was accepted")
	}
}
