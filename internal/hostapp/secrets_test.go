//go:build darwin || linux

package hostapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/keychain"
)

func TestSecretBackendSelectionStaysWithItsFirstStorage(t *testing.T) {
	for _, test := range []struct {
		name, custody, want  string
		eligible, probeFails bool
	}{
		{"development", "", "file", false, false},
		{"signed-gui", "", "keychain", true, false},
		{"unavailable-first-probe", "", "file", true, true},
		{"existing-file", "file", "file", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.Chmod(directory, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, "secret-store")
			probe := func(context.Context, string) error {
				if test.probeFails {
					return errors.New("locked")
				}
				return nil
			}
			selected, err := selectSecretBackend(t.Context(), path, test.custody, test.eligible, probe)
			if err != nil || selected.Backend != test.want {
				t.Fatalf("initial selection=%+v,%v", selected, err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			selected, err = selectSecretBackend(t.Context(), path, test.custody, false, func(context.Context, string) error { return errors.New("unavailable") })
			if test.want == "keychain" {
				if !errors.Is(err, keychain.ErrUnavailable) {
					t.Fatalf("Keychain selection fell back: %+v,%v", selected, err)
				}
			} else if err != nil || selected.Backend != "file" {
				t.Fatalf("file selection changed: %+v,%v", selected, err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(before) {
				t.Fatalf("selection record changed: %+v,%v", after, err)
			}
		})
	}
}

func TestRecordedKeychainFailureDoesNotCreateFileStorage(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "secret-store")
	if err := os.WriteFile(path, []byte(`{"backend":"keychain","namespace":"fixed-test-namespace"}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := selectSecretBackend(t.Context(), path, "keychain", true, func(context.Context, string) error { return errors.New("locked") })
	if !errors.Is(err, keychain.ErrUnavailable) {
		t.Fatalf("locked Keychain=%v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatalf("Keychain failure wrote file secrets: %v,%v", entries, err)
	}
}

func TestMissingKeychainSelectionCannotSelectAnotherStore(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	_, err := selectSecretBackend(t.Context(), filepath.Join(directory, "secret-store"), "secure-enclave", true, func(context.Context, string) error { return nil })
	if !errors.Is(err, keychain.ErrUnavailable) {
		t.Fatalf("missing Keychain record=%v", err)
	}
}

func TestFreshStateUsesANewKeychainNamespace(t *testing.T) {
	var previous string
	for range 2 {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, "secret-store")
		selected, err := selectSecretBackend(t.Context(), path, "", true, func(context.Context, string) error { return nil })
		if err != nil || selected.Namespace == "" || selected.Namespace == previous {
			t.Fatalf("fresh namespace=%q,%v", selected.Namespace, err)
		}
		reloaded, err := selectSecretBackend(t.Context(), path, "keychain", true, func(_ context.Context, namespace string) error {
			if namespace != selected.Namespace {
				t.Fatal("probe used another namespace")
			}
			return nil
		})
		if err != nil || reloaded != selected {
			t.Fatalf("persisted namespace changed: %+v,%v", reloaded, err)
		}
		previous = selected.Namespace
	}
}
