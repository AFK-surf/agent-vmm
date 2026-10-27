//go:build darwin || linux

package hostapp

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"github.com/AFK-surf/agent-vmm/internal/host/appliance"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/keychain"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

type secretStores struct {
	roots       trustkeys.Provider
	credentials remoteconnector.CredentialStore
	appliance   appliance.SecretStore
}

func newSecretStores(ctx context.Context, instanceDirectory, databasePath string) (secretStores, error) {
	database, err := store.Open(databasePath)
	if err != nil {
		return secretStores{}, err
	}
	record, readErr := database.GetDeviceIdentity(ctx)
	closeErr := database.Close()
	if readErr != nil && !errors.Is(readErr, sql.ErrNoRows) {
		return secretStores{}, errors.Join(readErr, closeErr)
	}
	if closeErr != nil {
		return secretStores{}, closeErr
	}
	selected, err := selectSecretBackend(ctx, filepath.Join(instanceDirectory, "secret-store"), record.Custody, keychain.Available() && isGUIHost(ctx), func(ctx context.Context, namespace string) error { return keychain.New(namespace).Probe(ctx) })
	if err != nil {
		return secretStores{}, err
	}
	if selected.Backend == "keychain" {
		return keychainSecretStores(keychain.New(selected.Namespace), record.Custody), nil
	}
	applianceSecrets, err := appliance.NewSecretStore(instanceDirectory)
	if err != nil {
		return secretStores{}, err
	}
	return secretStores{roots: trustkeys.NewFileProvider(filepath.Join(instanceDirectory, "device-root.key")), credentials: remoteconnector.NewCredentialStore(filepath.Join(instanceDirectory, "support", "remote-credentials")), appliance: applianceSecrets}, nil
}

type secretBackend struct {
	Backend   string `json:"backend"`
	Namespace string `json:"namespace,omitempty"`
}

func selectSecretBackend(ctx context.Context, path, custody string, eligible bool, probe func(context.Context, string) error) (secretBackend, error) {
	value, err := privatefs.ReadFile(path, 1, 256)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return secretBackend{}, fmt.Errorf("%w: read secret store selection: %w", keychain.ErrUnavailable, err)
	}
	var selected secretBackend
	fresh := errors.Is(err, os.ErrNotExist)
	if fresh {
		switch custody {
		case "file":
			selected.Backend = "file"
		case "keychain", "secure-enclave":
			return secretBackend{}, fmt.Errorf("%w: Keychain storage selection record is missing", keychain.ErrUnavailable)
		case "":
			selected.Backend = "file"
			if eligible {
				var namespace [16]byte
				if _, err := rand.Read(namespace[:]); err != nil {
					return secretBackend{}, err
				}
				selected.Namespace = hex.EncodeToString(namespace[:])
				if probe(ctx, selected.Namespace) == nil {
					selected.Backend = "keychain"
				} else {
					selected.Namespace = ""
				}
			}
		default:
			return secretBackend{}, fmt.Errorf("%w: unknown device root custody %q", keychain.ErrUnavailable, custody)
		}
	} else if err := json.Unmarshal(value, &selected); err != nil {
		return secretBackend{}, fmt.Errorf("%w: invalid secret store selection: %w", keychain.ErrUnavailable, err)
	}
	if selected.Backend != "file" && selected.Backend != "keychain" {
		return secretBackend{}, fmt.Errorf("%w: invalid secret store selection", keychain.ErrUnavailable)
	}
	if (selected.Backend == "file" && custody != "" && custody != "file") || (selected.Backend == "keychain" && custody == "file") {
		return secretBackend{}, fmt.Errorf("%w: secret store and device root custody differ", keychain.ErrUnavailable)
	}
	if selected.Backend == "keychain" {
		if selected.Namespace == "" {
			return secretBackend{}, fmt.Errorf("%w: Keychain storage namespace is missing", keychain.ErrUnavailable)
		}
		if !eligible {
			return secretBackend{}, fmt.Errorf("%w: recorded Keychain storage requires its signed GUI Host service", keychain.ErrUnavailable)
		}
		if !fresh {
			if err := probe(ctx, selected.Namespace); err != nil {
				return secretBackend{}, fmt.Errorf("%w: %w", keychain.ErrUnavailable, err)
			}
		}
	}
	if fresh {
		if err := ctx.Err(); err != nil {
			return secretBackend{}, err
		}
		data, err := json.Marshal(selected)
		if err != nil {
			return secretBackend{}, err
		}
		if err := atomicfile.PublishOnce(path, data, 0600); err != nil {
			return secretBackend{}, err
		}
	}
	return selected, nil
}

func keychainSecretStores(backend *keychain.Backend, custody string) secretStores {
	return secretStores{roots: trustkeys.NewKeychainProvider(backend, custody), credentials: remoteconnector.NewKeychainCredentialStore(backend), appliance: appliance.NewKeychainSecretStore(backend)}
}
