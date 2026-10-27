//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
)

var errBootstrapNotFound = errors.New("protected bootstrap envelope is not found")
var errBootstrapStoreIntegrity = errors.New("protected bootstrap store integrity failure")

type SecretStore interface {
	LoadMCPTokenKey(context.Context, string) ([]byte, error)
	StoreMCPTokenKey(context.Context, string, []byte) error
	StoreBootstrap(context.Context, string, string, []byte) error
	LoadBootstrap(context.Context, string, string) ([]byte, error)
	DeleteBootstrap(context.Context, string, string) error
	DeleteInstallation(context.Context, string) error
}

func applianceSecretID(installationID string) string {
	digest := sha256.Sum256([]byte(installationID))
	return hex.EncodeToString(digest[:16])
}

func syncHostDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
