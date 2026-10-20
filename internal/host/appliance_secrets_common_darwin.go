//go:build darwin

package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
)

var errBootstrapNotFound = errors.New("protected bootstrap envelope is not found")
var errBootstrapStoreIntegrity = errors.New("protected bootstrap store integrity failure")

type applianceSecretStore interface {
	StoreBootstrap(context.Context, string, string, []byte) error
	LoadBootstrap(context.Context, string, string) ([]byte, error)
	DeleteBootstrap(context.Context, string, string) error
	DeleteInstallation(context.Context, string) error
}

func applianceSecretID(installationID string) string {
	digest := sha256.Sum256([]byte(installationID))
	return hex.EncodeToString(digest[:16])
}

func clearSecret(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func syncHostDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
