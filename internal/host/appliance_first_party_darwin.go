//go:build darwin

package host

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

const firstPartyAppliancePublisher = "agent-vmm-first-party"

func installFirstPartyAppliancePublisher(ctx context.Context, database *store.Store, bundleDirectory string) error {
	key, err := os.ReadFile(filepath.Join(bundleDirectory, "mcp-appliance", "publisher.pub"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return errors.New("first-party appliance publisher key is invalid")
	}
	existing, err := database.GetAppliancePublisher(ctx, firstPartyAppliancePublisher)
	if err == nil {
		if !bytes.Equal(existing.PublicKey, key) {
			return errors.New("first-party appliance publisher key conflicts with persisted trust")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return database.PutAppliancePublisher(ctx, store.AppliancePublisher{KeyID: firstPartyAppliancePublisher, PublicKey: append([]byte(nil), key...), Revision: 1})
}
