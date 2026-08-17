//go:build darwin

package host

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestInstallFirstPartyAppliancePublisher(t *testing.T) {
	directory := t.TempDir()
	database, err := store.Open(filepath.Join(directory, "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	bundle := filepath.Join(directory, "bundle")
	if err := os.MkdirAll(filepath.Join(bundle, "mcp-appliance"), 0o755); err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{7}, 32)
	if err := os.WriteFile(filepath.Join(bundle, "mcp-appliance", "publisher.pub"), key, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := installFirstPartyAppliancePublisher(context.Background(), database, bundle); err != nil {
		t.Fatal(err)
	}
	publisher, err := database.GetAppliancePublisher(context.Background(), firstPartyAppliancePublisher)
	if err != nil || publisher.Revision != 1 || !bytes.Equal(publisher.PublicKey, key) {
		t.Fatalf("publisher=%+v err=%v", publisher, err)
	}
	publisher.Revoked = true
	if err := database.PutAppliancePublisher(context.Background(), publisher); err != nil {
		t.Fatal(err)
	}
	if err := installFirstPartyAppliancePublisher(context.Background(), database, bundle); err != nil {
		t.Fatal(err)
	}
	publisher, err = database.GetAppliancePublisher(context.Background(), firstPartyAppliancePublisher)
	if err != nil || !publisher.Revoked {
		t.Fatalf("publisher revocation was reset: %+v err=%v", publisher, err)
	}
}
