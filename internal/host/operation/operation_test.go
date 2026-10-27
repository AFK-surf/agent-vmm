//go:build darwin

package operation

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBeginOperationDeduplicatesAndRejectsRequestIDReuse(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	first, existing, err := Begin(context.Background(), database, "diagnostic", "environment-a", "request-a")
	if err != nil || existing {
		t.Fatalf("first operation = %+v, existing=%t, err=%v", first, existing, err)
	}
	second, existing, err := Begin(context.Background(), database, "diagnostic", "environment-a", "request-a")
	if err != nil || !existing || second.ID != first.ID {
		t.Fatalf("duplicate operation = %+v, existing=%t, err=%v", second, existing, err)
	}
	if _, _, err := Begin(context.Background(), database, "vm-stop", "system", "request-a"); status.Code(err) != codes.Aborted {
		t.Fatalf("request ID reuse error = %v", err)
	}
}
