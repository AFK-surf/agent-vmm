//go:build darwin

package host

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
	controller := &Controller{store: database}

	first, existing, err := controller.beginOperation(context.Background(), "diagnostic", "environment-a", "request-a")
	if err != nil || existing {
		t.Fatalf("first operation = %+v, existing=%t, err=%v", first, existing, err)
	}
	second, existing, err := controller.beginOperation(context.Background(), "diagnostic", "environment-a", "request-a")
	if err != nil || !existing || second.ID != first.ID {
		t.Fatalf("duplicate operation = %+v, existing=%t, err=%v", second, existing, err)
	}
	if _, _, err := controller.beginOperation(context.Background(), "vm-stop", "system", "request-a"); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("request ID reuse error = %v", err)
	}
}
