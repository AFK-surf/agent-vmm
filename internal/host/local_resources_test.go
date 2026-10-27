//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
)

func TestLocalResourcePagesKeepMoreThanPreviewLimitAndUnknownMetrics(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("environment-%03d", i)
		_, err = database.CreateEnvironment(context.Background(), store.Environment{ID: id, NamespaceID: id, DisplayName: "private cloud label", OwnerKind: store.OwnerLocal})
		if err != nil {
			t.Fatal(err)
		}
	}
	controller := newTestController(&Controller{store: database, manager: &vmm.Manager{}})
	server := &operatorServer{controller: controller}
	cursor := ""
	seen := map[string]bool{}
	for {
		result, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return server.ListLocalEnvironmentPage(ctx, &hostv1.ListLocalEnvironmentPageRequest{AfterId: cursor})
		})
		if err != nil {
			t.Fatal(err)
		}
		page := result.(*hostv1.ListLocalEnvironmentPageResponse)
		if len(page.GetEnvironments()) > 32 {
			t.Fatal("environment page exceeds request budget")
		}
		for _, row := range page.GetEnvironments() {
			if seen[row.GetId()] || row.PrivateDiskBytes != nil || row.GetSampledAt() != nil {
				t.Fatalf("duplicate or fabricated measurement: %v", row)
			}
			seen[row.GetId()] = true
		}
		cursor = page.GetNextCursor()
		if cursor == "" {
			break
		}
	}
	if len(seen) != 101 {
		t.Fatalf("paged environments=%d", len(seen))
	}
	database.Close()
	result, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return server.GetLocalDiskSummary(ctx, &hostv1.GetLocalDiskSummaryRequest{})
	})
	if err != nil || result.(*hostv1.GetLocalDiskSummaryResponse).GetGuestStateFs() != nil {
		t.Fatalf("disk summary depended on database or filled missing metric: %v,%v", result, err)
	}
}
