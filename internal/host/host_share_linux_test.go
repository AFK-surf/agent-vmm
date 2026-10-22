package host

import (
	"context"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLinuxHostShareMutationRejectsBeforePersistentState(t *testing.T) {
	controller := new(Controller)
	_, err := controller.mutateHostShare(context.Background(), "host-share-create", "request", nil, store.HostShare{}, 0)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unsupported host share = %v", err)
	}
}
