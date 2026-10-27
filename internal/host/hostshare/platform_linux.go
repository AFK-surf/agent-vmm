package hostshare

import (
	"context"
	"os"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func hostSharesSupported() error {
	return status.Error(codes.Unimplemented, "host directory sharing is not supported on Linux")
}

func identifyHostShareSource(*os.File) (store.HostShareSource, error) {
	return store.HostShareSource{}, hostSharesSupported()
}

func (controller *Owner) PrepareHostShares(context.Context) error { return nil }
