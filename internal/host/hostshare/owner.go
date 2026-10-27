//go:build darwin || linux

// Package hostshare owns approved Host directory identity and Guest projection.
package hostshare

import (
	"context"
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

type Workloads interface {
	LockEnvironment(context.Context, string) (func(), error)
	StopWorkloadContainerWithIntentLocked(context.Context, vmmdv1.VMMDServiceClient, string, *vmmdv1.StopContainerRequest, bool) (*vmmdv1.StopContainerResponse, error)
}

type Config struct {
	Store     *store.Store
	Guest     *guestconn.Connection
	Manager   Manager
	Inventory *lifecycle.InventoryFence
}

type Owner struct {
	store           *store.Store
	guestConnection *guestconn.Connection
	manager         Manager
	inventory       *lifecycle.InventoryFence
	workloads       Workloads
	mu              sync.Mutex
	shares          map[string]store.HostShare
	dirty           bool
}

func New(config Config) *Owner {
	return &Owner{store: config.Store, guestConnection: config.Guest, manager: config.Manager, inventory: config.Inventory}
}

// BindWorkloads completes the component graph before the Host starts.
func (owner *Owner) BindWorkloads(workloads Workloads) { owner.workloads = workloads }
