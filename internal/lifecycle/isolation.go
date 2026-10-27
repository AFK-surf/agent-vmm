//go:build linux || (darwin && !e2e)

package lifecycle

import (
	"os/user"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

// Production builds have no isolation seam; see isolation_e2e.go.
const hostLabel = productionHostLabel

func serviceAccount(account *user.User) *user.User { return account }

func currentSchema() int { return store.CurrentSchema() }
