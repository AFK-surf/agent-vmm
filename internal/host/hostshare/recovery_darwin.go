//go:build darwin

package hostshare

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/Code-Hex/vz/v3"
)

// The VM lifecycle owner holds the inventory transition lock through Start.
func (controller *Owner) PrepareHostShares(ctx context.Context) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	values, err := controller.store.ListHostShares(ctx, "")
	if err != nil {
		return err
	}
	controller.shares = make(map[string]store.HostShare)
	controller.dirty = true
	directories := make(map[string]*vz.SharedDirectory)
	for _, value := range values {
		if err := controller.reconcileHostShareLocked(ctx, nil, value); err != nil {
			return err
		}
		value, err := controller.store.GetHostShare(ctx, value.ID)
		if err != nil {
			return err
		}
		if value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED || value.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKING {
			continue
		}
		approved, err := controller.store.HostShareEnvironmentApproved(ctx, value.EnvironmentID)
		if err != nil {
			return err
		}
		if !approved {
			value.State = hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE
			value.UnavailableReason = "service environment is no longer approved"
			if err := controller.store.UpdateHostShare(ctx, value, value.Revision); err != nil {
				return err
			}
			continue
		}
		source, sourceErr := reopenHostShareSource(value.Source)
		if sourceErr == nil {
			source.Close()
			var directory *vz.SharedDirectory
			directory, sourceErr = vz.NewSharedDirectory(value.Source.Path, value.ReadOnly)
			if sourceErr == nil {
				directories[value.ID] = directory
				controller.shares[value.ID] = value
			}
		}
		value.State = hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE
		value.UnavailableReason = "waiting for Guest host share projection"
		if sourceErr != nil {
			value.UnavailableReason = sourceErr.Error()
		}
		if err := controller.store.UpdateHostShare(ctx, value, value.Revision); err != nil {
			return err
		}
	}
	return controller.manager.SetHostDirectoryShares(directories)
}
