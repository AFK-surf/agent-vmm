package trust

import (
	"context"
	"crypto/sha256"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/comma_recovery"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func (manager *Manager) SignCommaRecovery(ctx context.Context, raw []byte) (*trustv1.DeviceIdentity, []byte, error) {
	_, wire, err := comma_recovery.Parse(raw, manager.now())
	if err != nil {
		return nil, nil, err
	}
	record, err := manager.store.GetDeviceIdentity(ctx)
	if err != nil || record.State != store.DeviceIdentityReady {
		return nil, nil, ErrNeedsRepair
	}
	digest := sha256.Sum256(wire)
	signature, err := manager.keys.Sign(ctx, record.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, nil, err
	}
	return proto.Clone(record.Identity).(*trustv1.DeviceIdentity), signature, nil
}
