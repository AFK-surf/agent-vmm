package store

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (store *Store) DisconnectRemoteConnections(ctx context.Context, now time.Time) error {
	_, err := store.db.ExecContext(ctx, `UPDATE remote_registrations SET connection_state=?,connection_instance='',disconnected_at=?,updated_at=? WHERE connection_state=?`, int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED), now.UnixNano(), now.UnixNano(), int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED))
	return err
}
