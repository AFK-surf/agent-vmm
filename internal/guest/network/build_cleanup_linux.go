//go:build linux

package network

import (
	"context"
	"os"
)

func (m *Manager) CleanupBuildEndpoint(ctx context.Context, namespace, id string) error {
	for _, loopback := range []bool{false, true} {
		endpointID := id
		if loopback {
			endpointID = "loopback-" + id
		}
		path := endpointNetNS(namespace, endpointID)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		if loopback {
			if err := m.DetachLoopback(ctx, namespace, id, path); err != nil {
				return err
			}
		} else if err := m.CleanupStaleEndpoint(namespace, id); err != nil {
			return err
		}
	}
	return nil
}
