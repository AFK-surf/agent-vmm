//go:build linux

package network

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
)

func (m *Manager) DialContainer(ctx context.Context, namespaceID, containerID, instanceID string, port uint32) (net.Conn, error) {
	if port == 0 || port > 65535 {
		return nil, errors.New("port must be between 1 and 65535")
	}
	m.mu.RLock()
	endpoint, ok := m.endpoints[namespaceID][containerID]
	m.mu.RUnlock()
	if !ok || instanceID == "" || endpoint.InstanceID != instanceID {
		return nil, errors.New("container has no running network endpoint")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp4", net.JoinHostPort(endpoint.Address.String(), strconv.Itoa(int(port))))
	if err != nil {
		return nil, err
	}
	tracked := &trackedConn{Conn: connection, manager: m, key: namespaceID + "/" + containerID}
	m.mu.Lock()
	if current, stillRunning := m.endpoints[namespaceID][containerID]; !stillRunning || current.InstanceID != instanceID || m.closed {
		m.mu.Unlock()
		connection.Close()
		return nil, errors.New("container stopped while forward was connecting")
	}
	if m.forwards[tracked.key] == nil {
		m.forwards[tracked.key] = make(map[net.Conn]struct{})
	}
	m.forwards[tracked.key][tracked] = struct{}{}
	m.mu.Unlock()
	return tracked, nil
}

type trackedConn struct {
	net.Conn
	manager *Manager
	key     string
	once    sync.Once
}

func (connection *trackedConn) Close() error {
	err := connection.Conn.Close()
	connection.once.Do(func() {
		connection.manager.mu.Lock()
		delete(connection.manager.forwards[connection.key], connection)
		if len(connection.manager.forwards[connection.key]) == 0 {
			delete(connection.manager.forwards, connection.key)
		}
		connection.manager.mu.Unlock()
	})
	return err
}

func (connection *trackedConn) CloseWrite() error {
	if tcp, ok := connection.Conn.(*net.TCPConn); ok {
		return tcp.CloseWrite()
	}
	return nil
}
