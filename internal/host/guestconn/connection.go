//go:build darwin || linux

package guestconn

import (
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Connection struct {
	mu      sync.Mutex
	current *bootConnection
}

type bootConnection struct {
	bootID string
	conn   *grpc.ClientConn
	client vmmdv1.VMMDServiceClient
}

func (owner *Connection) Client() (vmmdv1.VMMDServiceClient, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.current == nil {
		return nil, status.Error(codes.Unavailable, "guest control service is unavailable")
	}
	return owner.current.client, nil
}

func (owner *Connection) Connect(manager interface {
	DialBoot(string) (*grpc.ClientConn, error)
}, bootID string) error {
	owner.mu.Lock()
	if current := owner.current; current != nil && current.bootID == bootID {
		owner.mu.Unlock()
		return nil
	}
	connection, err := manager.DialBoot(bootID)
	if err != nil {
		owner.mu.Unlock()
		return err
	}
	previous := owner.current
	owner.current = &bootConnection{bootID: bootID, conn: connection, client: vmmdv1.NewVMMDServiceClient(connection)}
	owner.mu.Unlock()
	if previous != nil {
		return previous.conn.Close()
	}
	return nil
}

func (owner *Connection) Disconnect() {
	owner.mu.Lock()
	previous := owner.current
	owner.current = nil
	owner.mu.Unlock()
	if previous != nil {
		_ = previous.conn.Close()
	}
}
