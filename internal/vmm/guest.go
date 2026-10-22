//go:build darwin || linux

package vmm

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (manager *Manager) ListenEgress() (net.Listener, string, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.state != hostv1.VMState_VM_STATE_RUNNING || manager.machine == nil || manager.bootID == "" {
		return nil, "", errors.New("VM is not running")
	}
	if manager.egressListener != nil {
		return nil, "", errors.New("egress listener already active")
	}
	listener, err := manager.machine.Listen(netprotocol.EgressPort)
	if err != nil {
		return nil, "", err
	}
	manager.egressListener = listener
	return listener, manager.bootID, nil
}

func (manager *Manager) CloseEgress() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.closeEgressLocked()
}

func (manager *Manager) closeEgressLocked() {
	if manager.egressListener != nil {
		_ = manager.egressListener.Close()
		manager.egressListener = nil
	}
}

func (manager *Manager) connectVMMD(ctx context.Context, allowStopping bool) (net.Conn, error) {
	manager.mu.Lock()
	machine := manager.machine
	running := manager.state == hostv1.VMState_VM_STATE_RUNNING || manager.state == hostv1.VMState_VM_STATE_STARTING || allowStopping && manager.state == hostv1.VMState_VM_STATE_STOPPING
	manager.mu.Unlock()
	if !running || machine == nil {
		return nil, errors.New("VM is not available")
	}
	return awaitVMMDConnection(ctx, func() (net.Conn, error) {
		return machine.Dial(ctx, VMMDPort)
	})
}

func (manager *Manager) DialVMMD(ctx context.Context) (*grpc.ClientConn, error) {
	return manager.dialVMMD(ctx, false)
}

func waitState(ctx context.Context, machine machineBackend, wanted hostv1.VMState) error {
	if machine.State() == wanted {
		return nil
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if machine.State() == wanted {
				return nil
			}
		case state, open := <-machine.StateChangedNotify():
			if !open {
				if machine.State() == wanted {
					return nil
				}
				return errors.New("VM state notification channel closed")
			}
			if state == wanted {
				return nil
			}
			if state == hostv1.VMState_VM_STATE_FAILED {
				return errors.New("VM entered error state")
			}
		}
	}
}

func (manager *Manager) waitHealth(ctx context.Context) (*vmmdv1.HealthResponse, error) {
	startedAt := time.Now()
	lastProgress := time.Time{}
	var lastErr error
	for {
		connection, err := manager.dialVMMD(ctx, false)
		lastErr = err
		if err == nil {
			client := vmmdv1.NewVMMDServiceClient(connection)
			health, healthErr := client.Health(ctx, &vmmdv1.HealthRequest{})
			_ = connection.Close()
			if healthErr == nil && health.GetReady() {
				return health, nil
			}
			lastErr = healthErr
		}
		if lastProgress.IsZero() || time.Since(lastProgress) >= 15*time.Second {
			log.Printf("vmm guest health waiting: elapsed=%s error=%v", time.Since(startedAt).Round(time.Millisecond), lastErr)
			lastProgress = time.Now()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (manager *Manager) prepareShutdown(ctx context.Context) error {
	connection, err := manager.dialVMMD(ctx, true)
	if err != nil {
		return err
	}
	defer connection.Close()
	_, err = vmmdv1.NewVMMDServiceClient(connection).PrepareShutdown(ctx, &vmmdv1.PrepareShutdownRequest{RequestId: manager.bootID})
	return err
}

func (manager *Manager) dialVMMD(ctx context.Context, allowStopping bool) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///vmmd", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return manager.connectVMMD(ctx, allowStopping)
	}))
}
