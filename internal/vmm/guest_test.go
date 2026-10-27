//go:build darwin || linux

package vmm

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type bootDialMachine struct {
	machineBackend
	listener *bufconn.Listener
	dials    atomic.Int32
}

func (machine *bootDialMachine) Dial(ctx context.Context, _ uint32) (net.Conn, error) {
	machine.dials.Add(1)
	return machine.listener.DialContext(ctx)
}

type bootHealthServer struct {
	vmmdv1.UnimplementedVMMDServiceServer
}

func (*bootHealthServer) ComponentHealth(context.Context, *vmmdv1.ComponentHealthRequest) (*vmmdv1.ComponentHealthResponse, error) {
	return &vmmdv1.ComponentHealthResponse{}, nil
}

func TestDialBootCannotReconnectToReplacementMachine(t *testing.T) {
	a := &bootDialMachine{listener: bufconn.Listen(1 << 20)}
	b := &bootDialMachine{listener: bufconn.Listen(1 << 20)}
	defer b.listener.Close()
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, &bootHealthServer{})
	go server.Serve(a.listener)
	defer server.Stop()
	manager := &Manager{state: hostv1.VMState_VM_STATE_RUNNING, bootID: "boot-a", machine: a}
	connection, err := manager.DialBoot("boot-a")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := vmmdv1.NewVMMDServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{}); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.machine, manager.bootID = b, "boot-b"
	manager.mu.Unlock()
	server.Stop()
	if _, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("replaced boot RPC = %v", err)
	}
	if b.dials.Load() != 0 {
		t.Fatal("boot-a connection dialed boot-b")
	}
}
