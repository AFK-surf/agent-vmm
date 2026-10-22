//go:build darwin || linux

package vmm

import (
	"context"
	"net"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type machineBackend interface {
	State() hostv1.VMState
	StateChangedNotify() <-chan hostv1.VMState
	CanStop() bool
	Stop() error
	CanRequestStop() bool
	RequestStop() (bool, error)
	Dial(context.Context, uint32) (net.Conn, error)
	Listen(uint32) (net.Listener, error)
}
