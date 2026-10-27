package hostshare

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/Code-Hex/vz/v3"
)

type Manager interface {
	Snapshot() *hostv1.VM
	HasMachineAttachment() bool
	SetHostDirectoryShares(map[string]*vz.SharedDirectory) error
}
