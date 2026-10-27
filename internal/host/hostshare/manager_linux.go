package hostshare

import hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"

type Manager interface {
	Snapshot() *hostv1.VM
	HasMachineAttachment() bool
}
