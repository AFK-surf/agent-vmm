package namespaceconfig

import (
	"fmt"
	"strconv"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

const LabelPrefix = "surf.afk.agentvmm."

func Decode(id string, labels map[string]string) (uint32, *vmmdv1.Namespace, error) {
	parse := func(name string, bits int) (uint64, error) {
		value, err := strconv.ParseUint(labels[LabelPrefix+name], 10, bits)
		if err != nil {
			return 0, fmt.Errorf("invalid namespace label %s: %w", name, err)
		}
		return value, nil
	}
	projectID, err := parse("project-id", 32)
	if err != nil {
		return 0, nil, err
	}
	pids, err := parse("pids", 32)
	if err != nil {
		return 0, nil, err
	}
	disk, err := parse("disk", 64)
	if err != nil {
		return 0, nil, err
	}
	egress, err := parse("egress", 32)
	if err != nil {
		return 0, nil, err
	}
	return uint32(projectID), &vmmdv1.Namespace{
		Id: id, Limits: &vmmdv1.NamespaceLimits{Pids: uint32(pids), DiskBytes: disk},
		EgressMode: vmmdv1.EgressMode(egress), ProjectId: uint32(projectID), Subnet: labels[LabelPrefix+"subnet"],
	}, nil
}
