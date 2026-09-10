package namespaceconfig

import (
	"fmt"
	"strconv"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

const (
	LabelPrefix             = "agentvmm.afk.surf/"
	LegacyReservationPrefix = LabelPrefix + "reservation-"
	LegacyStateMarker       = ".legacy-state-migration"
)

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
	cpu, err := parse("cpu", 32)
	if err != nil {
		return 0, nil, err
	}
	memory, err := parse("memory", 64)
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
		Id: id, Limits: &vmmdv1.NamespaceLimits{CpuMillis: uint32(cpu), MemoryBytes: memory, Pids: uint32(pids), DiskBytes: disk},
		EgressMode: vmmdv1.EgressMode(egress), ProjectId: uint32(projectID), Subnet: labels[LabelPrefix+"subnet"],
	}, nil
}

func ConvertLegacy(labels map[string]string) (map[string]string, bool, error) {
	hasLegacy := false
	for key := range labels {
		hasLegacy = hasLegacy || strings.HasPrefix(key, LegacyReservationPrefix)
	}
	if !hasLegacy {
		return labels, false, nil
	}
	converted := make(map[string]string, len(labels))
	for key, value := range labels {
		if !strings.HasPrefix(key, LegacyReservationPrefix) {
			converted[key] = value
		}
	}
	if labels[LegacyReservationPrefix+"allocation"] != "" {
		for current, legacy := range map[string]string{
			"cpu": "cpu-max", "memory": "memory-max", "pids": "pid-max", "disk": "writable-quota",
		} {
			value := labels[LegacyReservationPrefix+legacy]
			if value == "" {
				value = labels[LabelPrefix+current]
			}
			if existing := labels[LabelPrefix+current]; existing != "" && existing != value {
				return nil, false, fmt.Errorf("legacy namespace label %s conflicts with current value", current)
			}
			converted[LabelPrefix+current] = value
		}
	}
	if _, _, err := Decode("legacy", converted); err != nil {
		return nil, false, err
	}
	return converted, true, nil
}
