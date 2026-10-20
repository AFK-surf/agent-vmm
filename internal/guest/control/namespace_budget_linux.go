//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/bits"

	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

type namespaceDiskCommitment struct {
	namespace string
	limit     uint64
	usage     uint64
}

func namespaceQuotaBytes(bytes uint64) (uint64, error) {
	rounded, carry := bits.Add64(bytes, 1023, 0)
	if carry != 0 {
		return 0, guestdisk.ErrCapacityUnavailable
	}
	return rounded / 1024 * 1024, nil
}

func (r *Runtime) namespaceDiskCommitments(ctx context.Context, system bool) (map[uint32]namespaceDiskCommitment, error) {
	store := r.client.NamespaceService()
	names, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	facts := make(map[uint32]namespaceDiskCommitment, len(names))
	for _, name := range names {
		if isSystemNamespace(name) != system || name == runtimeContainerdNamespace || name == applianceBuildNamespace {
			continue
		}
		labels, err := store.Labels(ctx, name)
		if err != nil {
			return nil, err
		}
		project, namespace, err := namespaceFromLabels(name, labels)
		if err != nil {
			return nil, err
		}
		state, err := guestquota.Read(r.namespaceStateDevice(name), project)
		if err != nil {
			return nil, err
		}
		declared, err := namespaceQuotaBytes(namespace.Limits.DiskBytes)
		if err != nil {
			return nil, err
		}
		facts[project] = namespaceDiskCommitment{namespace: name, limit: max(state.HardLimitBytes, declared), usage: state.UsageBytes}
	}
	return facts, nil
}

func namespaceRemainingCommitments(before, after map[uint32]namespaceDiskCommitment) (uint64, error) {
	var total uint64
	for project, fact := range before {
		newer := after[project]
		limit := max(fact.limit, newer.limit)
		usage := min(fact.usage, newer.usage)
		if limit <= usage {
			continue
		}
		var carry uint64
		total, carry = bits.Add64(total, limit-usage, 0)
		if carry != 0 {
			return 0, errors.New("namespace disk commitments overflow byte accounting")
		}
	}
	return total, nil
}

func (r *Runtime) admitNamespaceDiskGrowth(ctx context.Context, namespace string, requested uint64, labels map[string]string) error {
	system := isSystemNamespace(namespace)
	requested, err := namespaceQuotaBytes(requested)
	if err != nil {
		return err
	}
	facts, err := r.namespaceDiskCommitments(ctx, system)
	if err != nil {
		return err
	}
	var old uint64
	var ownProject uint32
	if labels != nil {
		project, _, parseErr := namespaceFromLabels(namespace, labels)
		if parseErr != nil {
			return parseErr
		}
		ownProject = project
		old = facts[project].limit
	}
	if requested <= old {
		return nil
	}
	if system {
		committed := requested
		if labels != nil {
			committed = 0
		}
		overflow := false
		for project, fact := range facts {
			if labels != nil && project == ownProject {
				fact.limit = requested
			}
			var carry uint64
			committed, carry = bits.Add64(committed, max(fact.limit, fact.usage), 0)
			overflow = overflow || carry != 0
		}
		if overflow || committed > guestdisk.ControlSystemBytes {
			for project, fact := range facts {
				log.Printf("system namespace disk commitment namespace=%s project=%d quota_bytes=%d usage_bytes=%d", fact.namespace, project, fact.limit, fact.usage)
			}
			err := fmt.Errorf("%w: system namespace %q requested_quota_bytes=%d old_quota_bytes=%d committed_bytes=%d budget_bytes=%d accounting_overflow=%t", guestdisk.ErrCapacityUnavailable, namespace, requested, old, committed, guestdisk.ControlSystemBytes, overflow)
			log.Printf("system namespace storage admission rejected: %v", err)
			return atRuntimeStage("system_namespace_admission", err)
		}
		return nil
	}
	snapshot, err := r.guestDiskSnapshot(ctx)
	if err != nil {
		return err
	}
	available, ok := snapshot.AvailableBytes()
	if !ok || requested-old > available {
		log.Printf("namespace storage admission rejected namespace=%s project=%d requested_quota_bytes=%d old_quota_bytes=%d requested_delta_bytes=%d available_bytes=%d available_valid=%t capacity_bytes=%d used_bytes=%d import_and_builder_reservation_bytes=%d namespace_remaining_commitment_bytes=%d emergency_bytes=%d metadata_growth_bytes=%d error=%v", namespace, ownProject, requested, old, requested-old, available, ok, snapshot.CapacityBytes, snapshot.UsedBytes, snapshot.ImportReservationBytes, snapshot.NamespaceCommitmentBytes, snapshot.EmergencyBytes, snapshot.MetadataGrowthBytes, guestdisk.ErrCapacityUnavailable)
		return guestdisk.ErrCapacityUnavailable
	}
	return nil
}
