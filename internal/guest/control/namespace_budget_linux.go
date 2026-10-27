//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
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

// systemNamespaceCommitment counts the requested quota and the actual usage
// of every other system namespace. Each quota still bounds its namespace; the
// control reserve absorbs growth of other system namespaces after admission.
func systemNamespaceCommitment(facts map[uint32]namespaceDiskCommitment, ownProject uint32, existing bool, requested uint64) (uint64, bool) {
	committed := requested
	overflow := false
	for project, fact := range facts {
		if existing && project == ownProject {
			continue
		}
		var carry uint64
		committed, carry = bits.Add64(committed, fact.usage, 0)
		overflow = overflow || carry != 0
	}
	return committed, overflow
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
		committed, overflow := systemNamespaceCommitment(facts, ownProject, labels != nil, requested)
		if overflow || committed > guestdisk.ControlSystemBytes {
			for project, fact := range facts {
				r.logger.Warn("system namespace disk commitment", "namespace_id", fact.namespace, "project", project, "quota_bytes", fact.limit, "usage_bytes", fact.usage)
			}
			err := fmt.Errorf("%w: system namespace %q requested_quota_bytes=%d old_quota_bytes=%d committed_bytes=%d budget_bytes=%d accounting_overflow=%t", guestdisk.ErrCapacityUnavailable, namespace, requested, old, committed, guestdisk.ControlSystemBytes, overflow)
			r.logger.Warn("system namespace storage admission rejected", "error", err)
			return atRuntimeStage("system_namespace_admission", err)
		}
		return nil
	}
	return r.admitPoolGrowth(ctx, requested, old)
}

func (r *Runtime) poolCommitments(ctx context.Context) (map[uint32]namespaceDiskCommitment, error) {
	facts, err := r.namespaceDiskCommitments(ctx, false)
	if err != nil {
		return nil, err
	}
	appliance, err := newApplianceVolumeManager(r.stateDevice).commitments()
	if err != nil {
		return nil, err
	}
	for project, fact := range appliance {
		facts[project] = fact
	}
	return facts, nil
}

func (r *Runtime) admitPoolGrowth(ctx context.Context, requested, old uint64) error {
	if requested <= old {
		return nil
	}
	snapshot, err := r.guestDiskSnapshot(ctx)
	if err != nil {
		return err
	}
	available, ok := snapshot.AvailableBytes()
	if !ok || requested-old > available {
		r.logger.Warn("namespace storage admission rejected", "requested_quota_bytes", requested, "old_quota_bytes", old, "requested_delta_bytes", requested-old, "available_bytes", available, "available_valid", ok, "capacity_bytes", snapshot.CapacityBytes, "used_bytes", snapshot.UsedBytes, "import_and_builder_reservation_bytes", snapshot.ImportReservationBytes, "namespace_remaining_commitment_bytes", snapshot.NamespaceCommitmentBytes, "emergency_bytes", snapshot.EmergencyBytes, "metadata_growth_bytes", snapshot.MetadataGrowthBytes, "error", guestdisk.ErrCapacityUnavailable)
		return guestdisk.ErrCapacityUnavailable
	}
	return nil
}
