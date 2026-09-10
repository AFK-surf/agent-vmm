//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
)

const labelPrefix = namespaceconfig.LabelPrefix

func (r *Runtime) EnsureNamespace(ctx context.Context, request *vmmdv1.EnsureNamespaceRequest) (*vmmdv1.Namespace, error) {
	return r.ensureNamespace(ctx, request, false)
}

func (r *Runtime) ensureNamespace(ctx context.Context, request *vmmdv1.EnsureNamespaceRequest, system bool) (*vmmdv1.Namespace, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	var identityErr error
	if system {
		identityErr = validateSystemNamespace(request.GetNamespaceId())
	} else {
		identityErr = validateID("namespace_id", request.GetNamespaceId())
	}
	if identityErr != nil {
		return nil, identityErr
	}
	if err := validateNamespaceLimits(request.GetLimits()); err != nil {
		return nil, err
	}
	if request.GetEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
		return nil, errors.New("egress_mode is required")
	}
	unlock := r.lockNamespace(request.GetNamespaceId())
	defer unlock()

	store := r.client.NamespaceService()
	names, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	exists := false
	for _, name := range names {
		if name == request.GetNamespaceId() {
			exists = true
			break
		}
	}
	var labels map[string]string
	if exists {
		labels, err = store.Labels(ctx, request.GetNamespaceId())
		if err != nil {
			return nil, err
		}
		if system && labels[labelPrefix+"role"] != "system-appliance" {
			return nil, errors.New("system namespace role does not match")
		}
	}
	commit := func(transaction *guestdisk.Transaction) error {
		if exists {
			projectID, current, err := namespaceFromLabels(request.GetNamespaceId(), labels)
			if err != nil {
				return err
			}
			if !sameLimits(current.GetLimits(), request.GetLimits()) || current.GetEgressMode() != request.GetEgressMode() {
				updated := namespaceLabels(request.GetLimits(), request.GetEgressMode(), projectID, current.GetSubnet())
				if system {
					updated[labelPrefix+"role"] = "system-appliance"
				}
				if err := func() error {
					for key, value := range updated {
						if err := store.SetLabel(ctx, request.GetNamespaceId(), key, value); err != nil {
							return err
						}
					}
					return nil
				}(); err != nil {
					return err
				}
				labels = updated
			}
		} else {
			projectID, subnet, allocationErr := r.allocateNamespace(ctx, request.GetNamespaceId())
			if allocationErr != nil {
				return allocationErr
			}
			labels = namespaceLabels(request.GetLimits(), request.GetEgressMode(), projectID, subnet)
			if system {
				labels[labelPrefix+"role"] = "system-appliance"
			}
			if err := func() error { return store.Create(ctx, request.GetNamespaceId(), labels) }(); err != nil {
				return err
			}
		}
		projectID, namespace, err := namespaceFromLabels(request.GetNamespaceId(), labels)
		if err != nil {
			return err
		}
		for _, directory := range []string{volumePath(request.GetNamespaceId(), ""), workspacePath(request.GetNamespaceId()),
			namespaceStateRoot(request.GetNamespaceId()) + "/logs/" + request.GetNamespaceId(), namespaceStateRoot(request.GetNamespaceId()) + "/build-cache/" + request.GetNamespaceId(), namespaceStateRoot(request.GetNamespaceId()) + "/build-tmp/" + request.GetNamespaceId()} {
			if err := ensureDirectory(directory, 0o700); err != nil {
				return err
			}
			if err := assignProject(directory, projectID); err != nil {
				return err
			}
		}
		if err := setProjectQuota(r.namespaceStateDevice(request.GetNamespaceId()), projectID, request.GetLimits().GetDiskBytes()); err != nil {
			return err
		}
		if err := applyNamespaceCgroup(request.GetNamespaceId(), request.GetLimits()); err != nil {
			return err
		}
		return r.network.EnsureNamespace(namespace.GetId(), namespace.GetSubnet(), namespace.GetEgressMode())
	}
	if err := r.diskBudget.MutateInventory(commit); err != nil {
		return nil, err
	}
	_, namespace, err := namespaceFromLabels(request.GetNamespaceId(), labels)
	return namespace, err
}

func (r *Runtime) DeleteNamespace(ctx context.Context, namespaceID string) error {
	if err := validateID("namespace_id", namespaceID); err != nil {
		return err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	return r.deleteNamespaceLocked(ctx, namespaceID)
}

func (r *Runtime) deleteNamespaceLocked(ctx context.Context, namespaceID string) error {
	return r.mutateNamespaceInventory(func(*guestdisk.Transaction) error {
		return r.deleteNamespaceUnderBudgetLocked(ctx, namespaceID)
	})
}

func (r *Runtime) mutateNamespaceInventory(mutate func(*guestdisk.Transaction) error) error {
	return r.diskBudget.MutateInventory(mutate)
}

// deleteNamespaceUnderBudgetLocked removes the containerd namespace bucket and
// its quota state while holding both the namespace lock and the global disk
// mutation lock. OCI quota settlement selects an existing containerd namespace
// under the same disk lock, so deletion cannot invalidate or recreate that
// bucket between selection and the synchronous GC barrier.
func (r *Runtime) deleteNamespaceUnderBudgetLocked(ctx context.Context, namespaceID string) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	exists := false
	for _, name := range names {
		if name == namespaceID {
			exists = true
			break
		}
	}
	if !exists {
		return nil
	}

	nsctx := namespaceContext(ctx, namespaceID)
	if err := r.builder.CleanupNamespace(ctx, namespaceID); err != nil {
		return err
	}
	containers, err := r.ListContainers(ctx, namespaceID)
	if err != nil {
		return err
	}
	images, err := r.client.ListImages(nsctx)
	if err != nil {
		return err
	}
	volumes, err := directoryEntries(volumePath(namespaceID, ""))
	if err != nil {
		return err
	}
	if len(containers) != 0 || len(volumes) != 0 {
		return failedPrecondition("namespace is not empty")
	}
	if err := r.network.CanDeleteNamespace(namespaceID); err != nil {
		return err
	}
	activeLeases, err := r.client.LeasesService().List(nsctx)
	if err != nil {
		return err
	}
	for _, lease := range activeLeases {
		if err := r.client.LeasesService().Delete(nsctx, lease, leases.SynchronousDelete); err != nil {
			return err
		}
	}
	for _, image := range images {
		if err := r.client.ImageService().Delete(nsctx, image.Name(), coreimages.SynchronousDelete()); err != nil {
			return err
		}
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return err
	}
	projectID, _, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return err
	}
	if err := removeNamespaceState(namespaceID); err != nil {
		return err
	}
	if err := setProjectQuota(r.namespaceStateDevice(namespaceID), projectID, 0); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join("/sys/fs/cgroup", resourcev2.NamespaceCgroup(namespaceID))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := r.network.DeleteNamespace(namespaceID); err != nil {
		return err
	}
	if err := r.client.NamespaceService().Delete(ctx, namespaceID); err != nil {
		return err
	}
	return nil
}
func validateNamespaceLimits(limits *vmmdv1.NamespaceLimits) error {
	if limits == nil || limits.GetCpuMillis() == 0 || limits.GetMemoryBytes() < 16<<20 || limits.GetPids() == 0 || limits.GetDiskBytes() == 0 {
		return errors.New("positive cpu, memory, pids, and disk limits are required")
	}
	return nil
}

func (r *Runtime) allocateNamespace(ctx context.Context, namespaceID string) (uint32, string, error) {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return 0, "", err
	}
	usedProjects := make(map[uint32]bool)
	usedSubnets := make(map[string]bool)
	for _, name := range names {
		labels, err := r.client.NamespaceService().Labels(ctx, name)
		if err != nil {
			return 0, "", err
		}
		if value, err := strconv.ParseUint(labels[labelPrefix+"project-id"], 10, 32); err == nil {
			usedProjects[uint32(value)] = true
		}
		usedSubnets[labels[labelPrefix+"subnet"]] = true
	}
	hash := sha256.Sum256([]byte(namespaceID))
	for offset := uint32(0); offset < 16384; offset++ {
		index := ((uint32(hash[0])<<8 | uint32(hash[1])) + offset) % 16384
		projectID := uint32(10000) + index
		subnet := fmt.Sprintf("10.%d.%d.0/24", 64+index/256, index%256)
		if !usedProjects[projectID] && !usedSubnets[subnet] {
			return projectID, subnet, nil
		}
	}
	return 0, "", errors.New("namespace allocation exhausted")
}

func namespaceLabels(limits *vmmdv1.NamespaceLimits, egress vmmdv1.EgressMode, projectID uint32, subnet string) map[string]string {
	return map[string]string{
		labelPrefix + "project-id": strconv.FormatUint(uint64(projectID), 10),
		labelPrefix + "subnet":     subnet,
		labelPrefix + "cpu":        strconv.FormatUint(uint64(limits.GetCpuMillis()), 10),
		labelPrefix + "memory":     strconv.FormatUint(limits.GetMemoryBytes(), 10),
		labelPrefix + "pids":       strconv.FormatUint(uint64(limits.GetPids()), 10),
		labelPrefix + "disk":       strconv.FormatUint(limits.GetDiskBytes(), 10),
		labelPrefix + "egress":     strconv.FormatInt(int64(egress), 10),
	}
}

func namespaceFromLabels(id string, labels map[string]string) (uint32, *vmmdv1.Namespace, error) {
	return namespaceconfig.Decode(id, labels)
}

func sameLimits(left, right *vmmdv1.NamespaceLimits) bool {
	return left.GetCpuMillis() == right.GetCpuMillis() && left.GetMemoryBytes() == right.GetMemoryBytes() && left.GetPids() == right.GetPids() && left.GetDiskBytes() == right.GetDiskBytes()
}
