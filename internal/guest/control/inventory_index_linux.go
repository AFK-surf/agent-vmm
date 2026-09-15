//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	corecontainers "github.com/containerd/containerd/v2/core/containers"
)

// runtimeInventoryIndex is a rebuildable, in-memory projection of
// containerd-owned records. It lets request paths enumerate one owner without
// scanning the shared runtime containerd namespace. containerd remains the
// durable authority; NewRuntime rebuilds this index before VMMD is available.
type runtimeInventoryIndex struct {
	mu         sync.RWMutex
	containers map[string]map[string]struct{}
	images     map[string]map[string]struct{}
}

func newRuntimeInventoryIndex() *runtimeInventoryIndex {
	return &runtimeInventoryIndex{
		containers: make(map[string]map[string]struct{}),
		images:     make(map[string]map[string]struct{}),
	}
}

func (index *runtimeInventoryIndex) ensureNamespace(namespaceID string) {
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.containers[namespaceID] == nil {
		index.containers[namespaceID] = make(map[string]struct{})
	}
	if index.images[namespaceID] == nil {
		index.images[namespaceID] = make(map[string]struct{})
	}
}

func (index *runtimeInventoryIndex) hasNamespace(namespaceID string) bool {
	index.mu.RLock()
	defer index.mu.RUnlock()
	_, containersOK := index.containers[namespaceID]
	_, imagesOK := index.images[namespaceID]
	return containersOK && imagesOK
}

func (index *runtimeInventoryIndex) addContainer(namespaceID, containerID string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.containers[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	values[containerID] = struct{}{}
	return nil
}

func (index *runtimeInventoryIndex) removeContainer(namespaceID, containerID string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.containers[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if _, ok := values[containerID]; !ok {
		return fmt.Errorf("runtime container %q/%q is not indexed", namespaceID, containerID)
	}
	delete(values, containerID)
	return nil
}

func (index *runtimeInventoryIndex) addImage(namespaceID, reference string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.images[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	values[reference] = struct{}{}
	return nil
}

func (index *runtimeInventoryIndex) removeImage(namespaceID, reference string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.images[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if _, ok := values[reference]; !ok {
		return fmt.Errorf("runtime image %q/%q is not indexed", namespaceID, reference)
	}
	delete(values, reference)
	return nil
}

func (index *runtimeInventoryIndex) removeNamespace(namespaceID string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	containers, containersOK := index.containers[namespaceID]
	images, imagesOK := index.images[namespaceID]
	if !containersOK || !imagesOK {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if len(containers) != 0 || len(images) != 0 {
		return fmt.Errorf("runtime inventory namespace %q is not empty", namespaceID)
	}
	delete(index.containers, namespaceID)
	delete(index.images, namespaceID)
	return nil
}

func (index *runtimeInventoryIndex) containerIDs(namespaceID string, limit uint32) ([]string, error) {
	return index.names(namespaceID, limit, index.containers, "container")
}

func (index *runtimeInventoryIndex) imageReferences(namespaceID string, limit uint32) ([]string, error) {
	return index.names(namespaceID, limit, index.images, "image")
}

func (index *runtimeInventoryIndex) names(namespaceID string, limit uint32, inventory map[string]map[string]struct{}, kind string) ([]string, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	values, ok := inventory[namespaceID]
	if !ok {
		return nil, fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if limit > 0 && uint32(len(values)) >= limit {
		return nil, resourceExhausted(kind + " inventory reached the requested limit")
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func (r *Runtime) requireRuntimeInventory() (*runtimeInventoryIndex, error) {
	if r.inventory == nil {
		return nil, errors.New("runtime inventory is unavailable")
	}
	return r.inventory, nil
}

func (r *Runtime) rebuildRuntimeInventory(ctx context.Context) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	knownNamespaces := make(map[string]bool, len(names))
	for _, name := range names {
		if name == runtimeContainerdNamespace {
			continue
		}
		labels, err := r.client.NamespaceService().Labels(ctx, name)
		if err != nil {
			return fmt.Errorf("read runtime inventory namespace %q: %w", name, err)
		}
		if _, _, err := namespaceFromLabels(name, labels); err != nil {
			return fmt.Errorf("validate runtime inventory namespace %q: %w", name, err)
		}
		knownNamespaces[name] = true
	}

	containers, err := r.client.Containers(runtimeContext(ctx))
	if err != nil {
		return err
	}
	containerRecords := make([]corecontainers.Container, 0, len(containers))
	for _, container := range containers {
		info, err := container.Info(runtimeContext(ctx))
		if err != nil {
			return fmt.Errorf("read runtime container %q: %w", container.ID(), err)
		}
		containerRecords = append(containerRecords, info)
	}

	imageRecords := make(map[string][]string, len(knownNamespaces))
	for namespaceID := range knownNamespaces {
		records, err := r.client.ImageService().List(namespaceContext(ctx, namespaceID))
		if err != nil {
			return fmt.Errorf("read runtime images for namespace %q: %w", namespaceID, err)
		}
		for _, record := range records {
			imageRecords[namespaceID] = append(imageRecords[namespaceID], record.Name)
		}
	}
	rebuilt, err := buildRuntimeInventoryIndex(knownNamespaces, containerRecords, imageRecords)
	if err != nil {
		return err
	}
	r.inventory = rebuilt
	return nil
}

func buildRuntimeInventoryIndex(knownNamespaces map[string]bool, containers []corecontainers.Container, images map[string][]string) (*runtimeInventoryIndex, error) {
	rebuilt := newRuntimeInventoryIndex()
	for namespaceID := range knownNamespaces {
		rebuilt.ensureNamespace(namespaceID)
	}
	for _, container := range containers {
		owner, externalID, internal, err := validateRuntimeInventoryContainer(container, knownNamespaces)
		if err != nil {
			return nil, err
		}
		if internal {
			continue
		}
		if err := rebuilt.addContainer(owner, externalID); err != nil {
			return nil, err
		}
	}
	for namespaceID, references := range images {
		if !knownNamespaces[namespaceID] {
			return nil, fmt.Errorf("runtime images name unknown namespace %q", namespaceID)
		}
		for _, reference := range references {
			if reference == "" {
				return nil, fmt.Errorf("runtime image in namespace %q has no name", namespaceID)
			}
			if err := rebuilt.addImage(namespaceID, reference); err != nil {
				return nil, err
			}
		}
	}
	return rebuilt, nil
}

func validateRuntimeInventoryContainer(info corecontainers.Container, knownNamespaces map[string]bool) (owner, externalID string, internal bool, err error) {
	owner = info.Labels[runtimeOwnerLabel]
	externalID = info.Labels[runtimeExternalIDLabel]
	if info.Labels[guestbuild.InternalContainerLabel] == "true" {
		if owner != "" || externalID != "" {
			return "", "", false, fmt.Errorf("internal runtime container %q has workload ownership labels", info.ID)
		}
		return "", "", true, nil
	}
	if owner == "" || externalID == "" {
		return "", "", false, fmt.Errorf("runtime container %q has no complete owner identity", info.ID)
	}
	if !knownNamespaces[owner] {
		return "", "", false, fmt.Errorf("runtime container %q names unknown namespace %q", info.ID, owner)
	}
	if info.ID != runtimeContainerKey(owner, externalID) {
		return "", "", false, fmt.Errorf("runtime container %q does not match owner %q external ID %q", info.ID, owner, externalID)
	}
	return owner, externalID, false, nil
}

func indexedInventoryLoad[T any](identities []string, load func(string) (T, error)) ([]T, error) {
	result := make([]T, 0, len(identities))
	for _, identity := range identities {
		value, err := load(identity)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}
