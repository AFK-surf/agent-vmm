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
	"github.com/google/btree"
)

// runtimeInventoryIndex is a rebuildable, in-memory projection of
// containerd-owned records. It lets request paths enumerate one owner without
// scanning the shared runtime containerd namespace. containerd remains the
// durable authority; NewRuntime rebuilds this index before VMMD is available.
type runtimeInventoryIndex struct {
	mu              sync.RWMutex
	containers      map[string]*btree.BTreeG[string]
	images          map[string]map[string]string
	imageDigestRefs map[string]uint64
	runtimeImages   *btree.BTreeG[string]
}

func newRuntimeInventoryIndex() *runtimeInventoryIndex {
	return &runtimeInventoryIndex{
		containers:      make(map[string]*btree.BTreeG[string]),
		images:          make(map[string]map[string]string),
		imageDigestRefs: make(map[string]uint64),
		runtimeImages:   btree.NewG(8, func(left, right string) bool { return left < right }),
	}
}

func (index *runtimeInventoryIndex) addRuntimeImage(reference string) {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.runtimeImages.ReplaceOrInsert(reference)
}

func (index *runtimeInventoryIndex) removeRuntimeImage(reference string) {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.runtimeImages.Delete(reference)
}

func (index *runtimeInventoryIndex) runtimeImagePage(after string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	result := make([]string, 0, limit)
	index.runtimeImages.AscendGreaterOrEqual(after, func(value string) bool {
		if value == after {
			return true
		}
		result = append(result, value)
		return len(result) < limit
	})
	return result
}

func (index *runtimeInventoryIndex) ensureNamespace(namespaceID string) {
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.containers[namespaceID] == nil {
		index.containers[namespaceID] = btree.NewG(8, func(left, right string) bool { return left < right })
	}
	if index.images[namespaceID] == nil {
		index.images[namespaceID] = make(map[string]string)
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
	values.ReplaceOrInsert(containerID)
	return nil
}

func (index *runtimeInventoryIndex) removeContainer(namespaceID, containerID string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.containers[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if _, ok := values.Get(containerID); !ok {
		return fmt.Errorf("runtime container %q/%q is not indexed", namespaceID, containerID)
	}
	values.Delete(containerID)
	return nil
}

func (index *runtimeInventoryIndex) addImage(namespaceID, reference, targetDigest string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.images[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if targetDigest == "" {
		return fmt.Errorf("runtime image %q/%q has no target digest", namespaceID, reference)
	}
	if current, exists := values[reference]; exists {
		if current == targetDigest {
			return nil
		}
		if index.imageDigestRefs[current] <= 1 {
			delete(index.imageDigestRefs, current)
		} else {
			index.imageDigestRefs[current]--
		}
	}
	values[reference] = targetDigest
	index.imageDigestRefs[targetDigest]++
	return nil
}

func (index *runtimeInventoryIndex) removeImage(namespaceID, reference string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	values, ok := index.images[namespaceID]
	if !ok {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	targetDigest, ok := values[reference]
	if !ok {
		return fmt.Errorf("runtime image %q/%q is not indexed", namespaceID, reference)
	}
	delete(values, reference)
	if index.imageDigestRefs[targetDigest] <= 1 {
		delete(index.imageDigestRefs, targetDigest)
	} else {
		index.imageDigestRefs[targetDigest]--
	}
	return nil
}

func (index *runtimeInventoryIndex) imageDigestReferenced(targetDigest string) bool {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.imageDigestRefs[targetDigest] != 0
}

func (index *runtimeInventoryIndex) removeNamespace(namespaceID string) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	containers, containersOK := index.containers[namespaceID]
	images, imagesOK := index.images[namespaceID]
	if !containersOK || !imagesOK {
		return fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if containers.Len() != 0 || len(images) != 0 {
		return fmt.Errorf("runtime inventory namespace %q is not empty", namespaceID)
	}
	delete(index.containers, namespaceID)
	delete(index.images, namespaceID)
	return nil
}

func (index *runtimeInventoryIndex) containerIDs(namespaceID string, limit uint32) ([]string, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	values, ok := index.containers[namespaceID]
	if !ok {
		return nil, fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if limit > 0 && uint32(values.Len()) >= limit {
		return nil, resourceExhausted("container inventory reached the requested limit")
	}
	result := make([]string, 0, values.Len())
	values.Ascend(func(id string) bool { result = append(result, id); return true })
	return result, nil
}

func (index *runtimeInventoryIndex) containerPage(namespaceID, after string, limit int) ([]string, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	values, ok := index.containers[namespaceID]
	if !ok {
		return nil, fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	result := make([]string, 0, limit)
	values.AscendGreaterOrEqual(after, func(id string) bool {
		if id > after {
			result = append(result, id)
		}
		return len(result) < limit
	})
	return result, nil
}

func (index *runtimeInventoryIndex) imageReferences(namespaceID string, limit uint32) ([]string, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	values, ok := index.images[namespaceID]
	if !ok {
		return nil, fmt.Errorf("runtime inventory namespace %q is not indexed", namespaceID)
	}
	if limit > 0 && uint32(len(values)) >= limit {
		return nil, resourceExhausted("image inventory reached the requested limit")
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
		if err := convertContainerMountLabel(runtimeContext(ctx), container, info.Labels); err != nil {
			return fmt.Errorf("convert container mount label %q: %w", container.ID(), err)
		}
		containerRecords = append(containerRecords, info)
	}

	imageRecords := make(map[string]map[string]string, len(knownNamespaces))
	for namespaceID := range knownNamespaces {
		records, err := r.client.ImageService().List(namespaceContext(ctx, namespaceID))
		if err != nil {
			return fmt.Errorf("read runtime images for namespace %q: %w", namespaceID, err)
		}
		for _, record := range records {
			if imageRecords[namespaceID] == nil {
				imageRecords[namespaceID] = make(map[string]string)
			}
			imageRecords[namespaceID][record.Name] = record.Target.Digest.String()
		}
	}
	rebuilt, err := buildRuntimeInventoryIndex(knownNamespaces, containerRecords, imageRecords)
	if err != nil {
		return err
	}
	runtimeImages, err := r.client.ImageService().List(runtimeContext(ctx))
	if err != nil {
		return fmt.Errorf("read internal runtime images: %w", err)
	}
	for _, record := range runtimeImages {
		if scope := record.Labels[retainedScopeLabel]; scope != "" {
			rebuilt.addRuntimeImage(record.Name)
			owner := retainedImageOwner(scope)
			rebuilt.ensureNamespace(owner)
			if err := rebuilt.addImage(owner, record.Name, record.Target.Digest.String()); err != nil {
				return err
			}
			continue
		}
		rebuilt.addRuntimeImage(record.Name)
	}
	r.inventory = rebuilt
	return nil
}

func buildRuntimeInventoryIndex(knownNamespaces map[string]bool, containers []corecontainers.Container, images map[string]map[string]string) (*runtimeInventoryIndex, error) {
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
		for reference, targetDigest := range references {
			if reference == guestbuild.InternalImageReference {
				continue
			}
			if reference == "" {
				return nil, fmt.Errorf("runtime image in namespace %q has no name", namespaceID)
			}
			if err := rebuilt.addImage(namespaceID, reference, targetDigest); err != nil {
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
