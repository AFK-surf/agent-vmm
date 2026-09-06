//go:build linux

package control

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
	containerd "github.com/containerd/containerd/v2/client"
	corecontent "github.com/containerd/containerd/v2/core/content"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	maxGCNamespaces   = 128
	maxGCContentFacts = 4096
	maxGCDeletes      = 32
)

type contentCandidate struct {
	namespace string
	digest    digest.Digest
	updated   time.Time
}

type snapshotCandidate struct {
	namespace string
	name      string
	updated   time.Time
}

// reclaimOCI is the sole ref-safe cache GC owner. It derives references from
// containerd image, container, lease, and snapshot facts on every bounded pass;
// it never maintains a second refcount mirror or deletes namespace records.
// Model anchor: spec/tla/OCIContentLifecycle.tla reference-fenced GC.
func (r *Runtime) reclaimOCI(ctx context.Context, limit int) (int, error) {
	return r.reclaimOCIWithMaterializer(ctx, limit, false)
}

// reclaimOCIWithMaterializer keeps the global materializer as the only
// import/GC exclusion owner. Import calls it with materializerHeld=true while
// reserving space, so the capacity-pressure path cannot recursively lock the
// same global slot.
func (r *Runtime) reclaimOCIWithMaterializer(ctx context.Context, limit int, materializerHeld bool) (deleted int, resultErr error) {
	return r.reclaimOCIWithMaterializerTransaction(ctx, limit, materializerHeld, nil)
}

func (r *Runtime) reclaimOCIWithMaterializerTransaction(ctx context.Context, limit int, materializerHeld bool, transaction *guestdisk.Transaction) (deleted int, resultErr error) {
	defer func() {
		settle := func() error { return r.settleOCIProjectQuota(ctx) }
		if transaction != nil {
			resultErr = errors.Join(resultErr, transaction.Settle(settle))
		} else {
			resultErr = errors.Join(resultErr, r.diskBudget.Settle(settle))
		}
	}()
	if limit <= 0 || limit > maxGCDeletes {
		limit = maxGCDeletes
	}
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return 0, err
	}
	if len(names) > maxGCNamespaces {
		return 0, errors.New("OCI GC namespace bound exceeded")
	}
	pruned, err := r.pruneUnreferencedRuntimeImages(ctx, names, limit, materializerHeld)
	if err != nil {
		return 0, err
	}
	deleted += pruned
	snapshotReferences, snapshotCandidates, err := r.ociSnapshotFacts(ctx, names)
	if err != nil {
		return 0, err
	}
	sort.Slice(snapshotCandidates, func(i, j int) bool { return snapshotCandidates[i].updated.Before(snapshotCandidates[j].updated) })
	for _, candidate := range snapshotCandidates {
		if deleted >= limit || snapshotReferences[snapshotFactKey(candidate.namespace, candidate.name)] {
			continue
		}
		unlock := func() {}
		if !materializerHeld {
			unlock = r.materializer.Lock("snapshot:" + snapshotFactKey(candidate.namespace, candidate.name))
		}
		currentReferences, _, checkErr := r.ociSnapshotFacts(ctx, []string{candidate.namespace})
		if checkErr == nil && !currentReferences[snapshotFactKey(candidate.namespace, candidate.name)] {
			checkErr = r.client.SnapshotService(containerSnapshotter).Remove(namespaceContext(ctx, candidate.namespace), candidate.name)
			if errdefs.IsNotFound(checkErr) || errdefs.IsFailedPrecondition(checkErr) {
				checkErr = nil
			} else if checkErr == nil {
				deleted++
			}
		}
		unlock()
		if checkErr != nil {
			return deleted, checkErr
		}
	}
	references, candidates, err := r.ociFacts(ctx, names)
	if err != nil {
		return 0, err
	}
	facts := make([]ocimaterializer.ContentFact, 0, len(candidates))
	refStrings := make(map[string]bool, len(references))
	for value := range references {
		refStrings[value.String()] = true
	}
	for _, candidate := range candidates {
		facts = append(facts, ocimaterializer.ContentFact{Digest: candidate.digest.String(), Namespace: candidate.namespace, UpdatedAt: candidate.updated})
	}
	selected := ocimaterializer.SelectUnreferenced(facts, refStrings, limit)
	for _, fact := range selected {
		if deleted >= limit {
			break
		}
		candidate := contentCandidate{namespace: fact.Namespace, digest: digest.Digest(fact.Digest), updated: fact.UpdatedAt}
		unlock := func() {}
		if !materializerHeld {
			unlock = r.materializer.Lock(candidate.digest.String())
		}
		referenced, checkErr := r.ociDigestReferenced(ctx, names, candidate.digest)
		if checkErr == nil && !referenced {
			nsctx := namespaceContext(ctx, candidate.namespace)
			checkErr = r.client.ContentStore().Delete(nsctx, candidate.digest)
			if errdefs.IsNotFound(checkErr) {
				checkErr = nil
			}
			if checkErr == nil {
				deleted++
			}
		}
		unlock()
		if checkErr != nil {
			return deleted, checkErr
		}
	}
	return deleted, nil
}

// Logical namespace image records are authorization/retention grants. The
// internal runtime image record owns the one physical unpacked lower tree and
// may be removed only after the last logical grant and container reference are
// gone. OCIContentLifecycle models these records as retainedRefs/activeRefs.
func (r *Runtime) pruneUnreferencedRuntimeImages(ctx context.Context, names []string, limit int, materializerHeld bool) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	images, err := r.client.ImageService().List(runtimeContext(ctx))
	if err != nil {
		return 0, err
	}
	sort.Slice(images, func(i, j int) bool { return images[i].UpdatedAt.Before(images[j].UpdatedAt) })
	deleted := 0
	for _, image := range images {
		if deleted >= limit {
			break
		}
		retained, err := r.runtimeImageRetained(ctx, names, image.Name, image.Target.Digest)
		if err != nil {
			return deleted, err
		}
		if retained {
			continue
		}
		unlock := func() {}
		if !materializerHeld {
			unlock = r.materializer.Lock(image.Target.Digest.String())
		}
		retained, err = r.runtimeImageRetained(ctx, names, image.Name, image.Target.Digest)
		if err == nil && !retained {
			err = r.client.ImageService().Delete(runtimeContext(ctx), image.Name, coreimages.SynchronousDelete())
			if errdefs.IsNotFound(err) {
				err = nil
			} else if err == nil {
				deleted++
			}
		}
		unlock()
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (r *Runtime) runtimeImageRetained(ctx context.Context, names []string, reference string, wanted digest.Digest) (bool, error) {
	for _, name := range names {
		if name == runtimeContainerdNamespace {
			continue
		}
		records, err := r.client.ImageService().List(namespaceContext(ctx, name))
		if err != nil {
			return false, err
		}
		for _, record := range records {
			if record.Target.Digest == wanted {
				return true, nil
			}
		}
	}
	containers, err := r.client.Containers(runtimeContext(ctx))
	if err != nil {
		return false, err
	}
	for _, container := range containers {
		info, err := container.Info(runtimeContext(ctx))
		if err != nil {
			return false, err
		}
		if info.Image == reference {
			return true, nil
		}
	}
	return false, nil
}

func snapshotFactKey(namespace, name string) string { return namespace + "\x00" + name }

func (r *Runtime) ociSnapshotFacts(ctx context.Context, names []string) (map[string]bool, []snapshotCandidate, error) {
	references := make(map[string]bool)
	parents := make(map[string]string)
	candidates := make([]snapshotCandidate, 0)
	for _, name := range names {
		nsctx := namespaceContext(ctx, name)
		images, err := r.client.ImageService().List(nsctx)
		if err != nil {
			return nil, nil, err
		}
		for _, record := range images {
			diffs, err := containerd.NewImage(r.client, record).RootFS(nsctx)
			if err != nil && !errdefs.IsNotFound(err) {
				return nil, nil, err
			}
			if len(diffs) != 0 {
				references[snapshotFactKey(name, identity.ChainID(diffs).String())] = true
			}
		}
		containers, err := r.client.Containers(nsctx)
		if err != nil {
			return nil, nil, err
		}
		for _, container := range containers {
			info, err := container.Info(nsctx)
			if err != nil {
				return nil, nil, err
			}
			if info.Snapshotter == containerSnapshotter && info.SnapshotKey != "" {
				references[snapshotFactKey(name, info.SnapshotKey)] = true
			}
		}
		leases, err := r.client.LeasesService().List(nsctx)
		if err != nil {
			return nil, nil, err
		}
		for _, lease := range leases {
			resources, err := r.client.LeasesService().ListResources(nsctx, lease)
			if err != nil {
				return nil, nil, err
			}
			for _, resource := range resources {
				if resource.Type == "snapshots/"+containerSnapshotter {
					references[snapshotFactKey(name, resource.ID)] = true
				}
			}
		}
		if err := r.client.SnapshotService(containerSnapshotter).Walk(nsctx, func(_ context.Context, info snapshots.Info) error {
			if len(candidates) >= maxGCContentFacts {
				return errors.New("OCI GC snapshot fact bound exceeded")
			}
			key := snapshotFactKey(name, info.Name)
			if info.Parent != "" {
				parents[key] = snapshotFactKey(name, info.Parent)
			}
			candidates = append(candidates, snapshotCandidate{namespace: name, name: info.Name, updated: info.Updated})
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return ocimaterializer.SnapshotReferenceClosure(references, parents), candidates, nil
}

func (r *Runtime) ociFacts(ctx context.Context, names []string) (map[digest.Digest]bool, []contentCandidate, error) {
	references := make(map[digest.Digest]bool)
	for _, name := range names {
		nsctx := namespaceContext(ctx, name)
		images, err := r.client.ImageService().List(nsctx)
		if err != nil {
			return nil, nil, err
		}
		for _, image := range images {
			if err := markImageGraph(nsctx, r.client.ContentStore(), image.Target, references); err != nil && !errdefs.IsNotFound(err) {
				return nil, nil, err
			}
		}
		leases, err := r.client.LeasesService().List(nsctx)
		if err != nil {
			return nil, nil, err
		}
		for _, lease := range leases {
			resources, err := r.client.LeasesService().ListResources(nsctx, lease)
			if err != nil {
				return nil, nil, err
			}
			for _, resource := range resources {
				if resource.Type == "content" {
					if value, err := digest.Parse(resource.ID); err == nil {
						references[value] = true
					}
				}
			}
		}
	}
	contentNamespace := "agent-vmm-global-gc"
	if len(names) != 0 {
		contentNamespace = names[0]
	}
	candidates := make([]contentCandidate, 0)
	seen := make(map[digest.Digest]bool)
	err := r.client.ContentStore().Walk(namespaceContext(ctx, contentNamespace), func(info corecontent.Info) error {
		if seen[info.Digest] {
			return nil
		}
		if len(candidates) >= maxGCContentFacts {
			return errors.New("OCI GC content fact bound exceeded")
		}
		seen[info.Digest] = true
		candidates = append(candidates, contentCandidate{namespace: contentNamespace, digest: info.Digest, updated: info.UpdatedAt})
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return references, candidates, nil
}

func markImageGraph(ctx context.Context, store corecontent.Store, root ocispec.Descriptor, references map[digest.Digest]bool) error {
	var referencesMu sync.Mutex
	handler := coreimages.Handlers(coreimages.HandlerFunc(func(_ context.Context, descriptor ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		referencesMu.Lock()
		references[descriptor.Digest] = true
		referencesMu.Unlock()
		return nil, nil
	}), coreimages.ChildrenHandler(store))
	return coreimages.Dispatch(ctx, handler, nil, root)
}

func (r *Runtime) ociDigestReferenced(ctx context.Context, names []string, wanted digest.Digest) (bool, error) {
	for _, name := range names {
		nsctx := namespaceContext(ctx, name)
		images, err := r.client.ImageService().List(nsctx)
		if err != nil {
			return false, err
		}
		refs := make(map[digest.Digest]bool)
		for _, image := range images {
			if err := markImageGraph(nsctx, r.client.ContentStore(), image.Target, refs); err != nil && !errdefs.IsNotFound(err) {
				return false, err
			}
			if refs[wanted] {
				return true, nil
			}
		}
		leases, err := r.client.LeasesService().List(nsctx)
		if err != nil {
			return false, err
		}
		for _, lease := range leases {
			resources, err := r.client.LeasesService().ListResources(nsctx, lease)
			if err != nil {
				return false, err
			}
			for _, resource := range resources {
				if resource.Type == "content" && resource.ID == wanted.String() {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
