//go:build linux

package build

import (
	"context"
	"errors"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"path/filepath"
	"sync"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
)

type cleanupContainers struct{ containers.Store }

func (cleanupContainers) List(context.Context, ...string) ([]containers.Container, error) {
	return nil, nil
}

type cleanupImages struct {
	images.Store
	namespace, reference string
	synchronous          bool
	failure              error
	calls                int
}

func (store *cleanupImages) Delete(ctx context.Context, name string, options ...images.DeleteOpt) error {
	store.calls++
	store.namespace, _ = namespaces.Namespace(ctx)
	store.reference = name
	var settings images.DeleteOptions
	for _, option := range options {
		if err := option(ctx, &settings); err != nil {
			return err
		}
	}
	store.synchronous = settings.Synchronous
	return store.failure
}

func TestCleanupNamespaceRemovesOnlyInternalBuilderImage(t *testing.T) {
	for _, failure := range []error{nil, errdefs.ErrNotFound, errors.New("GC failed")} {
		store := &cleanupImages{failure: failure}
		client, err := containerd.New("", containerd.WithServices(containerd.WithContainerStore(cleanupContainers{}), containerd.WithImageStore(store)))
		if err != nil {
			t.Fatal(err)
		}
		builder := &Builder{client: client, active: map[string]int{}, namespace: make(map[string]*sync.Mutex)}
		err = builder.CleanupNamespace(context.Background(), "exact-owner")
		if (err != nil) != (failure != nil && !errdefs.IsNotFound(failure)) {
			t.Fatalf("cleanup failure %v: %v", failure, err)
		}
		if store.namespace != "exact-owner" || store.reference != InternalImageReference || !store.synchronous || store.calls != 1 {
			t.Fatalf("cleanup scope %+v", store)
		}
		builder.active["exact-owner"] = 1
		if err := builder.CleanupNamespace(context.Background(), "exact-owner"); err == nil || store.calls != 1 {
			t.Fatalf("active build cleanup: %v", err)
		}
		_ = client.Close()
	}
}

func TestWorkspaceCloseRetainsOwnerUntilTaskCleanupCompletes(t *testing.T) {
	root := t.TempDir()
	builder := &Builder{active: map[string]int{"owner": 1}}
	workspace := &Workspace{builder: builder, header: &vmmdv1.BuildImageHeader{ActorNamespaceId: "owner"}, root: filepath.Join(root, "context"), secretRoot: filepath.Join(root, "secrets"), cleanupPending: true}
	if err := workspace.Close(); err == nil || builder.active["owner"] != 1 {
		t.Fatalf("unconfirmed cleanup released owner: %v active=%v", err, builder.active)
	}
	workspace.cleanupPending = false
	if err := workspace.Close(); err != nil || builder.active["owner"] != 0 {
		t.Fatalf("confirmed cleanup retained owner: %v active=%v", err, builder.active)
	}
}
