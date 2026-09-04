//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"golang.org/x/sys/unix"
)

const (
	containerdSocket = "/run/containerd/containerd.sock"
	containerdRoot   = "/state/containerd"
	stateRoot        = "/state"
	runscRuntime     = "io.containerd.runsc.v1"
	ociProjectID     = uint32(9000)
)

type Runtime struct {
	client            *containerd.Client
	network           *guestnetwork.Manager
	builder           *guestbuild.Builder
	stateDevice       string
	mu                sync.Mutex
	waiters           map[string][]chan uint32
	resourceLocks     sync.Map
	volumeUsageMu     sync.Mutex
	volumeUsage       map[string]volumeUsageEntry
	volumeSampling    map[string]bool
	volumeGeneration  map[string]uint64
	volumeSampleSlots chan struct{}
	workspaceSlots    chan struct{}
	diskBudget        *guestdisk.Budget
	materializer      *ocimaterializer.Manager
	volumeSampler     func(string) (uint64, error)
	tasks             sync.WaitGroup
}

func (r *Runtime) lockNamespace(namespaceID string) func() {
	value, _ := r.resourceLocks.LoadOrStore(namespaceID, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func NewRuntime(stateDevice string, network *guestnetwork.Manager, startup OCIStartupState) (*Runtime, error) {
	client, err := containerd.New(containerdSocket, containerd.WithDefaultRuntime(runscRuntime))
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{client: client, network: network, stateDevice: stateDevice,
		waiters: make(map[string][]chan uint32), volumeUsage: make(map[string]volumeUsageEntry), volumeSampling: make(map[string]bool),
		volumeGeneration: make(map[string]uint64), volumeSampleSlots: make(chan struct{}, 2), workspaceSlots: make(chan struct{}, 4),
		volumeSampler: directoryUsage, diskBudget: new(guestdisk.Budget), materializer: new(ocimaterializer.Manager)}
	runtime.builder = guestbuild.NewBuilder(client, network, runtime.guardBuilderOCIGrowth, runtime.guardOCIMetadataGrowth)
	if err := cleanupVolumeStaging(); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean incomplete volumes: %w", err)
	}
	if err := os.RemoveAll(filepath.Join(stateRoot, "imports")); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean incomplete image imports: %w", err)
	}
	if err := os.RemoveAll(filepath.Join(stateRoot, "import-reservations")); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean incomplete import reservations: %w", err)
	}
	if err := runtime.cleanupTasks(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean stale tasks: %w", err)
	}
	if err := runtime.rehydrateNetwork(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("rebuild namespace network: %w", err)
	}
	settleErr := settleOCIStartupQuota(startup,
		func() error { return runtime.settleOCIProjectQuota(context.Background()) },
		func() error { return retryOCIProjectQuota(runtime.tightenOCIProjectQuota, 10, 100*time.Millisecond) })
	if settleErr != nil {
		client.Close()
		return nil, fmt.Errorf("settle OCI startup quota: %w", settleErr)
	}
	return runtime, nil
}

func settleOCIStartupQuota(startup OCIStartupState, settleExisting, tightenFresh func() error) error {
	// Model: spec/tla/GuestOCIStartup.tla. Existing state can carry scheduled
	// containerd GC; fresh state cannot and must not wait on an unready scheduler.
	if startup.Existing {
		return settleExisting()
	}
	// A fresh containerd has no prior records for startup GC. Avoid creating a
	// synchronous GC barrier before its scheduler has initialized.
	return tightenFresh()
}

func (r *Runtime) rehydrateNetwork(ctx context.Context) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		labels, err := r.client.NamespaceService().Labels(ctx, name)
		if err != nil {
			return err
		}
		_, namespace, err := namespaceFromLabels(name, labels)
		if err != nil {
			return err
		}
		if err := r.guardOCIMetadataGrowth(ctx, func() error {
			return r.client.NamespaceService().SetLabel(ctx, name, labelPrefix+"egress", fmt.Sprint(int32(vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL)))
		}); err != nil {
			return err
		}
		if err := r.network.EnsureNamespace(name, namespace.GetSubnet(), vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
			return err
		}
		containers, err := r.client.Containers(namespaceContext(ctx, name))
		if err != nil {
			return err
		}
		for _, container := range containers {
			if err := r.network.CleanupStaleEndpoint(name, container.ID()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runtime) Close() error { return r.client.Close() }

func syncStateFilesystem() error {
	root, err := os.Open(stateRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	return unix.Syncfs(int(root.Fd()))
}

func namespaceContext(ctx context.Context, namespaceID string) context.Context {
	return namespaces.WithNamespace(ctx, namespaceID)
}

func volumePath(namespaceID, volumeID string) string {
	return filepath.Join(stateRoot, "volumes", namespaceID, volumeID)
}

func workspacePath(namespaceID string) string {
	return filepath.Join(stateRoot, "workspaces", namespaceID)
}

func logPath(namespaceID, containerID, generationID string) string {
	return filepath.Join(stateRoot, "logs", namespaceID, containerID, generationID, "records.log")
}

func logMetaPath(namespaceID, containerID, generationID string) string {
	return filepath.Join(stateRoot, "logs", namespaceID, containerID, generationID, "records.meta")
}

func (r *Runtime) namespaceExists(ctx context.Context, namespaceID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	return r.namespaceExistsRaw(ctx, namespaceID)
}

func (r *Runtime) namespaceExistsRaw(ctx context.Context, namespaceID string) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == namespaceID {
			return nil
		}
	}
	return errdefs.ErrNotFound
}

func (r *Runtime) namespaceExistsForRole(ctx context.Context, namespaceID string, system bool) error {
	if system {
		if err := validateSystemNamespace(namespaceID); err != nil {
			return err
		}
		return r.namespaceExistsRaw(ctx, namespaceID)
	}
	return r.namespaceExists(ctx, namespaceID)
}

func ensureDirectory(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state path is not a directory")
	}
	return os.Chmod(path, mode)
}
