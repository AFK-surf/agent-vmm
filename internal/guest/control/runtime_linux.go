//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"github.com/AFK-surf/agent-vmm/internal/guest/containerddaemon"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sys/unix"
)

const (
	containerdSocket = "/run/containerd/containerd.sock"
	containerdRoot   = "/control/containerd"
	controlRoot      = "/control"
	stateRoot        = "/state"
	ociProjectID     = uint32(9000)
	cacheProjectID   = uint32(9001)
	runscRuntime     = "io.containerd.runsc.v1"
)

type Runtime struct {
	logger                   *slog.Logger
	hostShareMu              sync.RWMutex
	hostShares               map[string]*vmmdv1.HostShareProjection
	client                   *containerd.Client
	network                  *guestnetwork.Manager
	builder                  *guestbuild.Builder
	stateDevice              string
	controlDevice            string
	mu                       sync.Mutex
	waiters                  map[string][]chan uint32
	resourceLocks            sync.Map
	imageOperations          sync.Map
	imageOperationsClosing   bool
	imageOperationsRecovered bool
	imageOperationWriterID   string
	volumeUsageMu            sync.Mutex
	volumeUsage              map[string]volumeUsageEntry
	volumeSampling           map[string]bool
	volumeGeneration         map[string]uint64
	volumeSampleSlots        chan struct{}
	workspaceSlots           chan struct{}
	diskBudget               *guestdisk.Budget
	materializationSlot      *semaphore.Weighted
	inventory                *runtimeInventoryIndex
	gcMu                     sync.Mutex
	gcRuntimeImageCursor     string
	gcRetainedImageCursor    string
	volumeSampler            func(string) (uint64, error)
	tasks                    sync.WaitGroup
}

func (r *Runtime) lockResource(ctx context.Context, key string) (func(), error) {
	value, _ := r.resourceLocks.LoadOrStore(key, make(chan struct{}, 1))
	lock := value.(chan struct{})
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Runtime) tryLockResource(key string) (func(), bool) {
	value, _ := r.resourceLocks.LoadOrStore(key, make(chan struct{}, 1))
	lock := value.(chan struct{})
	select {
	case lock <- struct{}{}:
		return func() { <-lock }, true
	default:
		return nil, false
	}
}

func (r *Runtime) lockNamespace(ctx context.Context, namespaceID string) (func(), error) {
	return r.lockResource(ctx, "namespace\x00"+namespaceID)
}

func (r *Runtime) lockContainer(ctx context.Context, namespaceID, containerID string) (func(), error) {
	return r.lockResource(ctx, "container\x00"+namespaceID+"/"+containerID)
}

func NewRuntime(stateDevice, controlDevice string, network *guestnetwork.Manager, logger *slog.Logger) (*Runtime, error) {
	if err := initializeResourcePools(); err != nil {
		return nil, fmt.Errorf("initialize resource pools: %w", err)
	}
	client, err := containerd.New(containerdSocket, containerd.WithDefaultRuntime(runscRuntime))
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{logger: logger.With("component", "guest-control"), client: client, network: network, stateDevice: stateDevice, controlDevice: controlDevice,
		waiters: make(map[string][]chan uint32), volumeUsage: make(map[string]volumeUsageEntry), volumeSampling: make(map[string]bool),
		volumeGeneration: make(map[string]uint64), volumeSampleSlots: make(chan struct{}, 2), workspaceSlots: make(chan struct{}, 4),
		inventory: newRuntimeInventoryIndex(), volumeSampler: directoryUsage, diskBudget: new(guestdisk.Budget), materializationSlot: semaphore.NewWeighted(1)}
	writerID, err := containerddaemon.Identity(containerdSocket)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("read containerd execution owner: %w", err)
	}
	runtime.imageOperationWriterID = writerID
	runtime.builder = guestbuild.NewBuilder(client, network, runtime.guardBuilderOCIGrowth)
	if err := ensureRuntimeContainerdNamespace(context.Background(), client); err != nil {
		client.Close()
		return nil, fmt.Errorf("ensure internal runtime namespace: %w", err)
	}

	if err := cleanupVolumeStaging(); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean incomplete volumes: %w", err)
	}
	if err := runtime.cleanupTasks(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean stale tasks: %w", err)
	}
	runtime.imageOperationsRecovered = true

	rebuildContext, cancelRebuild := context.WithTimeout(context.Background(), 30*time.Second)
	if err := runtime.rebuildRuntimeInventory(rebuildContext); err != nil {
		cancelRebuild()
		client.Close()
		return nil, fmt.Errorf("rebuild bounded runtime inventory: %w", err)
	}
	cancelRebuild()
	if err := runtime.rehydrateNetwork(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("rebuild namespace network: %w", err)
	}

	return runtime, nil
}

func (r *Runtime) rehydrateNetwork(ctx context.Context) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == runtimeContainerdNamespace {
			continue
		}
		labels, err := r.client.NamespaceService().Labels(ctx, name)
		if err != nil {
			return err
		}
		projectID, namespace, err := namespaceFromLabels(name, labels)
		if err != nil {
			return err
		}
		if err := setProjectQuota(r.namespaceStateDevice(name), projectID, namespace.GetLimits().GetDiskBytes()); err != nil {
			return fmt.Errorf("restore namespace quota %s: %w", name, err)
		}
		if err := func() error {
			return r.client.NamespaceService().SetLabel(ctx, name, labelPrefix+"egress", fmt.Sprint(int32(vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL)))
		}(); err != nil {
			return err
		}
		if err := r.network.EnsureNamespace(name, namespace.GetSubnet(), vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL); err != nil {
			return err
		}
		containerIDs, err := r.inventory.containerIDs(name, 0)
		if err != nil {
			return err
		}
		for _, containerID := range containerIDs {
			container, err := ownedRuntimeContainer(ctx, r.client, name, containerID)
			if err != nil {
				return fmt.Errorf("load indexed runtime container %s/%s: %w", name, containerID, err)
			}
			task, err := container.Task(runtimeContext(ctx), nil)
			if err == nil {
				status, err := task.Status(runtimeContext(ctx))
				if err != nil {
					return err
				}
				if status.Status != containerd.Stopped {
					labels, err := container.Labels(runtimeContext(ctx))
					if err != nil {
						return err
					}
					if err := r.network.RestoreEndpoint(ctx, name, containerID, labels[containerInstanceLabel]); err != nil {
						return err
					}
					continue
				}
			} else if !errdefs.IsNotFound(err) {
				return err
			}
			if err := r.network.CleanupStaleEndpoint(name, containerID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runtime) Close() error { return r.client.Close() }

func syncStateFilesystem() error {
	for _, path := range []string{stateRoot, controlRoot} {
		if err := syncFilesystem(path); err != nil {
			return err
		}
	}
	return nil
}

func syncFilesystem(path string) error {
	root, err := os.Open(path)
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
	return filepath.Join(namespaceStateRoot(namespaceID), "volumes", namespaceID, volumeID)
}

func volumeGenerationPath(namespaceID, volumeID string) string {
	return filepath.Join(namespaceStateRoot(namespaceID), "volume-generations", namespaceID, volumeID)
}

func workspacePath(namespaceID string) string {
	return filepath.Join(namespaceStateRoot(namespaceID), "workspaces", namespaceID)
}

func logPath(namespaceID, containerID, generationID string) string {
	return filepath.Join(namespaceStateRoot(namespaceID), "logs", namespaceID, containerID, generationID, "records.log")
}

func (r *Runtime) namespaceExists(ctx context.Context, namespaceID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
	return r.namespaceExistsRaw(ctx, namespaceID)
}

func (r *Runtime) namespaceExistsRaw(ctx context.Context, namespaceID string) error {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	if !inventory.hasNamespace(namespaceID) {
		return errdefs.ErrNotFound
	}
	_, err = r.client.NamespaceService().Labels(ctx, namespaceID)
	return err
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

func namespaceStateRoot(namespaceID string) string {
	if isSystemNamespace(namespaceID) {
		return controlRoot
	}
	return stateRoot
}

func (r *Runtime) namespaceStateDevice(namespaceID string) string {
	if isSystemNamespace(namespaceID) {
		return r.controlDevice
	}
	return r.stateDevice
}
