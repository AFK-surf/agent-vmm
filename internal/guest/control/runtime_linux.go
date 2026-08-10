//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
)

const (
	containerdSocket = "/run/containerd/containerd.sock"
	stateRoot        = "/state"
	runscRuntime     = "io.containerd.runsc.v1"
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
	volumeSampler     func(string) (uint64, error)
	tasks             sync.WaitGroup
}

func (r *Runtime) lockNamespace(namespaceID string) func() {
	value, _ := r.resourceLocks.LoadOrStore(namespaceID, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func NewRuntime(stateDevice string, network *guestnetwork.Manager) (*Runtime, error) {
	client, err := containerd.New(containerdSocket, containerd.WithDefaultRuntime(runscRuntime))
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{client: client, network: network, builder: guestbuild.NewBuilder(client, network), stateDevice: stateDevice, waiters: make(map[string][]chan uint32), volumeUsage: make(map[string]volumeUsageEntry), volumeSampling: make(map[string]bool), volumeGeneration: make(map[string]uint64), volumeSampleSlots: make(chan struct{}, 2), volumeSampler: directoryUsage}
	if err := cleanupVolumeStaging(); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean incomplete volumes: %w", err)
	}
	if err := runtime.cleanupTasks(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("clean stale tasks: %w", err)
	}
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
		labels, err := r.client.NamespaceService().Labels(ctx, name)
		if err != nil {
			return err
		}
		_, namespace, err := namespaceFromLabels(name, labels)
		if err != nil {
			return err
		}
		if err := r.client.NamespaceService().SetLabel(ctx, name, labelPrefix+"egress", fmt.Sprint(int32(vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL))); err != nil {
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

func namespaceContext(ctx context.Context, namespaceID string) context.Context {
	return namespaces.WithNamespace(ctx, namespaceID)
}

func volumePath(namespaceID, volumeID string) string {
	return filepath.Join(stateRoot, "volumes", namespaceID, volumeID)
}

func logPath(namespaceID, containerID string) string {
	return filepath.Join(stateRoot, "logs", namespaceID, containerID, "records.log")
}

func logMetaPath(namespaceID, containerID string) string {
	return filepath.Join(stateRoot, "logs", namespaceID, containerID, "records.meta")
}

func (r *Runtime) namespaceExists(ctx context.Context, namespaceID string) error {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return err
	}
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
