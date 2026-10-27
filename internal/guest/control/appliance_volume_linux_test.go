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
	"syscall"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	containerd "github.com/containerd/containerd/v2/client"
)

func TestApplianceDataPersistsWithIndependentQuotaAndExactDeletion(t *testing.T) {
	requireGuestRoot(t)
	manager := newApplianceVolumeManager("pool")
	manager.root = t.TempDir()
	projects := make(map[uint64]uint32)
	inode := func(path string) uint64 {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Sys().(*syscall.Stat_t).Ino
	}
	limits := make(map[uint32]uint64)
	manager.project = func(path string) (uint32, error) { return projects[inode(path)], nil }
	manager.assign = func(path string, project uint32) error { projects[inode(path)] = project; return nil }
	manager.limit = func(device string, project uint32, size uint64) error {
		if device != "pool" {
			t.Fatal("wrong device")
		}
		limits[project] = size
		return nil
	}
	first, err := manager.prepare("controller-a", "volume-a", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(first.mountPath, "workflow")
	if err := os.WriteFile(marker, []byte("durable"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := manager.prepare("controller-b", "volume-a", 2<<30)
	if err != nil {
		t.Fatal(err)
	}
	replacement := manager
	replacement.mu = new(sync.Mutex)
	again, err := replacement.prepare("controller-a", "volume-a", 1<<30)
	if err != nil || again.mountPath != first.mountPath {
		t.Fatalf("replacement: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("lost persistent data")
	}
	a, b := projects[inode(first.mountPath)], projects[inode(second.mountPath)]
	if a == b || limits[a] != 1<<30 || limits[b] != 2<<30 {
		t.Fatal("quota isolation lost")
	}
	info, err := os.Stat(first.mountPath)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("private mode lost")
	}
	if err := manager.delete("controller-a", "volume-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first.mountPath); !os.IsNotExist(err) {
		t.Fatal("exact volume remains")
	}
	if _, err := os.Stat(second.mountPath); err != nil {
		t.Fatal("foreign volume removed")
	}
	if limits[a] != 0 || limits[b] != 2<<30 {
		t.Fatal("foreign quota changed")
	}
}

func TestApplianceDataRejectsSymlinkAndInvalidIdentity(t *testing.T) {
	manager := newApplianceVolumeManager("pool")
	manager.root = t.TempDir()
	if _, err := manager.prepare("../foreign", "volume-a", 1<<30); err == nil {
		t.Fatal("accepted invalid owner")
	}
	owner := filepath.Join(manager.root, "controller-a")
	if err := os.Mkdir(owner, 0700); err != nil {
		t.Fatal(err)
	}
	foreign := t.TempDir()
	if err := os.Symlink(foreign, filepath.Join(owner, "volume-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.prepare("controller-a", "volume-a", 1<<30); err == nil {
		t.Fatal("accepted symlink")
	}
	if err := manager.delete("controller-a", "volume-a"); err == nil {
		t.Fatal("deleted symlink scope")
	}
}

func TestApplianceDataPublishesOnlyInitializedDirectory(t *testing.T) {
	requireGuestRoot(t)
	manager := newApplianceVolumeManager("pool")
	manager.root = t.TempDir()
	assigned := false
	manager.project = func(path string) (uint32, error) { return applianceProjectBase, nil }
	manager.assign = func(path string, project uint32) error {
		if filepath.Base(path) == "volume-a" {
			t.Fatal("assigned visible unfinished volume")
		}
		assigned = true
		return nil
	}
	fail := true
	manager.limit = func(string, uint32, uint64) error {
		if !assigned {
			t.Fatal("quota before assignment")
		}
		if fail {
			return errors.New("quota unavailable")
		}
		return nil
	}
	path := filepath.Join(manager.root, "controller-a", "volume-a")
	if _, err := manager.prepare("controller-a", "volume-a", 1<<30); err == nil {
		t.Fatal("published failed initialization")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed volume visible")
	}
	owner := filepath.Dir(path)
	stale := filepath.Join(owner, applianceStagingPrefix+"crashed")
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	fail = false
	if _, err := manager.prepare("controller-a", "volume-a", 1<<30); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale private staging retained")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestApplianceQuotaProjectsStartAfterEntireNamespaceRange(t *testing.T) {
	maximumNamespaceProject := namespaceProjectBase + namespaceProjectCount - 1
	if applianceProjectBase <= maximumNamespaceProject || applianceProjectBase <= ociProjectID {
		t.Fatal("appliance projects overlap namespace or OCI quotas")
	}
	manager := newApplianceVolumeManager("pool")
	manager.root = t.TempDir()
	manager.project = func(string) (uint32, error) { return maximumNamespaceProject, nil }
	touched := false
	manager.limit = func(string, uint32, uint64) error { touched = true; return nil }
	owner := filepath.Join(manager.root, "controller-a")
	path := filepath.Join(owner, "volume-a")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.prepare("controller-a", "volume-a", 1<<30); err == nil {
		t.Fatal("accepted namespace quota owner")
	}
	if err := manager.delete("controller-a", "volume-a"); err == nil {
		t.Fatal("deleted namespace quota owner")
	}
	if touched {
		t.Fatal("changed foreign quota")
	}
}

func TestApplianceAndWorkloadQuotaAdmissionSharesPoolInEitherOrder(t *testing.T) {
	device := os.Getenv("AGENT_VMM_BUILDER_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires a disposable /state ext4 project-quota mount")
	}
	if err := PrepareOCIStorage(device); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for index, workloadFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("workload-first-%t", workloadFirst), func(t *testing.T) {
			facts := &operationNamespaces{labels: make(map[string]map[string]string)}
			client, err := containerd.New("", containerd.WithServices(containerd.WithNamespaceService(facts)))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			runtime := &Runtime{client: client, stateDevice: device, diskBudget: new(guestdisk.Budget), logger: slog.Default().With("component", "guest-control")}
			supervisor := &applianceSupervisor{runtime: runtime, volumes: newApplianceVolumeManager(device)}
			snapshot, err := runtime.guestDiskSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			available, ok := snapshot.AvailableBytes()
			if !ok || available < 2<<30 || available > 120<<30 {
				t.Fatalf("fixture needs 2..120 GiB available: %d, %t", available, ok)
			}
			requested := (available/2 + (1 << 30)) &^ 1023
			project := uint32(19000 + index)
			namespace := fmt.Sprintf("fg04-workload-%d", index)
			path := volumePath(namespace, "")
			defer os.RemoveAll(path)
			defer setProjectQuota(device, project, 0)
			defer supervisor.volumes.delete("fg04-appliance", "data")
			workload := func() error {
				return runtime.diskBudget.MutateInventory(func(*guestdisk.Transaction) error {
					if err := runtime.admitNamespaceDiskGrowth(ctx, namespace, requested, nil); err != nil {
						return err
					}
					if err := os.MkdirAll(path, 0700); err != nil {
						return err
					}
					if err := assignProject(path, project); err != nil {
						return err
					}
					if err := setProjectQuota(device, project, requested); err != nil {
						return err
					}
					facts.labels[namespace] = namespaceLabels(&vmmdv1.NamespaceLimits{Pids: 64, DiskBytes: requested}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, project, "10.42.1.0/24")
					return nil
				})
			}
			appliance := func() error {
				_, err := supervisor.prepareData("fg04-appliance", "data", requested)
				return err
			}
			first, second := appliance, workload
			if workloadFirst {
				first, second = workload, appliance
			}
			if err := first(); err != nil {
				t.Fatalf("first admission: %v", err)
			}
			if err := second(); !errors.Is(err, guestdisk.ErrCapacityUnavailable) {
				t.Fatalf("second admission exceeded shared pool: %v", err)
			}
		})
	}
}
