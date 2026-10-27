//go:build linux

package control

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDeleteSystemApplianceDataSlowCleanupIsInstallationLocal(t *testing.T) {
	for _, scenario := range []struct{ stage, outcome string }{
		{"containerd", "success"}, {"containerd", "failure"}, {"containerd", "cancel"},
		{"volume", "success"}, {"volume", "failure"},
	} {
		t.Run(scenario.stage+"/"+scenario.outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var blocked atomic.Bool
			pause := func() error {
				if !blocked.CompareAndSwap(false, true) {
					return nil
				}
				close(entered)
				select {
				case <-release:
					if scenario.outcome == "failure" {
						return status.Error(codes.Unavailable, "cleanup fixture failed")
					}
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			var released atomic.Bool
			resume := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			t.Cleanup(resume)
			supervisor, header := systemStreamFixture()
			peer := supervisor.records[header.Identity.InstallationId]
			peer.status.RestartCount = 1
			key := runtimeContainerKey(header.Identity.NamespaceId, header.Identity.ContainerId)
			process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"controller"}})
			if err != nil {
				t.Fatal(err)
			}
			fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{
				key: {ID: key, Spec: &anypb.Any{Value: []byte(`{}`)}, Labels: map[string]string{
					runtimeOwnerLabel: header.Identity.NamespaceId, runtimeExternalIDLabel: header.Identity.ContainerId,
					runtimeImageLabel: "image", containerStateLabel: "running", containerInstanceLabel: header.Identity.ContainerInstanceId,
				}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}},
			}}
			listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "containerd.sock"))
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if scenario.stage == "containerd" && info.FullMethod == "/containerd.services.containers.v1.Containers/List" {
					if err := pause(); err != nil {
						return nil, err
					}
				}
				return handler(ctx, request)
			}))
			containersapi.RegisterContainersServer(server, &systemContainerFixture{fixture: fixture})
			tasksapi.RegisterTasksServer(server, &systemTaskFixture{})
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			client, err := containerd.New(listener.Addr().String(), containerd.WithServices(containerd.WithNamespaceService(&operationNamespaces{labels: map[string]map[string]string{}})))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			runtime := &Runtime{client: client, materializationSlot: semaphore.NewWeighted(1), diskBudget: new(guestdisk.Budget), inventory: newRuntimeInventoryIndex(), waiters: make(map[string][]chan uint32), logger: slog.Default().With("component", "guest-control")}
			supervisor.runtime = runtime
			t.Cleanup(func() {
				supervisor.mu.Lock()
				peer.stopping = true
				supervisor.mu.Unlock()
				fixture.mu.Lock()
				container := proto.Clone(fixture.containers[key]).(*containersapi.Container)
				container.Labels[containerStateLabel] = "stopped"
				fixture.containers[key] = container
				fixture.mu.Unlock()
				runtime.notifyWaiters(header.Identity.NamespaceId+"/"+header.Identity.ContainerId+"/"+header.Identity.ContainerInstanceId, 0)
			})
			supervisor.order = newApplianceSupervisor(runtime, "boot-a").order
			supervisor.order.ReplaceOrInsert(header.Identity.InstallationId)
			supervisor.volumes = newApplianceVolumeManager("")
			supervisor.volumes.root = t.TempDir()
			dataPath := filepath.Join(supervisor.volumes.root, "removed", "volume")
			if err := os.MkdirAll(dataPath, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataPath, "durable"), []byte("keep on failed cleanup"), 0600); err != nil {
				t.Fatal(err)
			}
			supervisor.volumes.project = func(string) (uint32, error) {
				if scenario.stage == "volume" {
					if err := pause(); err != nil {
						return 0, err
					}
				}
				return applianceProjectBase, nil
			}
			supervisor.volumes.limit = func(string, uint32, uint64) error { return nil }
			done := make(chan struct{})
			var result error
			go func() {
				result = supervisor.deleteData(ctx, "removed", "volume")
				close(done)
			}()
			t.Cleanup(func() { resume(); <-done })
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not reach slow consumer")
			}
			management := make(chan error, 1)
			request := &vmmdv1.EnsureSystemApplianceRequest{RequestId: "ensure-removed", Identity: &vmmdv1.SystemApplianceIdentity{
				InstallationId: "removed", BootId: "boot-a", NamespaceId: systemInstallationNamespace("removed"), ContainerId: "payload", ImageManifestDigest: "sha256:image", BootNonce: make([]byte, 32),
			}, ConfigRevision: 1, ImageReference: "image", DataVolumeId: "volume", DataVolumeBytes: 1, Limits: &vmmdv1.NamespaceLimits{}, EgressMode: vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}
			go func() {
				if _, err := supervisor.get(header.Identity.InstallationId); err != nil {
					management <- err
					return
				}
				if appliances, _ := supervisor.list("", 32); len(appliances) != 2 {
					management <- fmt.Errorf("list during cleanup: %v", appliances)
					return
				}
				_, _, releaseStream, err := supervisor.acquireStream(header)
				if err != nil {
					management <- err
					return
				}
				releaseStream()
				deleting, err := supervisor.get("removed")
				if err != nil || deleting.GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED || deleting.GetStatus() != "deleting data" {
					management <- fmt.Errorf("deletion state = %v %v", deleting, err)
					return
				}
				supervisor.mu.Lock()
				deletingRecord := supervisor.records["removed"]
				supervisor.mu.Unlock()
				if deletingRecord.cleanupMu.TryLock() {
					deletingRecord.cleanupMu.Unlock()
					management <- fmt.Errorf("shutdown can release the deletion record before cleanup")
					return
				}
				stale := cloneSystemIdentity(request.Identity)
				stale.ContainerInstanceId = "previous-instance"
				if _, err := supervisor.stop(ctx, &vmmdv1.StopSystemApplianceRequest{RequestId: "stale-stop", Identity: stale}); !errdefs.IsNotFound(err) {
					management <- fmt.Errorf("stale stop accepted deletion record: %v", err)
					return
				}
				if _, err := supervisor.ensure(ctx, request); err == nil || !strings.Contains(err.Error(), "different system appliance launch") {
					management <- fmt.Errorf("deleting installation escaped launch fence: %v", err)
					return
				}
				management <- nil
			}()
			select {
			case err := <-management:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("cleanup blocked unrelated management or same-installation fencing")
			}
			restarted := make(chan struct{})
			go func() {
				supervisor.restartAfterBackoff(header.Identity.InstallationId, header.Identity.NamespaceId, header.Identity.ContainerId, header.Identity.ContainerInstanceId, 1)
				close(restarted)
			}()
			select {
			case <-restarted:
			case <-time.After(3 * time.Second):
				t.Fatal("cleanup blocked unrelated restart")
			}
			if current, err := supervisor.get(header.Identity.InstallationId); err != nil || current.GetStatus() != "running after restart" {
				t.Fatalf("unrelated restart = %v %v", current, err)
			}
			// Finish the observer created by the successful restart.
			waitKey := header.Identity.NamespaceId + "/" + header.Identity.ContainerId + "/" + header.Identity.ContainerInstanceId
			deadline := time.Now().Add(time.Second)
			for {
				runtime.mu.Lock()
				observing := len(runtime.waiters[waitKey]) != 0
				runtime.mu.Unlock()
				if observing {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("restart observer did not attach")
				}
				time.Sleep(time.Millisecond)
			}
			supervisor.mu.Lock()
			peer.stopping = true
			supervisor.mu.Unlock()
			runtime.notifyWaiters(waitKey, 0)
			if scenario.outcome == "cancel" {
				cancel()
			} else {
				resume()
			}
			<-done
			if (result == nil) != (scenario.outcome == "success") {
				t.Fatalf("cleanup outcome %s: %v", scenario.outcome, result)
			}
			if _, err := supervisor.get("removed"); err == nil {
				t.Fatal("cleanup retained installation fence")
			}
			if scenario.outcome != "success" {
				if _, err := os.Stat(filepath.Join(dataPath, "durable")); err != nil {
					t.Fatal("failed cleanup deleted data", err)
				}
				if err := supervisor.deleteData(context.Background(), "removed", "volume"); err != nil {
					t.Fatalf("retry: %v", err)
				}
			}
			if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
				t.Fatalf("successful cleanup retained data: %v", err)
			}
			if _, err := supervisor.ensure(context.Background(), request); err == nil || !strings.Contains(err.Error(), "data volume size is invalid") {
				t.Fatalf("cleanup fence survived into later launch: %v", err)
			}
		})
	}
}
