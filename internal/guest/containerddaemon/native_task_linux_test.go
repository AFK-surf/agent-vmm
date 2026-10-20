//go:build linux

package containerddaemon

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	namespacesapi "github.com/containerd/containerd/api/services/namespaces/v1"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type nativeNamespaces struct {
	namespacesapi.UnimplementedNamespacesServer
}

func (*nativeNamespaces) List(context.Context, *namespacesapi.ListNamespacesRequest) (*namespacesapi.ListNamespacesResponse, error) {
	return &namespacesapi.ListNamespacesResponse{Namespaces: []*namespacesapi.Namespace{{Name: "runtime"}}}, nil
}

type nativeContainers struct {
	containersapi.UnimplementedContainersServer
}

func (*nativeContainers) List(context.Context, *containersapi.ListContainersRequest) (*containersapi.ListContainersResponse, error) {
	return &containersapi.ListContainersResponse{Containers: []*containersapi.Container{
		{ID: "old-builder", Snapshotter: "native", Labels: map[string]string{namespaceconfig.LabelPrefix + "internal-build": "true"}},
		{ID: "workload", Snapshotter: "overlayfs"},
		{ID: "foreign-native", Snapshotter: "native"},
	}}, nil
}

func (f *nativeContainers) Get(ctx context.Context, r *containersapi.GetContainerRequest) (*containersapi.GetContainerResponse, error) {
	records, _ := f.List(ctx, nil)
	for _, record := range records.Containers {
		if record.ID == r.ID {
			return &containersapi.GetContainerResponse{Container: record}, nil
		}
	}
	return nil, errors.New("missing fixture container")
}

type nativeTasks struct {
	tasksapi.UnimplementedTasksServer
	exited  chan struct{}
	killed  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	deletes []string
	touched []string
}

func (f *nativeTasks) Get(_ context.Context, r *tasksapi.GetRequest) (*tasksapi.GetResponse, error) {
	f.mu.Lock()
	f.touched = append(f.touched, r.ContainerID)
	f.mu.Unlock()
	state := tasktypes.Status_RUNNING
	select {
	case <-f.exited:
		state = tasktypes.Status_STOPPED
	default:
	}
	return &tasksapi.GetResponse{Process: &tasktypes.Process{ID: r.ContainerID, ContainerID: r.ContainerID, Status: state}}, nil
}
func (f *nativeTasks) Wait(ctx context.Context, _ *tasksapi.WaitRequest) (*tasksapi.WaitResponse, error) {
	select {
	case <-f.exited:
		return &tasksapi.WaitResponse{ExitStatus: 137}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (f *nativeTasks) Kill(_ context.Context, r *tasksapi.KillRequest) (*emptypb.Empty, error) {
	f.once.Do(func() { close(f.killed) })
	return &emptypb.Empty{}, nil
}
func (f *nativeTasks) Delete(_ context.Context, r *tasksapi.DeleteTaskRequest) (*tasksapi.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.exited:
	default:
		return nil, errors.New("shim still live")
	}
	f.deletes = append(f.deletes, r.ContainerID)
	return &tasksapi.DeleteResponse{ExitStatus: 137}, nil
}
func TestNativeRetirementWaitsForExactShimExit(t *testing.T) {
	for _, expires := range []bool{false, true} {
		t.Run(map[bool]string{false: "late-exit", true: "live-after-deadline"}[expires], func(t *testing.T) {
			listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "containerd.sock"))
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			fixture := &nativeTasks{exited: make(chan struct{}), killed: make(chan struct{})}
			namespacesapi.RegisterNamespacesServer(server, &nativeNamespaces{})
			containersapi.RegisterContainersServer(server, &nativeContainers{})
			tasksapi.RegisterTasksServer(server, fixture)
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- StopRetiredNativeTasks(ctx, listener.Addr().String()) }()
			select {
			case <-fixture.killed:
			case err := <-done:
				t.Fatalf("retirement failed before kill: %v", err)
			case <-time.After(time.Second):
				t.Fatal("owned task not killed")
			}
			select {
			case err := <-done:
				t.Fatalf("kill was treated as exit: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if expires {
				cancel()
				if err := <-done; err == nil {
					t.Fatal("live shim accepted as stopped")
				}
			} else {
				close(fixture.exited)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if len(fixture.touched) == 0 {
				t.Fatal("owned task not observed")
			}
			for _, id := range fixture.touched {
				if id != "old-builder" {
					t.Fatalf("other task touched: %v", fixture.touched)
				}
			}
			if expires && len(fixture.deletes) != 0 || !expires && (len(fixture.deletes) != 1 || fixture.deletes[0] != "old-builder") {
				t.Fatalf("wrong deletion proof: %v", fixture.deletes)
			}
		})
	}
}
