//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type shutdownTask struct {
	command  *exec.Cmd
	exited   chan struct{}
	deleted  bool
	failStop bool
	unknown  bool
}
type shutdownTasks struct {
	tasksapi.UnimplementedTasksServer
	mu    sync.Mutex
	tasks map[string]*shutdownTask
}

func (f *shutdownTasks) Get(_ context.Context, r *tasksapi.GetRequest) (*tasksapi.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task := f.tasks[r.ContainerID]
	if task == nil || task.deleted {
		return nil, status.Error(codes.NotFound, "task ended")
	}
	state := tasktypes.Status_RUNNING
	if task.unknown {
		state = tasktypes.Status_UNKNOWN
	}
	select {
	case <-task.exited:
		state = tasktypes.Status_STOPPED
	default:
	}
	id := r.ExecID
	if id == "" {
		id = r.ContainerID
	}
	return &tasksapi.GetResponse{Process: &tasktypes.Process{ID: id, ContainerID: r.ContainerID, Pid: uint32(task.command.Process.Pid), Status: state}}, nil
}
func (f *shutdownTasks) Wait(ctx context.Context, r *tasksapi.WaitRequest) (*tasksapi.WaitResponse, error) {
	f.mu.Lock()
	task := f.tasks[r.ContainerID]
	f.mu.Unlock()
	select {
	case <-task.exited:
		return &tasksapi.WaitResponse{ExitStatus: 0}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (f *shutdownTasks) Kill(_ context.Context, r *tasksapi.KillRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	task := f.tasks[r.ContainerID]
	failStop := task.failStop
	f.mu.Unlock()
	select {
	case <-task.exited:
		return nil, status.Error(codes.NotFound, "process exited")
	default:
	}
	if failStop {
		return nil, status.Error(codes.PermissionDenied, "writer stop denied")
	}
	if err := task.command.Process.Signal(syscall.Signal(r.Signal)); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
func (f *shutdownTasks) Delete(_ context.Context, r *tasksapi.DeleteTaskRequest) (*tasksapi.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task := f.tasks[r.ContainerID]
	select {
	case <-task.exited:
		task.deleted = true
		return &tasksapi.DeleteResponse{}, nil
	default:
		return nil, status.Error(codes.FailedPrecondition, "writer remains live")
	}
}
func (f *shutdownTasks) DeleteProcess(_ context.Context, r *tasksapi.DeleteProcessRequest) (*tasksapi.DeleteResponse, error) {
	f.mu.Lock()
	task := f.tasks[r.ContainerID]
	f.mu.Unlock()
	select {
	case <-task.exited:
		return &tasksapi.DeleteResponse{}, nil
	default:
		return nil, status.Error(codes.FailedPrecondition, "writer remains live")
	}
}

func TestShutdownStopsRunningWritersBeforeHostPreparation(t *testing.T) {
	requireGuestRoot(t)
	for _, mode := range []string{"prepare", "signal", "stop-failure", "unknown-stop-failure", "archive-cleanup-failure"} {
		t.Run(mode, func(t *testing.T) {
			root := filepath.Join(stateRoot, "shutdown-fixture-"+strconv.FormatInt(time.Now().UnixNano(), 10))
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(root)
			mountTest := os.Getenv("AGENT_VMM_SHUTDOWN_MOUNT_TEST") == "1"
			if mountTest {
				if err := unix.Mount("tmpfs", root, "tmpfs", 0, ""); err != nil {
					t.Fatal(err)
				}
				defer unix.Unmount(root, 0)
			}
			fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{}}
			tasks := &shutdownTasks{tasks: map[string]*shutdownTask{}}
			names := &operationNamespaces{labels: map[string]map[string]string{runtimeContainerdNamespace: {}}}
			index := newRuntimeInventoryIndex()
			process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"sleep", "30"}})
			if err != nil {
				t.Fatal(err)
			}
			for i, owner := range []string{"gitea", "runner", "system.controller"} {
				namespace := "shutdown-" + strconv.Itoa(i) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
				if i == 2 {
					namespace = "system." + namespace
				}
				id := runtimeContainerKey(namespace, "payload")
				file, err := os.Create(filepath.Join(root, owner))
				if err != nil {
					t.Fatal(err)
				}
				command := exec.Command("sleep", "30")
				command.ExtraFiles = []*os.File{file}
				if err := command.Start(); err != nil {
					file.Close()
					t.Fatal(err)
				}
				file.Close()
				task := &shutdownTask{command: command, exited: make(chan struct{}), failStop: (mode == "stop-failure" || mode == "unknown-stop-failure") && i == 0, unknown: mode == "unknown-stop-failure" && i == 0}
				tasks.tasks[id] = task
				go func() { _ = command.Wait(); close(task.exited) }()
				t.Cleanup(func() { _ = command.Process.Kill(); <-task.exited })
				if _, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/3", command.Process.Pid)); err != nil {
					t.Fatalf("writer has no state FD: %v", err)
				}
				fixture.containers[id] = &containersapi.Container{ID: id, Labels: map[string]string{runtimeOwnerLabel: namespace, runtimeExternalIDLabel: "payload", runtimeImageLabel: "image", containerStateLabel: "running", containerInstanceLabel: "instance", containerLogLimitLabel: "4096"}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
				index.ensureNamespace(namespace)
				if err := index.addContainer(namespace, "payload"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { os.RemoveAll(filepath.Join(namespaceStateRoot(namespace), "logs", namespace)) })
			}
			if mountTest {
				if err := unix.Unmount(root, 0); !errors.Is(err, unix.EBUSY) {
					t.Fatalf("running writers did not hold mount: %v", err)
				}
			}
			listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "containerd.sock"))
			if err != nil {
				t.Fatal(err)
			}
			daemon := grpc.NewServer()
			imagesapi.RegisterImagesServer(daemon, fixture)
			containersapi.RegisterContainersServer(daemon, &systemContainerFixture{fixture: fixture})
			tasksapi.RegisterTasksServer(daemon, tasks)
			go daemon.Serve(listener)
			defer daemon.Stop()
			client, err := containerd.New(listener.Addr().String(), containerd.WithServices(containerd.WithNamespaceService(names)))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			runtime := &Runtime{client: client, network: new(guestnetwork.Manager), inventory: index, waiters: make(map[string][]chan uint32), logger: slog.Default().With("component", "guest-control")}

			containers, err := client.Containers(runtimeContext(context.Background()))
			if err != nil {
				t.Fatal(err)
			}
			for _, container := range containers {
				ctx := runtimeContext(context.Background())
				labels, err := container.Labels(ctx)
				if err != nil {
					t.Fatal(err)
				}
				task, err := container.Task(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				process, err := task.LoadProcess(ctx, mainProcessID, nil)
				if err != nil {
					t.Fatal(err)
				}
				taskExit, err := task.Wait(ctx)
				if err != nil {
					t.Fatal(err)
				}
				mainExit, err := process.Wait(ctx)
				if err != nil {
					t.Fatal(err)
				}
				runtime.tasks.Add(1)
				go func() {
					defer runtime.tasks.Done()
					runtime.monitorTask(labels[runtimeOwnerLabel], labels[runtimeExternalIDLabel], labels[containerInstanceLabel], container, task, process, mainExit, taskExit)
				}()
			}

			archiveFailure := errors.New("archive workspace cleanup incomplete")
			if mode == "archive-cleanup-failure" {
				done := make(chan struct{})
				var once sync.Once
				runtime.imageOperations.Store("appliance-build\x00fixture", &imageOperationRun{cancel: func() { once.Do(func() { close(done) }) }, done: done, cleanup: func(context.Context) error { return archiveFailure }})
			}
			prepared := false
			server := NewServer(runtime, HealthState{BootID: "boot"}, func(context.Context) error {
				prepared = true
				tasks.mu.Lock()
				defer tasks.mu.Unlock()
				for id, task := range tasks.tasks {
					if !task.deleted {
						t.Errorf("task %s resources still live before prepare", id)
					}
					if _, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/3", task.command.Process.Pid)); !os.IsNotExist(err) {
						t.Errorf("writer FD remains open: %v", err)
					}
				}
				if mountTest {
					if err := unix.Unmount(root, 0); err != nil {
						t.Errorf("settled writers kept mount busy: %v", err)
					}
				}
				return status.Error(codes.Aborted, "fixture reached host unmount")
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if mode == "signal" {
				if err := server.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
				if mountTest {
					if err := unix.Unmount(root, 0); err != nil {
						t.Fatalf("signal shutdown left mount busy: %v", err)
					}
				}
			} else {
				socket, err := net.Listen("unix", filepath.Join(t.TempDir(), "vmmd.sock"))
				if err != nil {
					t.Fatal(err)
				}
				rpc := NewGRPCServer()
				vmmdv1.RegisterVMMDServiceServer(rpc, server)
				go rpc.Serve(socket)
				defer rpc.Stop()
				connection, err := grpc.NewClient("unix://"+socket.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				_, err = vmmdv1.NewVMMDServiceClient(connection).PrepareShutdown(ctx, &vmmdv1.PrepareShutdownRequest{RequestId: "stop-boot"})
				if mode == "stop-failure" || mode == "unknown-stop-failure" || mode == "archive-cleanup-failure" {
					if err == nil || prepared {
						t.Fatalf("failed writer teardown reached host unmount: %v prepared=%t", err, prepared)
					}
					tasks.mu.Lock()
					for _, task := range tasks.tasks {
						task.failStop = false
						task.unknown = false
					}
					tasks.mu.Unlock()
					archiveFailure = nil
					_, err = vmmdv1.NewVMMDServiceClient(connection).PrepareShutdown(ctx, &vmmdv1.PrepareShutdownRequest{RequestId: "stop-boot"})
					if status.Code(err) != codes.Aborted || !prepared {
						t.Fatalf("same request did not retry writer cleanup: %v prepared=%t", err, prepared)
					}
				} else if status.Code(err) != codes.Aborted || !prepared {
					t.Fatalf("shutdown failed before host unmount: %v prepared=%t", err, prepared)
				}
			}
			fixture.mu.Lock()
			if len(fixture.containers) != 3 {
				t.Errorf("shutdown deleted durable container configuration: %d", len(fixture.containers))
			}
			fixture.mu.Unlock()
			if mode != "stop-failure" {
				tasks.mu.Lock()
				for id, task := range tasks.tasks {
					if !task.deleted {
						t.Errorf("task %s was not deleted", id)
					}
				}
				tasks.mu.Unlock()
			}
		})
	}
}

func TestShutdownFreezesStartingAndFailedSupervisorRecords(t *testing.T) {
	for _, state := range []vmmdv1.SystemApplianceState{vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STARTING, vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED} {
		t.Run(state.String(), func(t *testing.T) {
			supervisor := newApplianceSupervisor(new(Runtime), "boot")
			path := filepath.Join(t.TempDir(), "private-runtime")
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			record := &systemApplianceRecord{status: &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "controller", ContainerInstanceId: "previous"}, State: state, RestartCount: 1}, runtimePath: path}
			supervisor.records["controller"] = record
			supervisor.order.ReplaceOrInsert("controller")
			restartDone := make(chan struct{})
			go func() {
				supervisor.restartAfterBackoff("controller", "system.controller", "payload", "previous", 1)
				close(restartDone)
			}()
			if err := supervisor.shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			<-restartDone
			if !record.stopping || len(supervisor.records) != 0 {
				t.Fatal("supervisor retained a restartable record")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("private directory retained: %v", err)
			}
		})
	}
}

func TestShutdownWaitsForRestartAlreadyPastLaunchCheck(t *testing.T) {
	fixture := &systemImageFixture{containers: map[string]*containersapi.Container{}}
	runtime := &Runtime{client: systemImageClient(t, fixture), logger: slog.Default().With("component", "guest-control")}
	supervisor := newApplianceSupervisor(runtime, "boot")
	record := &systemApplianceRecord{status: &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "controller", ContainerInstanceId: "previous"}, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STARTING, RestartCount: 1}}
	supervisor.records["controller"] = record
	supervisor.order.ReplaceOrInsert("controller")
	unlock, lockErr := runtime.lockNamespace(context.Background(), "system.controller")
	if lockErr != nil {
		t.Fatal(lockErr)
	}
	restartDone := make(chan struct{})
	go func() {
		supervisor.restartAfterBackoff("controller", "system.controller", "payload", "previous", 1)
		close(restartDone)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for record.cleanupMu.TryLock() {
		record.cleanupMu.Unlock()
		if time.Now().After(deadline) {
			unlock()
			t.Fatal("restart did not reach launch")
		}
		time.Sleep(time.Millisecond)
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- supervisor.shutdown(context.Background()) }()
	for {
		supervisor.mu.Lock()
		stopping := record.stopping
		supervisor.mu.Unlock()
		if stopping {
			break
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-shutdownDone:
		unlock()
		t.Fatalf("shutdown crossed active restart: %v", err)
	default:
	}
	unlock()
	<-restartDone
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if len(supervisor.records) != 0 {
		t.Fatal("restart survived shutdown")
	}
}
