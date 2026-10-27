//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"syscall"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestTaskStartPublishesRunningOnlyAfterProcessStarts(t *testing.T) {
	var calls []string
	process := &guardedProcess{calls: &calls}
	task := &guardedTask{calls: &calls, process: process}
	container := &guardedContainer{calls: &calls, task: task}
	started, err := startTaskMetadata(context.Background(), context.Background(), container, &specs.Process{}, io.Discard, io.Discard, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"new-task", "task-wait", "task-start", "exec", "process-wait", "process-start", "set-labels"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("task-start mutations = %v, want %v", calls, want)
	}
	if started.task != task || started.mainProcess != process {
		t.Fatalf("returned wrong task: %+v", started)
	}
}

func TestTaskStartLabelFailureCleansStartedTask(t *testing.T) {
	want := errors.New("running label could not be committed")
	var calls []string
	process := &guardedProcess{calls: &calls}
	task := &guardedTask{calls: &calls, process: process}
	container := &guardedContainer{calls: &calls, task: task, labelError: func() error { return want }}
	_, err := startTaskMetadata(context.Background(), context.Background(), container, &specs.Process{}, io.Discard, io.Discard, "instance-a")
	if !errors.Is(err, want) {
		t.Fatalf("start error = %v, want %v", err, want)
	}
	if got := calls[len(calls)-4:]; !reflect.DeepEqual(got, []string{"process-kill", "process-delete", "task-kill", "task-delete"}) {
		t.Fatalf("post-guard cleanup = %v", got)
	}
}

func TestTaskStartCancellationUsesIndependentRetryingCleanup(t *testing.T) {
	want := errors.New("request cancelled after task start")

	namespaceContext, cancelNamespace := context.WithCancel(context.Background())
	defer cancelNamespace()
	var calls []string
	process := &guardedProcess{calls: &calls, requireLiveCleanup: true}
	task := &guardedTask{calls: &calls, process: process, deleteFailures: 2, requireLiveCleanup: true}
	container := &guardedContainer{calls: &calls, task: task, labelError: func() error { cancelNamespace(); return want }}
	_, err := startTaskMetadata(namespaceContext, context.Background(), container, &specs.Process{}, io.Discard, io.Discard, "instance-a")
	if !errors.Is(err, want) {
		t.Fatalf("start error = %v, want %v", err, want)
	}
	if task.deleteCalls != 3 {
		t.Fatalf("task delete attempts = %d, want 3", task.deleteCalls)
	}
}

type guardedContainer struct {
	labels     map[string]string
	labelError func() error
	containerd.Container
	calls *[]string
	task  containerd.Task
}

func (container *guardedContainer) NewTask(context.Context, cio.Creator, ...containerd.NewTaskOpts) (containerd.Task, error) {
	container.record("new-task")
	return container.task, nil
}

func (container *guardedContainer) SetLabels(context.Context, map[string]string) (map[string]string, error) {
	container.record("set-labels")
	if container.labelError != nil {
		return nil, container.labelError()
	}
	return nil, nil
}

func (container *guardedContainer) record(call string) {
	*container.calls = append(*container.calls, call)
}

type guardedTask struct {
	containerd.Task
	calls              *[]string
	process            containerd.Process
	deleteCalls        int
	deleteFailures     int
	requireLiveCleanup bool
}

func (task *guardedTask) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	task.record("task-wait")
	return make(chan containerd.ExitStatus), nil
}

func (task *guardedTask) Start(context.Context) error {
	task.record("task-start")
	return nil
}

func (task *guardedTask) Exec(context.Context, string, *specs.Process, cio.Creator) (containerd.Process, error) {
	task.record("exec")
	return task.process, nil
}

func (task *guardedTask) Kill(ctx context.Context, _ syscall.Signal, _ ...containerd.KillOpts) error {
	if task.requireLiveCleanup && ctx.Err() != nil {
		return ctx.Err()
	}
	task.record("task-kill")
	return nil
}

func (task *guardedTask) Delete(ctx context.Context, _ ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	if task.requireLiveCleanup && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	task.deleteCalls++
	*task.calls = append(*task.calls, "task-delete")
	if task.deleteCalls <= task.deleteFailures {
		return nil, errors.New("containerd task delete still in progress")
	}
	return nil, nil
}

func (task *guardedTask) record(call string) {
	*task.calls = append(*task.calls, call)
}

type guardedProcess struct {
	containerd.Process
	calls              *[]string
	requireLiveCleanup bool
}

func (process *guardedProcess) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	process.record("process-wait")
	return make(chan containerd.ExitStatus), nil
}

func (process *guardedProcess) Start(context.Context) error {
	process.record("process-start")
	return nil
}

func (process *guardedProcess) Kill(ctx context.Context, _ syscall.Signal, _ ...containerd.KillOpts) error {
	if process.requireLiveCleanup && ctx.Err() != nil {
		return ctx.Err()
	}
	*process.calls = append(*process.calls, "process-kill")
	return nil
}

func (process *guardedProcess) Delete(ctx context.Context, _ ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	if process.requireLiveCleanup && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	*process.calls = append(*process.calls, "process-delete")
	return nil, nil
}

func (process *guardedProcess) record(call string) {
	*process.calls = append(*process.calls, call)
}

func TestOldMonitorCannotCleanReplacementInstance(t *testing.T) {
	runtime := &Runtime{waiters: make(map[string][]chan uint32), logger: slog.Default().With("component", "guest-control")}
	unlock, err := runtime.lockContainer(context.Background(), "owner", "main")
	if err != nil {
		t.Fatal(err)
	}
	calls := []string{}
	container := &guardedContainer{calls: &calls, labels: map[string]string{containerInstanceLabel: "old", containerStateLabel: "running"}}
	task := &guardedTask{calls: &calls}
	process := &guardedProcess{calls: &calls}
	exit := make(chan containerd.ExitStatus)
	done := make(chan struct{})
	waiter := runtime.addWaiter("owner/main/replacement")
	go func() {
		runtime.monitorTask("owner", "main", "old", container, task, process, exit, make(chan containerd.ExitStatus))
		close(done)
	}()
	exit <- *containerd.NewExitStatus(23, time.Now(), nil)
	container.labels[containerInstanceLabel] = "replacement"
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old monitor did not stop")
	}
	if len(calls) != 0 || container.labels[containerStateLabel] != "running" || container.labels[containerInstanceLabel] != "replacement" {
		t.Fatalf("replacement changed: %v %+v", calls, container.labels)
	}
	select {
	case <-waiter:
		t.Fatal("replacement waiter was notified")
	default:
	}
}

func (container *guardedContainer) Labels(context.Context) (map[string]string, error) {
	return container.labels, nil
}
