//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"reflect"
	"syscall"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestTaskStartMetadataMutationsRunInsideOneOCIGrowthGuard(t *testing.T) {
	inside := false
	var calls []string
	process := &guardedProcess{inside: &inside, calls: &calls}
	task := &guardedTask{inside: &inside, calls: &calls, process: process}
	container := &guardedContainer{inside: &inside, calls: &calls, task: task}
	guard := func(_ context.Context, run func() error) error {
		if inside {
			t.Fatal("metadata guard was nested")
		}
		inside = true
		defer func() { inside = false }()
		return run()
	}
	started, err := startTaskMetadata(guard, context.Background(), context.Background(), context.Background(), container, &specs.Process{}, io.Discard, io.Discard, "instance-a")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"new-task", "task-wait", "task-start", "exec", "process-wait", "process-start", "set-labels"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("task-start mutations = %v, want %v", calls, want)
	}
	if inside || started.task != task || started.mainProcess != process {
		t.Fatalf("guard escaped or returned wrong task: inside=%t started=%+v", inside, started)
	}
}

func TestTaskStartMetadataGuardTightenFailureCleansStartedTask(t *testing.T) {
	want := errors.New("metadata ceiling could not be tightened")
	inside := false
	var calls []string
	process := &guardedProcess{inside: &inside, calls: &calls}
	task := &guardedTask{inside: &inside, calls: &calls, process: process}
	container := &guardedContainer{inside: &inside, calls: &calls, task: task}
	guard := func(_ context.Context, run func() error) error {
		inside = true
		if err := run(); err != nil {
			return err
		}
		inside = false
		return want
	}
	_, err := startTaskMetadata(guard, context.Background(), context.Background(), context.Background(), container, &specs.Process{}, io.Discard, io.Discard, "instance-a")
	if !errors.Is(err, want) {
		t.Fatalf("start error = %v, want %v", err, want)
	}
	if got := calls[len(calls)-3:]; !reflect.DeepEqual(got, []string{"process-kill", "process-delete", "task-delete"}) {
		t.Fatalf("post-guard cleanup = %v", got)
	}
}

func TestTaskStartCancellationUsesIndependentRetryingCleanup(t *testing.T) {
	want := errors.New("request cancelled after task start")
	requestContext, cancelRequest := context.WithCancel(context.Background())
	namespaceContext, cancelNamespace := context.WithCancel(context.Background())
	inside := false
	var calls []string
	process := &guardedProcess{inside: &inside, calls: &calls, requireLiveCleanup: true}
	task := &guardedTask{inside: &inside, calls: &calls, process: process, deleteFailures: 2, requireLiveCleanup: true}
	container := &guardedContainer{inside: &inside, calls: &calls, task: task}
	guard := func(_ context.Context, run func() error) error {
		inside = true
		if err := run(); err != nil {
			return err
		}
		inside = false
		cancelRequest()
		cancelNamespace()
		return want
	}
	_, err := startTaskMetadata(guard, requestContext, namespaceContext, context.Background(), container, &specs.Process{}, io.Discard, io.Discard, "instance-a")
	if !errors.Is(err, want) {
		t.Fatalf("start error = %v, want %v", err, want)
	}
	if task.deleteCalls != 3 {
		t.Fatalf("task delete attempts = %d, want 3", task.deleteCalls)
	}
}

type guardedContainer struct {
	containerd.Container
	inside *bool
	calls  *[]string
	task   containerd.Task
}

func (container *guardedContainer) NewTask(context.Context, cio.Creator, ...containerd.NewTaskOpts) (containerd.Task, error) {
	container.record("new-task")
	return container.task, nil
}

func (container *guardedContainer) SetLabels(context.Context, map[string]string) (map[string]string, error) {
	container.record("set-labels")
	return nil, nil
}

func (container *guardedContainer) record(call string) {
	if !*container.inside {
		panic(call + " ran outside metadata guard")
	}
	*container.calls = append(*container.calls, call)
}

type guardedTask struct {
	containerd.Task
	inside             *bool
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
	if !*task.inside {
		panic(call + " ran outside metadata guard")
	}
	*task.calls = append(*task.calls, call)
}

type guardedProcess struct {
	containerd.Process
	inside             *bool
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
	if !*process.inside {
		panic(call + " ran outside metadata guard")
	}
	*process.calls = append(*process.calls, call)
}
