//go:build linux

package control

import (
	"context"
	"errors"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
)

func TestSandboxStopRequiresTaskRemovalAndEmptyCgroup(t *testing.T) {
	for _, test := range []struct {
		name      string
		taskErr   error
		status    containerd.ProcessStatus
		statusErr error
		empty     bool
		emptyErr  error
		want      vmmdv1.ContainerState
	}{
		{name: "running", status: containerd.Running, want: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING},
		{name: "exited task remains", status: containerd.Stopped, empty: true, want: vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN},
		{name: "task observation lost", taskErr: errors.New("disconnected"), empty: true, want: vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN},
		{name: "status observation lost", statusErr: errors.New("disconnected"), empty: true, want: vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN},
		{name: "orphan process remains", taskErr: errdefs.ErrNotFound, want: vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN},
		{name: "cgroup observation lost", taskErr: errdefs.ErrNotFound, emptyErr: errors.New("unavailable"), want: vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN},
		{name: "fully reaped", taskErr: errdefs.ErrNotFound, empty: true, want: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED},
	} {
		t.Run(test.name, func(t *testing.T) {
			container := &stateContainer{task: &stateTask{status: test.status, err: test.statusErr}, err: test.taskErr}
			got := observedSandboxState(context.Background(), container, map[string]string{containerStateLabel: "running", containerInstanceLabel: "instance-a"}, func() (bool, error) { return test.empty, test.emptyErr })
			if got != test.want {
				t.Fatalf("state=%v, want %v", got, test.want)
			}
		})
	}
}

type stateContainer struct {
	containerd.Container
	task containerd.Task
	err  error
}

func (c *stateContainer) Task(context.Context, cio.Attach) (containerd.Task, error) {
	return c.task, c.err
}

type stateTask struct {
	containerd.Task
	status containerd.ProcessStatus
	err    error
}

func (t *stateTask) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{Status: t.status}, t.err
}
