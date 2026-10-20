//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	containerd "github.com/containerd/containerd/v2/client"
)

func TestStartupCleanupPreservesRunningWorkloadAndFIFO(t *testing.T) {
	requireGuestRoot(t)
	fixture := &systemImageFixture{containers: map[string]*containersapi.Container{
		"retained": {ID: "retained", Labels: map[string]string{runtimeOwnerLabel: "owner", runtimeExternalIDLabel: "payload", containerStateLabel: "running", containerInstanceLabel: "original"}},
	}}
	namespaces := &operationNamespaces{labels: map[string]map[string]string{runtimeContainerdNamespace: {}}}
	runtime := &Runtime{client: systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(namespaces)))}
	if err := os.MkdirAll("/run/containerd/fifo", 0700); err != nil {
		t.Fatal(err)
	}
	fifo, err := os.CreateTemp("/run/containerd/fifo", "retained-test-")
	if err != nil {
		t.Fatal(err)
	}
	fifo.Close()
	defer os.Remove(fifo.Name())
	if err := runtime.cleanupTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Clean(fifo.Name())); err != nil {
		t.Fatalf("retained task IO removed: %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.containers) != 1 || fixture.containers["retained"].Labels[containerInstanceLabel] != "original" {
		t.Fatal("running instance was changed")
	}
}

type recoveredProcess struct {
	containerd.Process
	exit    chan containerd.ExitStatus
	deleted bool
}

func (p *recoveredProcess) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	return p.exit, nil
}
func (p *recoveredProcess) Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	p.deleted = true
	return nil, nil
}

type recoveredTask struct {
	containerd.Task
	process     *recoveredProcess
	exit        chan containerd.ExitStatus
	attached    bool
	missingMain bool
	deleted     bool
	killed      bool
}

func (task *recoveredTask) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	return task.exit, nil
}
func (task *recoveredTask) LoadProcess(_ context.Context, id string, attach cio.Attach) (containerd.Process, error) {
	if id != mainProcessID || attach == nil {
		return nil, errors.New("retained main process was not attached")
	}
	if task.missingMain {
		return nil, errdefs.ErrNotFound
	}
	task.attached = true
	return task.process, nil
}
func (task *recoveredTask) Kill(context.Context, syscall.Signal, ...containerd.KillOpts) error {
	if !task.killed {
		task.exit <- *containerd.NewExitStatus(0, time.Now(), nil)
		task.killed = true
	}
	return nil
}
func (task *recoveredTask) Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	task.deleted = true
	return nil, nil
}

type recoveredContainer struct {
	containerd.Container
	labels map[string]string
}

func (container *recoveredContainer) SetLabels(_ context.Context, labels map[string]string) (map[string]string, error) {
	container.labels = labels
	return labels, nil
}

func TestRecoveredMonitorAttachesOriginalMainAndSettlesAbandonedSupervisor(t *testing.T) {
	requireGuestRoot(t)
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			namespace := fmt.Sprintf("recover-%d", time.Now().UnixNano())
			generation := "original-generation"
			path := logPath(namespace, "payload", generation)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(filepath.Join(stateRoot, "logs", namespace))
			process := &recoveredProcess{exit: make(chan containerd.ExitStatus, 1)}
			task := &recoveredTask{process: process, exit: make(chan containerd.ExitStatus, 1), missingMain: missing}
			container := new(recoveredContainer)
			runtime := &Runtime{network: new(guestnetwork.Manager)}
			labels := map[string]string{runtimeExternalIDLabel: "payload", containerLogLimitLabel: "1000", containerInstanceLabel: "original-instance"}
			if err := runtime.recoverTaskMonitor(context.Background(), namespace, container, task, labels, generation); err != nil {
				t.Fatal(err)
			}
			if missing {
				if !task.deleted || !task.killed || process.deleted || container.labels[containerExitLabel] != strconv.FormatUint(containerd.UnknownExitStatus, 10) {
					t.Fatalf("abandoned supervisor was not settled: %+v", container.labels)
				}
				return
			}
			if !task.attached || task.deleted || process.deleted || task.killed {
				t.Fatal("recovery mutated the live execution")
			}
			process.exit <- *containerd.NewExitStatus(23, time.Now(), nil)
			runtime.tasks.Wait()
			if !task.deleted || !process.deleted || !task.killed || container.labels[containerExitLabel] != "23" || container.labels[containerInstanceLabel] != "" {
				t.Fatalf("recovered monitor failed to settle exit: %+v", container.labels)
			}
		})
	}
}
