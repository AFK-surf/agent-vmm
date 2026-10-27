//go:build linux

package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

func TestReadonlyWorkloadSupervisorBindWithoutImageMountpoint(t *testing.T) {
	runsc, source := os.Getenv("AGENT_VMM_SYSTEM_TEST_RUNSC"), os.Getenv("AGENT_VMM_SYSTEM_TEST_ROOTFS")
	if runsc == "" || source == "" {
		t.Skip("requires real runsc and a disposable linux/arm64 rootfs")
	}
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, supervisorPath)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	testFixedSupervisorSandbox(t, runsc, root, "missing-supervisor", false)
	info, err := os.Lstat(filepath.Join(root, supervisorPath))
	if err != nil || !info.Mode().IsRegular() || info.Size() != 0 {
		t.Fatalf("runsc mountpoint must contain no supervisor bytes: %v, %v", info, err)
	}
}

func testFixedSupervisorSandbox(t *testing.T, runsc, root, name string, system bool) {
	t.Helper()
	bundle := t.TempDir()
	state := filepath.Join(bundle, "state")
	t.Cleanup(func() {
		if err := unix.Unmount(filepath.Join(state, "null-netns"), 0); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EINVAL) {
			t.Errorf("unmount test runtime namespace: %v", err)
		}
	})
	supervisor := os.Getenv("AGENT_VMM_SYSTEM_TEST_SUPERVISOR")
	if supervisor == "" {
		supervisor = supervisorHostPath
	}
	spec := &specs.Spec{
		Version: specs.Version, Root: &specs.Root{Path: root, Readonly: true},
		Process:     &specs.Process{Args: []string{supervisorPath}, Cwd: "/", Env: []string{"PATH=/bin"}, NoNewPrivileges: true, Capabilities: &specs.LinuxCapabilities{}},
		Linux:       &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.PIDNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}, {Type: specs.MountNamespace}}},
		Mounts:      []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}, {Destination: supervisorPath, Type: "bind", Source: supervisor, Options: []string{"bind", "ro", "nosuid", "nodev", "exec"}}},
		Annotations: map[string]string{runscContainerType: runscSandbox},
	}
	user := "0:0"
	script := "test -x /.agent-vmm-supervisor; if echo bad > /.agent-vmm-supervisor; then exit 1; fi"
	if system {
		for _, path := range []string{"runtime", "data", "content"} {
			directory := filepath.Join(bundle, path)
			if err := os.Mkdir(directory, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(directory, systemContainerUID, systemContainerUID); err != nil {
				t.Fatal(err)
			}
		}
		if err := systemSpecOption(filepath.Join(bundle, "runtime"), filepath.Join(bundle, "data"), filepath.Join(bundle, "content"))(context.Background(), nil, nil, spec); err != nil {
			t.Fatal(err)
		}
		user = fmt.Sprintf("%d:%d", systemContainerUID, systemContainerUID)
		script += "; test \"$(/bin/busybox cat /version)\" = active-10; if echo bad > /version; then exit 1; fi; echo persistent > /var/lib/agent-vmm/result; test \"$(/bin/busybox cat /var/lib/agent-vmm/result)\" = persistent"
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	base := []string{"--root=" + state, "--platform=systrap", "--directfs=false", "--overlay2=none", "--host-uds=create", "--network=none"}
	if os.Getenv("AGENT_VMM_SYSTEM_TEST_IGNORE_CGROUPS") == "1" {
		base = append(base, "--ignore-cgroups=true")
	}
	run := func(args ...string) ([]byte, error) {
		output, err := os.CreateTemp(bundle, "runsc-*.log")
		if err != nil {
			return nil, err
		}
		defer os.Remove(output.Name())
		command := exec.CommandContext(ctx, runsc, append(append([]string{}, base...), args...)...)
		command.Stdout, command.Stderr = output, output
		err = command.Run()
		output.Close()
		data, readErr := os.ReadFile(output.Name())
		return data, errors.Join(err, readErr)
	}
	if output, err := run("run", "--detach", "--bundle="+bundle, name); err != nil {
		t.Fatalf("runsc supervisor %s: %v\n%s", name, err, output)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(cleanup, runsc, append(append([]string{}, base...), "delete", "--force", name)...).CombinedOutput(); err != nil {
			t.Errorf("delete test sandbox: %v: %s", err, output)
		}
	}()
	if output, err := run("exec", "--user="+user, name, "/bin/busybox", "sh", "-ec", script); err != nil {
		t.Fatalf("supervisor bind and readonly root: %v\n%s", err, output)
	}
	output, err := run("exec", "--user="+user, name, "/bin/busybox", "cat", "/proc/self/status")
	if err != nil {
		t.Fatalf("exec parent: %v: %s", err, output)
	}
	parent := -1
	for _, line := range strings.Split(string(output), "\n") {
		if value, found := strings.CutPrefix(line, "PPid:"); found {
			parent, err = strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if parent != 0 {
		t.Fatalf("exec parent=%d, supervisor must not reap retained exec processes", parent)
	}
	output, err = run("exec", "--user="+user, name, "/bin/busybox", "sh", "-c", "/bin/busybox sleep 0.05 & exit 17")
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 17 {
		t.Fatalf("retained exec exit changed: %v: %s", err, output)
	}
	deadline := time.Now().Add(time.Second)
	for {
		output, err = run("exec", "--user="+user, name, "/bin/busybox", "ps", "-o", "pid,ppid,stat,args")
		if err != nil {
			t.Fatalf("orphan processes: %v: %s", err, output)
		}
		orphan := false
		for _, line := range strings.Split(string(output), "\n")[1:] {
			fields := strings.Fields(line)
			orphan = orphan || len(fields) >= 3 && fields[1] == "1"
		}
		if !orphan {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("supervisor left an orphan process:\n%s", output)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
