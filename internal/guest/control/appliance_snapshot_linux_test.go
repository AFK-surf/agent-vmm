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
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

func TestSystemOverlayReadonlyLayersAndCollection(t *testing.T) {
	base := os.Getenv("AGENT_VMM_QUOTA_TEST_ROOT")
	if base == "" {
		t.Skip("requires disposable ext4 filesystem")
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(base, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type != unix.EXT4_SUPER_MAGIC {
		t.Fatalf("fixture filesystem type=%x, want ext4", fs.Type)
	}
	root, err := os.MkdirTemp(base, "system-overlay-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	snapshotter, err := overlay.NewSnapshotter(root)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshotter.Close()
	ctx := context.Background()
	parent := ""
	for layer := range 11 {
		key := fmt.Sprintf("active-%d", layer)
		mounts, err := snapshotter.Prepare(ctx, key, parent)
		if err != nil {
			t.Fatal(err)
		}
		err = mount.WithTempMount(ctx, mounts, func(root string) error {
			if fixture := os.Getenv("AGENT_VMM_SYSTEM_TEST_ROOTFS"); layer == 0 && fixture != "" {
				if err := os.CopyFS(root, os.DirFS(fixture)); err != nil {
					return err
				}
			}
			for _, path := range []string{"opt/agent-vmm/content", "run/agent-vmm", "var/lib/agent-vmm", "etc"} {
				if err := os.MkdirAll(filepath.Join(root, path), 0755); err != nil {
					return err
				}
			}
			for _, path := range []string{"etc/resolv.conf", ".agent-vmm-supervisor", "version"} {
				if err := os.WriteFile(filepath.Join(root, path), []byte(key), 0755); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		parent = fmt.Sprintf("layer-%d", layer)
		if err := snapshotter.Commit(ctx, parent, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"current", "pending-update", "restart"} {
		mounts, err := snapshotter.View(ctx, name, parent)
		if err != nil {
			t.Fatal(err)
		}
		err = mount.WithReadonlyTempMount(ctx, mounts, func(root string) error {
			if err := validateSystemImageMountpoints(root); err != nil {
				return err
			}
			content, err := os.ReadFile(filepath.Join(root, "version"))
			if err != nil || string(content) != "active-10" {
				return fmt.Errorf("top layer=%q: %w", content, err)
			}
			if err := os.WriteFile(filepath.Join(root, "unexpected"), nil, 0600); !errors.Is(err, unix.EROFS) {
				return fmt.Errorf("read-only write error=%v, want EROFS", err)
			}
			if runsc := os.Getenv("AGENT_VMM_SYSTEM_TEST_RUNSC"); runsc != "" {
				testSystemOverlaySandbox(t, runsc, root, name)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := snapshotter.Remove(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	for layer := 10; layer >= 0; layer-- {
		if err := snapshotter.Remove(ctx, fmt.Sprintf("layer-%d", layer)); err != nil {
			t.Fatal(err)
		}
	}
	if err := snapshotter.(snapshots.Cleaner).Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	remaining, err := os.ReadDir(filepath.Join(root, "snapshots"))
	if err != nil || len(remaining) != 0 {
		t.Fatalf("snapshot GC remaining=%d error=%v", len(remaining), err)
	}
}

func testSystemOverlaySandbox(t *testing.T, runsc, root, name string) {
	t.Helper()
	bundle := t.TempDir()
	t.Cleanup(func() {
		// runsc retains this namespace mount for reuse under its runtime root.
		path := filepath.Join(bundle, "state", "null-netns")
		if err := unix.Unmount(path, 0); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EINVAL) {
			t.Errorf("unmount test runtime namespace: %v", err)
		}
	})
	for _, path := range []string{"runtime", "data", "content"} {
		directory := filepath.Join(bundle, path)
		if err := os.Mkdir(directory, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(directory, systemContainerUID, systemContainerUID); err != nil {
			t.Fatal(err)
		}
	}
	spec := &specs.Spec{
		Version: specs.Version,
		Root:    &specs.Root{Path: root},
		Process: &specs.Process{
			Args: []string{"/bin/busybox", "sh", "-ec", "test \"$(/bin/busybox cat /version)\" = active-10; if echo bad > /version; then exit 1; fi; echo persistent > /var/lib/agent-vmm/result; test \"$(/bin/busybox cat /var/lib/agent-vmm/result)\" = persistent"},
			Cwd:  "/", Env: []string{"PATH=/bin"},
		},
		Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{
			{Type: specs.PIDNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace},
			{Type: specs.UTSNamespace}, {Type: specs.MountNamespace},
		}},
		Mounts:      []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}},
		Annotations: map[string]string{runscContainerType: runscSandbox},
	}
	if err := systemSpecOption(filepath.Join(bundle, "runtime"), filepath.Join(bundle, "data"), filepath.Join(bundle, "content"))(context.Background(), nil, nil, spec); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, runsc, "--root="+filepath.Join(bundle, "state"), "--platform=systrap", "--directfs=false", "--overlay2=none", "--host-uds=create", "--network=none", "run", "--bundle="+bundle, name)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("runsc system view %s: %v\n%s", name, err, output)
	}
}
