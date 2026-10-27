//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
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
	type lowerState struct {
		project uint32
		content [32]byte
	}
	lower := make(map[string]lowerState)
	entries, err := os.ReadDir(filepath.Join(root, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := filepath.Join(root, "snapshots", entry.Name(), "fs")
		project, err := projectID(path)
		if err != nil {
			t.Fatal(err)
		}
		lower[path] = lowerState{project: project, content: snapshotContentHash(t, path)}
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
				testFixedSupervisorSandbox(t, runsc, root, name, true)
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
	for path, before := range lower {
		project, err := projectID(path)
		if err != nil || project != before.project || snapshotContentHash(t, path) != before.content {
			t.Fatalf("sandbox start changed lower snapshot %s: project=%d, error=%v", path, project, err)
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

func snapshotContentHash(t *testing.T, root string) [32]byte {
	t.Helper()
	hash := sha256.New()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fmt.Fprintf(hash, "%s\x00%d\x00", relative, info.Mode())
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			io.WriteString(hash, target)
		} else if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(hash, file)
			return errors.Join(err, file.Close())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return [32]byte(hash.Sum(nil))
}
