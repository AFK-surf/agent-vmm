//go:build linux

package control

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/opencontainers/image-spec/identity"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func systemSpecOption(runtimePath, dataPath, contentPath string) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, spec *oci.Spec) error {
		spec.Root.Readonly = true
		spec.Process.User = specs.User{UID: systemContainerUID, GID: systemContainerUID}
		spec.Process.NoNewPrivileges = true
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
		spec.Process.Rlimits = append(spec.Process.Rlimits, specs.POSIXRlimit{Type: "RLIMIT_CORE", Hard: 0, Soft: 0})
		if spec.Linux == nil {
			spec.Linux = &specs.Linux{}
		}
		spec.Linux.Devices = nil
		if spec.Linux.Resources == nil {
			spec.Linux.Resources = &specs.LinuxResources{}
		}
		spec.Linux.Resources.Devices = []specs.LinuxDeviceCgroup{{Allow: false, Access: "rwm"}}
		spec.Mounts = append(spec.Mounts,
			specs.Mount{Destination: "/run/agent-vmm", Type: "bind", Source: runtimePath, Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
			specs.Mount{Destination: "/var/lib/agent-vmm", Type: "bind", Source: dataPath, Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
		)
		spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/opt/agent-vmm/content", Type: "bind", Source: contentPath, Options: []string{"rbind", "ro", "nosuid", "nodev", "noexec"}})
		return nil
	}
}

func validateSystemSnapshotMountpoints(ctx context.Context, client *containerd.Client, container containerd.Container) error {
	info, err := container.Info(ctx)
	if err != nil {
		return atRuntimeStage("system_mountpoints", err)
	}
	mounts, err := client.SnapshotService(info.Snapshotter).Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return atRuntimeStage("system_mountpoints", err)
	}
	return atRuntimeStage("system_mountpoints", mount.WithReadonlyTempMount(ctx, mounts, validateSystemImageMountpoints))
}

func validateSystemImageMountpoints(root string) error {
	for _, target := range []struct {
		path      string
		directory bool
	}{
		{"/opt/agent-vmm/content", true}, {"/run/agent-vmm", true}, {"/var/lib/agent-vmm", true}, {"/etc/resolv.conf", false}, {supervisorPath, false},
	} {
		components := strings.Split(strings.TrimPrefix(target.path, "/"), "/")
		current := root
		for index, component := range components {
			current = filepath.Join(current, component)
			info, err := os.Lstat(current)
			directory := index < len(components)-1 || target.directory
			if err != nil || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
				return fmt.Errorf("system image requires a direct %s mountpoint", target.path)
			}
		}
	}
	return nil
}

func validateSystemImageMaterialization(ctx context.Context, client *containerd.Client, image containerd.Image) (resultErr error) {
	diffIDs, err := image.RootFS(ctx)
	if err != nil {
		return atRuntimeStage("system_mountpoints", err)
	}
	snapshots := client.SnapshotService(containerSnapshotter)
	key := "system-mountpoint-validation-" + rand.Text()
	mounts, err := snapshots.View(ctx, key, identity.ChainID(diffIDs).String())
	if err != nil {
		return atRuntimeStage("system_mountpoints", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, atRuntimeStage("system_mountpoints_cleanup", snapshots.Remove(cleanupCtx, key)))
	}()
	return atRuntimeStage("system_mountpoints", mount.WithReadonlyTempMount(ctx, mounts, validateSystemImageMountpoints))
}
