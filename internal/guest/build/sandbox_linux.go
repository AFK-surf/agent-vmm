//go:build linux

package build

import (
	"context"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/resourcev2"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func (w *Workspace) specOptions(image containerd.Image, netNSPath string, limits *vmmdv1.NamespaceLimits) []oci.SpecOpts {
	args := []string{"/usr/bin/buildctl-daemonless.sh", "build", "--progress=plain", "--frontend", "dockerfile.v0", "--local", "context=/workspace", "--local", "dockerfile=/workspace", "--opt", "filename=" + w.header.GetDockerfilePath(), "--opt", "platform=linux/arm64", "--output", "type=oci,dest=/output/image.tar,name=" + w.header.GetImage()}
	if w.baseRoot != "" {
		args = append(args, "--oci-layout", "base=/base-image", "--opt", "context:base=oci-layout://base@"+w.header.GetBaseManifestDigest())
	}
	if w.header.Target != nil {
		args = append(args, "--opt", "target="+w.header.GetTarget())
	}
	for _, argument := range w.header.GetBuildArgs() {
		args = append(args, "--opt", "build-arg:"+argument.GetName()+"="+argument.GetValue())
	}
	for _, secret := range w.header.GetSecrets() {
		args = append(args, "--secret", "id="+secret.GetId()+",src=/run/secrets/"+secret.GetId())
	}
	capabilities := []string{"CAP_SETUID", "CAP_SETGID", "CAP_SETPCAP"}
	return []oci.SpecOpts{
		oci.WithDefaultSpec(), oci.WithImageConfig(image),
		oci.WithProcessArgs(args...), oci.WithEnv([]string{
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/output/home", "USER=user",
			"XDG_RUNTIME_DIR=/output", "TMPDIR=/output/tmp", "BUILDKIT_HOST=unix:///output/buildkit/buildkitd.sock",
			"ROOTLESSKIT=/usr/bin/rootlesskit --net=host", "BUILDKITD_FLAGS=--root=/home/user/.local/share/buildkit/state --oci-worker-snapshotter=overlayfs --oci-worker-binary=/usr/bin/buildkit-runc --oci-worker-no-process-sandbox",
		}), oci.WithProcessCwd("/"), oci.WithUser("1000:1000"), oci.WithCgroup(filepath.Join(resourcev2.NamespaceCgroup(w.header.GetActorNamespaceId()), "build-"+w.id)),
		func(_ context.Context, _ oci.Client, _ *containers.Container, spec *oci.Spec) error {
			if spec.Annotations == nil {
				spec.Annotations = make(map[string]string)
			}
			spec.Annotations["io.kubernetes.cri.container-type"] = "sandbox"
			spec.Process.NoNewPrivileges = false
			spec.Process.Capabilities = &specs.LinuxCapabilities{Bounding: capabilities, Effective: capabilities, Permitted: capabilities, Inheritable: capabilities, Ambient: capabilities}
			spec.Linux.Resources = buildResources(limits)
			foundNetwork := false
			for index, namespace := range spec.Linux.Namespaces {
				if namespace.Type == specs.NetworkNamespace {
					spec.Linux.Namespaces[index].Path = netNSPath
					foundNetwork = true
				}
			}
			if !foundNetwork {
				spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{Type: specs.NetworkNamespace, Path: netNSPath})
			}
			spec.Mounts = append(spec.Mounts,
				specs.Mount{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"nosuid", "noexec", "nodev", "relatime", "ro"}},
				specs.Mount{Destination: "/workspace", Type: "bind", Source: w.contextRoot, Options: []string{"rbind", "ro", "nosuid", "nodev"}},
				specs.Mount{Destination: "/home/user/.local/share/buildkit", Type: "bind", Source: w.cacheRoot(), Options: []string{"rbind", "rw", "nosuid", "nodev"}},
				specs.Mount{Destination: "/run/secrets", Type: "bind", Source: w.secretRoot, Options: []string{"rbind", "ro", "nosuid", "nodev", "noexec"}},
				specs.Mount{Destination: "/output", Type: "bind", Source: w.outputRoot, Options: []string{"rbind", "rw", "nosuid", "nodev"}},
			)
			if w.baseRoot != "" {
				spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/base-image", Type: "bind", Source: w.baseRoot, Options: []string{"rbind", "ro", "nosuid", "nodev", "noexec"}})
			}
			return nil
		},
	}
}

func buildResources(limits *vmmdv1.NamespaceLimits) *specs.LinuxResources {
	pids := int64(limits.GetPids())
	return &specs.LinuxResources{
		Unified: resourcev2.Contract{PIDMax: uint64(limits.GetPids())}.CgroupValues(),
		Pids:    &specs.LinuxPids{Limit: &pids},
	}
}
