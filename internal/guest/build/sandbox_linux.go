//go:build linux

package build

import (
	"context"
	"path/filepath"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func (w *Workspace) specOptions(image containerd.Image, netNSPath string, limits *vmmdv1.NamespaceLimits) []oci.SpecOpts {
	args := []string{"/usr/bin/buildctl-daemonless.sh", "build", "--progress=plain", "--frontend", "dockerfile.v0", "--local", "context=/workspace", "--local", "dockerfile=/workspace", "--opt", "filename=" + w.header.GetDockerfilePath(), "--opt", "platform=linux/arm64", "--output", "type=oci,dest=/output/image.tar,name=" + w.header.GetImage()}
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
			"ROOTLESSKIT=/usr/bin/rootlesskit --net=host", "BUILDKITD_FLAGS=--root=/home/user/.local/share/buildkit/state --oci-worker-snapshotter=native --oci-worker-binary=/usr/bin/buildkit-runc --oci-worker-no-process-sandbox",
		}), oci.WithProcessCwd("/"), oci.WithUser("1000:1000"), oci.WithCgroup(filepath.Join("/agent-vmm", w.header.GetActorNamespaceId(), "build-"+w.id)),
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
				specs.Mount{Destination: "/home/user/.local/share/buildkit", Type: "bind", Source: filepath.Join("/state/build-cache", w.header.GetActorNamespaceId()), Options: []string{"rbind", "rw", "nosuid", "nodev"}},
				specs.Mount{Destination: "/run/secrets", Type: "bind", Source: w.secretRoot, Options: []string{"rbind", "ro", "nosuid", "nodev", "noexec"}},
				specs.Mount{Destination: "/output", Type: "bind", Source: w.outputRoot, Options: []string{"rbind", "rw", "nosuid", "nodev"}},
			)
			return nil
		},
	}
}

func buildResources(limits *vmmdv1.NamespaceLimits) *specs.LinuxResources {
	period, quota := uint64(100000), int64(limits.GetCpuMillis())*100
	memory, pids := int64(limits.GetMemoryBytes()), int64(limits.GetPids())
	return &specs.LinuxResources{CPU: &specs.LinuxCPU{Period: &period, Quota: &quota}, Memory: &specs.LinuxMemory{Limit: &memory}, Pids: &specs.LinuxPids{Limit: &pids}}
}
