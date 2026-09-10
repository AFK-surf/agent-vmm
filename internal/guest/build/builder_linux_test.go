//go:build linux

package build

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/containerd/v2/pkg/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestValidateHeader(t *testing.T) {
	valid := func() *vmmdv1.BuildImageHeader {
		return &vmmdv1.BuildImageHeader{
			RequestId: "request", ActorNamespaceId: "actor", DockerfilePath: "Dockerfile",
			Image: "agent.local/image:latest", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE,
			BuildArgs: []*vmmdv1.BuildArgument{{Name: "MESSAGE", Value: "value"}}, Secrets: []*vmmdv1.BuildSecret{{Id: "token"}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader
		valid  bool
	}{
		{name: "valid", mutate: func(header *vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader { return header }, valid: true},
		{name: "nil", mutate: func(*vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader { return nil }},
		{name: "escaping Dockerfile", mutate: func(header *vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader {
			header.DockerfilePath = "../Dockerfile"
			return header
		}},
		{name: "network unspecified", mutate: func(header *vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader { header.NetworkMode = 0; return header }},
		{name: "duplicate argument", mutate: func(header *vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader {
			header.BuildArgs = append(header.BuildArgs, header.BuildArgs[0])
			return header
		}},
		{name: "duplicate secret", mutate: func(header *vmmdv1.BuildImageHeader) *vmmdv1.BuildImageHeader {
			header.Secrets = append(header.Secrets, header.Secrets[0])
			return header
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateHeader(test.mutate(valid()))
			if (err == nil) != test.valid {
				t.Fatalf("validateHeader() = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestValidateHeaderStoresCanonicalImage(t *testing.T) {
	header := &vmmdv1.BuildImageHeader{RequestId: "request", ActorNamespaceId: "actor", DockerfilePath: "Dockerfile", Image: "example:latest", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE}
	if err := validateHeader(header); err != nil {
		t.Fatal(err)
	}
	if header.GetImage() != "docker.io/library/example:latest" {
		t.Fatalf("image = %q", header.GetImage())
	}
}

func TestBuildSandboxPolicy(t *testing.T) {
	header := &vmmdv1.BuildImageHeader{ActorNamespaceId: "actor", DockerfilePath: "Dockerfile", Image: "agent.local/image:latest", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE}
	workspace := &Workspace{header: header, id: "build", contextRoot: "/state/build-tmp/actor/build/context", outputRoot: "/state/build-tmp/actor/build/output", secretRoot: "/run/agent-vmm-build/build"}
	limits := &vmmdv1.NamespaceLimits{CpuMillis: 500, MemoryBytes: 256 << 20, Pids: 64}
	options := workspace.specOptions(nil, "/run/netns/build", limits)
	spec := &oci.Spec{Process: &specs.Process{}, Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.NetworkNamespace}}}}
	for _, option := range options[2:] {
		if err := option(context.Background(), nil, nil, spec); err != nil {
			t.Fatal(err)
		}
	}
	if spec.Linux.CgroupsPath != "/agent-vmm/workload/actor/build-build" {
		t.Fatalf("build escaped workload pool: %s", spec.Linux.CgroupsPath)
	}
	wantCapabilities := []string{"CAP_SETUID", "CAP_SETGID", "CAP_SETPCAP"}
	capabilities := spec.Process.Capabilities
	if spec.Process.NoNewPrivileges || !reflect.DeepEqual(capabilities.Bounding, wantCapabilities) || !reflect.DeepEqual(capabilities.Effective, wantCapabilities) || !reflect.DeepEqual(capabilities.Inheritable, wantCapabilities) || !reflect.DeepEqual(capabilities.Permitted, wantCapabilities) || !reflect.DeepEqual(capabilities.Ambient, wantCapabilities) {
		t.Fatalf("capability policy = %+v", spec.Process)
	}
	if spec.Linux.Resources.Memory == nil || *spec.Linux.Resources.Memory.Limit != 256<<20 || spec.Linux.Namespaces[0].Path != "/run/netns/build" {
		t.Fatalf("resource or network policy = %+v", spec.Linux)
	}
	wantDestinations := []string{"/sys/fs/cgroup", "/workspace", "/home/user/.local/share/buildkit", "/run/secrets", "/output"}
	gotDestinations := make([]string, len(spec.Mounts))
	for index, mount := range spec.Mounts {
		gotDestinations[index] = mount.Destination
	}
	if !reflect.DeepEqual(gotDestinations, wantDestinations) {
		t.Fatalf("mount destinations = %q", gotDestinations)
	}
}

func TestSemaphoreWaitCancellation(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	builder := &Builder{semaphore: make(chan struct{}, 1), active: map[string]int{"actor": 1}}
	builder.semaphore <- struct{}{}
	workspace := &Workspace{builder: builder, contextClosed: true, contextRoot: root, header: &vmmdv1.BuildImageHeader{ActorNamespaceId: "actor", DockerfilePath: "Dockerfile"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := workspace.RunArchive(ctx, new(vmmdv1.NamespaceLimits), nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunArchive() = %v", err)
	}
	if err := builder.CanDeleteNamespace("actor"); err != nil {
		t.Fatalf("cancelled workspace retained namespace: %v", err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildOutputIsBounded(t *testing.T) {
	output := new(buildOutput)
	data := make([]byte, 2<<20)
	if count, err := output.Write(data); err != nil || count != len(data) {
		t.Fatalf("Write() = %d, %v", count, err)
	}
	if len(output.data) != 1<<20 {
		t.Fatalf("captured output = %d bytes", len(output.data))
	}
}

func TestOCIGrowthFailsClosedAndRunsOnlyInsideInjectedGate(t *testing.T) {
	operationRan := false
	if err := (&Builder{}).growOCI(context.Background(), "output", func() error {
		operationRan = true
		return nil
	}); err == nil || operationRan {
		t.Fatalf("missing gate err=%v operationRan=%v", err, operationRan)
	}
	gateCalls := 0
	builder := &Builder{growthGate: func(_ context.Context, identity string, operation func() error) error {
		gateCalls++
		if identity != "output" {
			t.Fatalf("growth identity=%q", identity)
		}
		return operation()
	}}
	if err := builder.growOCI(context.Background(), "output", func() error {
		operationRan = true
		return nil
	}); err != nil || !operationRan || gateCalls != 1 {
		t.Fatalf("gated growth err=%v operationRan=%v gateCalls=%d", err, operationRan, gateCalls)
	}
}
