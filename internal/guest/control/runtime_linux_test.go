//go:build linux

package control

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestShutdownWaitsForMutation(t *testing.T) {
	server := new(Server)
	started := make(chan struct{})
	release := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		_, err := server.mutate(context.Background(), "ensure-namespace", &vmmdv1.EnsureNamespaceRequest{RequestId: "active"}, func() (any, error) {
			close(started)
			<-release
			return nil, nil
		})
		mutationDone <- err
	}()
	<-started
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.beginShutdown(context.Background()) }()
	for !server.shuttingDown.Load() {
		runtime.Gosched()
	}
	if _, err := server.mutate(context.Background(), "ensure-namespace", &vmmdv1.EnsureNamespaceRequest{RequestId: "late"}, func() (any, error) { return nil, nil }); err == nil {
		t.Fatal("mutation accepted during shutdown")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown completed before mutation: %v", err)
	default:
	}
	close(release)
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestPublicRegistryClientIgnoresProxyEnvironment(t *testing.T) {
	transport, ok := publicRegistryClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("registry transport = %T", publicRegistryClient().Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("registry transport uses a proxy")
	}
}

func TestNamespaceLabelsRoundTrip(t *testing.T) {
	t.Parallel()
	limits := &vmmdv1.NamespaceLimits{CpuMillis: 1500, MemoryBytes: 256 << 20, Pids: 64, DiskBytes: 2 << 30}
	labels := namespaceLabels(limits, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10240, "10.64.240.0/24")
	projectID, namespace, err := namespaceFromLabels("agent-a", labels)
	if err != nil {
		t.Fatal(err)
	}
	if projectID != 10240 || !sameLimits(namespace.GetLimits(), limits) || namespace.GetEgressMode() != vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL || namespace.GetSubnet() != "10.64.240.0/24" {
		t.Fatalf("unexpected namespace: %+v", namespace)
	}
}

func TestFixedSpecPolicy(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{network: new(guestnetwork.Manager)}
	cpu, memory, pids := uint32(500), uint64(64<<20), uint32(16)
	request := &vmmdv1.CreateContainerRequest{
		ContainerId: "container", ReadOnlyRoot: true,
		Limits:  &vmmdv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids},
		Volumes: []*vmmdv1.VolumeMount{{VolumeId: "data", Destination: "/data", ReadOnly: true}},
	}
	options := runtime.specOptions("agent-a", request, nil)
	spec := &oci.Spec{Process: &specs.Process{Args: []string{"/bin/sh"}, Capabilities: &specs.LinuxCapabilities{Effective: []string{"CAP_SYS_ADMIN"}}}, Linux: &specs.Linux{}}
	if err := options[len(options)-1](context.Background(), nil, nil, spec); err != nil {
		t.Fatal(err)
	}
	if !spec.Process.NoNewPrivileges || !reflect.DeepEqual(spec.Process.Capabilities, &specs.LinuxCapabilities{}) {
		t.Fatalf("capabilities were not removed: %+v", spec.Process)
	}
	if spec.Annotations[runscContainerType] != runscSandbox {
		t.Fatalf("runsc container type = %q", spec.Annotations[runscContainerType])
	}
	if spec.Linux.Resources.CPU == nil || *spec.Linux.Resources.CPU.Quota != 50000 || spec.Linux.Resources.Memory == nil || *spec.Linux.Resources.Memory.Limit != int64(memory) || spec.Linux.Resources.Pids == nil || *spec.Linux.Resources.Pids.Limit != int64(pids) {
		t.Fatalf("resource policy mismatch: %+v", spec.Linux.Resources)
	}
	want := []specs.Mount{
		{Destination: "/data", Type: "bind", Source: "/state/volumes/agent-a/data", Options: []string{"rbind", "ro", "nosuid", "nodev"}},
		{Destination: "/etc/resolv.conf", Type: "bind", Source: "/state/network/resolv/agent-a.conf", Options: []string{"bind", "ro", "nosuid", "nodev"}},
	}
	if !reflect.DeepEqual(spec.Mounts, want) {
		t.Fatalf("mounts = %+v, want %+v", spec.Mounts, want)
	}
	encoded, err := typeurl.MarshalAny(spec)
	if err != nil {
		t.Fatal(err)
	}
	metadata := &containers.Container{Spec: encoded}
	if err := withSupervisedProcess()(context.Background(), nil, metadata); err != nil {
		t.Fatal(err)
	}
	value, err := typeurl.UnmarshalAny(metadata.Spec)
	if err != nil {
		t.Fatal(err)
	}
	supervised := value.(*specs.Spec)
	if !reflect.DeepEqual(supervised.Process.Args, []string{supervisorPath}) || metadata.Extensions[containerProcess] == nil {
		t.Fatalf("supervised spec = %+v", supervised.Process)
	}
	stored := new(specs.Process)
	if err := typeurl.UnmarshalTo(metadata.Extensions[containerProcess], stored); err != nil {
		t.Fatal(err)
	}
	if stored.Args[0] != "/bin/sh" || !stored.NoNewPrivileges {
		t.Fatalf("stored process = %+v", stored)
	}
}

func TestCappedLog(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "stdout.log")
	writer, err := newCappedLog(path, 5)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := writer.Write([]byte("abcdefgh")); err != nil || count != 8 {
		t.Fatalf("write = %d, %v", count, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcde" {
		t.Fatalf("log = %q", data)
	}
}

func TestForwardWriteHandlesBackpressure(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	payload := make([]byte, 1<<20)
	done := make(chan error, 1)
	go func() { done <- writeForwardData(client, payload) }()
	read, err := io.ReadAll(io.LimitReader(server, int64(len(payload))))
	if err != nil || len(read) != len(payload) {
		t.Fatalf("read = %d, %v", len(read), err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNumericUserAndEnvironment(t *testing.T) {
	t.Parallel()
	user, err := numericUser("1000:1001")
	if err != nil || user.UID != 1000 || user.GID != 1001 {
		t.Fatalf("user = %+v, %v", user, err)
	}
	if _, err := numericUser("root"); err == nil {
		t.Fatal("symbolic exec user was accepted")
	}
	got := mergeEnvironment([]string{"A=1", "B=2"}, []string{"A=3", "C=4"})
	want := []string{"A=3", "B=2", "C=4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("environment = %v, want %v", got, want)
	}
}
