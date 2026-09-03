//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	opencontainers "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestReusableSourceUnpackedSkipsStaleImageRecord(t *testing.T) {
	ready, err := reusableSourceUnpacked(false, errdefs.ErrNotFound)
	if err != nil || ready {
		t.Fatalf("stale source = (%v, %v), want (false, nil)", ready, err)
	}

	want := io.ErrUnexpectedEOF
	if _, err := reusableSourceUnpacked(false, want); !errors.Is(err, want) {
		t.Fatalf("non-not-found error = %v, want %v", err, want)
	}
}

func TestReusableTargetDescriptorPreservesManifestMetadata(t *testing.T) {
	want := opencontainers.Descriptor{
		MediaType: "application/vnd.oci.image.manifest.v1+json",
		Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Size:      1234,
	}
	got := reusableTargetDescriptor(images.Image{Target: want})
	if got == nil || !reflect.DeepEqual(*got, want) {
		t.Fatalf("reusable target descriptor = %+v, want %+v", got, want)
	}
}

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

func TestImportImageHeaderIsIdempotentAndRejectsConflictingReplay(t *testing.T) {
	server := new(Server)
	header := &vmmdv1.ImportImageHeader{RequestId: "import", ActorNamespaceId: "namespace", ImageReference: "example.com/agent/pi:latest",
		ArchiveSha256: make([]byte, 32), ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: "linux/arm64", ArchiveSize: 4096}
	calls := 0
	operation := func() (any, error) {
		calls++
		return &vmmdv1.Image{Reference: header.GetImageReference(), Digest: header.GetManifestDigest()}, nil
	}
	for range 2 {
		value, err := server.mutate(context.Background(), "import-image", header, operation)
		if err != nil || value.(*vmmdv1.Image).GetDigest() != header.GetManifestDigest() {
			t.Fatalf("import replay value=%+v err=%v", value, err)
		}
	}
	if calls != 1 {
		t.Fatalf("import operation ran %d times", calls)
	}
	conflict := proto.Clone(header).(*vmmdv1.ImportImageHeader)
	conflict.ArchiveSha256[0] = 1
	if _, err := server.mutate(context.Background(), "import-image", conflict, operation); err == nil {
		t.Fatal("conflicting import replay succeeded")
	}
}

func TestNormalizeImageReference(t *testing.T) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: "postgres:16-alpine", want: "docker.io/library/postgres:16-alpine"},
		{input: "pgvector/pgvector:pg16", want: "docker.io/pgvector/pgvector:pg16"},
		{input: "docker.io/pgvector/pgvector:pg16", want: "docker.io/pgvector/pgvector:pg16"},
		{input: "pgvector/pgvector@" + digest, want: "docker.io/pgvector/pgvector@" + digest},
	} {
		got, err := normalizeImageReference(test.input)
		if err != nil || got != test.want {
			t.Errorf("normalizeImageReference(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
	for _, input := range []string{"https://docker.io/library/postgres:16", "", "UPPERCASE/image:tag"} {
		if _, err := normalizeImageReference(input); err == nil {
			t.Errorf("normalizeImageReference(%q) succeeded", input)
		}
	}
}

func TestContainerGenerationTracksCreation(t *testing.T) {
	created := time.Unix(100, 25)
	first := containerGenerationID("service", created)
	if first == "" || first != containerGenerationID("service", created) {
		t.Fatalf("generation is not stable: %q", first)
	}
	if first == containerGenerationID("service", created.Add(time.Nanosecond)) || first == containerGenerationID("other", created) {
		t.Fatal("generation did not change with container identity")
	}
}

func TestImageGenerationTracksMetadataIdentity(t *testing.T) {
	created := time.Unix(100, 25)
	base := images.Image{Name: "docker.io/library/test:latest", Target: opencontainers.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, CreatedAt: created, UpdatedAt: created}
	first := imageGenerationID(base)
	if first == "" || first != imageGenerationID(base) {
		t.Fatalf("generation is not stable: %q", first)
	}
	changed := base
	changed.UpdatedAt = changed.UpdatedAt.Add(time.Nanosecond)
	if first == imageGenerationID(changed) {
		t.Fatal("generation did not change after record recreation")
	}
}

func TestContainerLogsAreGenerationIsolated(t *testing.T) {
	first := logPath("namespace", "service", "generation-a")
	second := logPath("namespace", "service", "generation-b")
	if first == second {
		t.Fatal("log paths are shared across container generations")
	}
	value, err := parseLogCursor("service:generation-a:42", "service", "generation-a")
	if err != nil || value != 42 {
		t.Fatalf("cursor = %d, %v", value, err)
	}
	if _, err := parseLogCursor("service:generation-a:42", "service", "generation-b"); status.Code(err) != codes.Aborted {
		t.Fatalf("stale cursor = %v", err)
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
	spec := &oci.Spec{Root: &specs.Root{}, Process: &specs.Process{Args: []string{"/bin/sh"}, Capabilities: &specs.LinuxCapabilities{Effective: []string{"CAP_SYS_ADMIN"}}}, Linux: &specs.Linux{}}
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
		{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"rw", "nosuid", "nodev", "noexec", "mode=1777", "size=64m"}},
		{Destination: "/etc/resolv.conf", Type: "bind", Source: "/state/network/resolv/agent-a.conf", Options: []string{"bind", "ro", "nosuid", "nodev"}},
	}
	if !reflect.DeepEqual(spec.Mounts, want) {
		t.Fatalf("mounts = %+v, want %+v", spec.Mounts, want)
	}
	if err := systemSpecOption("/run/system-a", "/data/system-a")(context.Background(), nil, nil, spec); err != nil {
		t.Fatal(err)
	}
	tmpMounts := 0
	for _, mount := range spec.Mounts {
		if filepath.Clean(mount.Destination) == "/tmp" {
			tmpMounts++
		}
	}
	if tmpMounts != 1 {
		t.Fatalf("system appliance received %d /tmp mounts: %+v", tmpMounts, spec.Mounts)
	}
	request.Volumes = append(request.Volumes, &vmmdv1.VolumeMount{VolumeId: "custom-tmp", Destination: "/tmp/../tmp"})
	spec.Mounts = nil
	options = runtime.specOptions("agent-a", request, nil)
	if err := options[len(options)-1](context.Background(), nil, nil, spec); err != nil {
		t.Fatal(err)
	}
	tmpMounts = 0
	for _, mount := range spec.Mounts {
		if filepath.Clean(mount.Destination) == "/tmp" {
			tmpMounts++
		}
	}
	if tmpMounts != 1 || spec.Mounts[len(spec.Mounts)-2].Type != "bind" {
		t.Fatalf("explicit /tmp volume was not preserved: %+v", spec.Mounts)
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
	if len(supervised.Process.Rlimits) != 1 || supervised.Process.Rlimits[0] != (specs.POSIXRlimit{Type: "RLIMIT_CORE", Hard: 0, Soft: 0}) {
		t.Fatalf("supervisor core rlimit = %+v", supervised.Process.Rlimits)
	}
	if len(supervised.Process.Env) != 0 {
		t.Fatalf("supervisor environment = %q", supervised.Process.Env)
	}
	stored := new(specs.Process)
	if err := typeurl.UnmarshalTo(metadata.Extensions[containerProcess], stored); err != nil {
		t.Fatal(err)
	}
	if stored.Args[0] != "/bin/sh" || !stored.NoNewPrivileges {
		t.Fatalf("stored process = %+v", stored)
	}
}

func TestProcessArgumentsPreserveAndOverrideOCIConfiguration(t *testing.T) {
	entrypoint := []string{"docker-entrypoint.sh"}
	command := []string{"postgres"}
	for _, test := range []struct {
		name       string
		entrypoint *vmmdv1.StringList
		command    *vmmdv1.StringList
		want       []string
	}{
		{name: "preserve", want: []string{"docker-entrypoint.sh", "postgres"}},
		{name: "command", command: &vmmdv1.StringList{Values: []string{"postgres", "-c", "shared_buffers=128MB"}}, want: []string{"docker-entrypoint.sh", "postgres", "-c", "shared_buffers=128MB"}},
		{name: "entrypoint", entrypoint: &vmmdv1.StringList{Values: []string{"/bin/check"}}, want: []string{"/bin/check", "postgres"}},
		{name: "replace-all", entrypoint: &vmmdv1.StringList{Values: []string{"true"}}, command: &vmmdv1.StringList{}, want: []string{"true"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := processArguments(entrypoint, command, test.entrypoint, test.command); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("arguments = %v, want %v", got, test.want)
			}
		})
	}
}

func TestStructuredLogReportsTruncation(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "records.log")
	stdout, stderr, err := newRecordLog(path, filepath.Join(directory, "records.meta"), logHeaderBytes+5)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := stdout.Write([]byte("abcdefgh")); err != nil || count != 8 {
		t.Fatalf("write = %d, %v", count, err)
	}
	if err := stdout.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stderr.Close(); err != nil {
		t.Fatal(err)
	}
	records, _, err := readLogRecords(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0].data) != "abcde" {
		t.Fatalf("records = %+v", records)
	}
	_, dropped, err := readLogMeta(filepath.Join(directory, "records.meta"))
	if err != nil || dropped != 1 {
		t.Fatalf("dropped = %d, %v", dropped, err)
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
