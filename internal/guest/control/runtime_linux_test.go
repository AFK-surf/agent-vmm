//go:build linux

package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	snapshotsapi "github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	digest "github.com/opencontainers/go-digest"
	specsVersion "github.com/opencontainers/image-spec/specs-go"
	opencontainers "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	for _, input := range []string{"https://docker.io/library/postgres:16", "", "docker.io/UPPERCASE:tag"} {
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
	limits := &vmmdv1.NamespaceLimits{Pids: 64, DiskBytes: 2 << 30}
	labels := namespaceLabels(limits, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10240, "10.64.240.0/24")
	projectID, namespace, err := namespaceFromLabels("agent-a", labels)
	if err != nil {
		t.Fatal(err)
	}
	if projectID != 10240 || !sameLimits(namespace.GetLimits(), limits) || namespace.GetEgressMode() != vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL || namespace.GetSubnet() != "10.64.240.0/24" {
		t.Fatalf("unexpected namespace: %+v", namespace)
	}
}

func TestRuntimeContainerOwnershipCannotCrossNamespaces(t *testing.T) {
	labels := map[string]string{runtimeOwnerLabel: "environment-a", runtimeExternalIDLabel: "main"}
	if err := validateRuntimeContainerOwnership(labels, "environment-a", "main"); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeContainerOwnership(labels, "environment-b", "main"); err == nil {
		t.Fatal("container owner mismatch was accepted")
	}
	if err := validateRuntimeContainerOwnership(labels, "environment-a", "other"); err == nil {
		t.Fatal("container identity mismatch was accepted")
	}
}

func TestContainerRoleKeepsSystemAndWorkloadNamespacesSeparate(t *testing.T) {
	if err := validateContainerIdentityForRole("environment-a", "main", false); err != nil {
		t.Fatal(err)
	}
	if err := validateContainerIdentityForRole("environment-a", "main", true); err == nil {
		t.Fatal("workload namespace was accepted for a system container")
	}
	if err := validateContainerIdentityForRole("system.appliance-a", "main", false); err == nil {
		t.Fatal("system namespace was accepted for a workload container")
	}
	if err := validateContainerIdentityForRole("system.appliance-a", "main", true); err != nil {
		t.Fatal(err)
	}
}

func TestFixedSpecPolicy(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{network: new(guestnetwork.Manager), logger: slog.Default().With("component", "guest-control")}
	pids := uint32(16)
	request := &vmmdv1.CreateContainerRequest{
		ContainerId: "container", ReadOnlyRoot: true,
		Limits:  &vmmdv1.ResourceLimits{Pids: &pids},
		Volumes: []*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data", ReadOnly: true}},
	}
	options := runtime.specOptions("agent-a", request, nil)
	spec := &oci.Spec{Root: &specs.Root{}, Process: &specs.Process{Args: []string{"/bin/sh"}, Capabilities: &specs.LinuxCapabilities{Effective: []string{"CAP_SYS_ADMIN"}}}, Linux: &specs.Linux{}}
	for _, option := range options[2:] {
		if err := option(context.Background(), nil, nil, spec); err != nil {
			t.Fatal(err)
		}
	}
	if spec.Linux.CgroupsPath != "/agent-vmm/workload/agent-a/container" {
		t.Fatalf("container escaped workload pool: %s", spec.Linux.CgroupsPath)
	}
	if !spec.Process.NoNewPrivileges || !reflect.DeepEqual(spec.Process.Capabilities, &specs.LinuxCapabilities{}) {
		t.Fatalf("capabilities were not removed: %+v", spec.Process)
	}
	if spec.Annotations[runscContainerType] != runscSandbox {
		t.Fatalf("runsc container type = %q", spec.Annotations[runscContainerType])
	}
	if spec.Linux.Resources.CPU != nil || spec.Linux.Resources.Memory != nil || spec.Linux.Resources.Unified["memory.max"] != "max" || spec.Linux.Resources.Unified["cpu.max"] != "max 100000" || spec.Linux.Resources.Pids == nil || *spec.Linux.Resources.Pids.Limit != int64(pids) {
		t.Fatalf("resource policy mismatch: %+v", spec.Linux.Resources)
	}
	want := []specs.Mount{
		{Destination: supervisorPath, Type: "bind", Source: supervisorHostPath, Options: []string{"bind", "ro", "nosuid", "nodev", "exec"}},
		{Destination: "/data", Type: "bind", Source: "/state/volumes/agent-a/data", Options: []string{"rbind", "ro", "nosuid", "nodev"}},
		{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"rw", "nosuid", "nodev", "noexec", "mode=1777", "size=64m"}},
		{Destination: "/etc/resolv.conf", Type: "bind", Source: "/state/network/resolv/agent-a.conf", Options: []string{"bind", "ro", "nosuid", "nodev"}},
	}
	if !reflect.DeepEqual(spec.Mounts, want) {
		t.Fatalf("mounts = %+v, want %+v", spec.Mounts, want)
	}
	if err := systemSpecOption("/run/system-a", "/data/system-a", "/state/content-projections/system-a")(context.Background(), nil, nil, spec); err != nil {
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
	request.Volumes = append(request.Volumes, &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_OwnedVolume{OwnedVolume: "custom-tmp"}, Destination: "/tmp/../tmp"})
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
	stdout, stderr, err := newRecordLog(path, logHeaderBytes+5)
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
	if err := os.WriteFile(path, append(mustReadLog(t, path), []byte("partial")...), 0600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = newRecordLog(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := stdout.Write([]byte("next")); err != nil || count != 4 {
		t.Fatalf("append = %d, %v", count, err)
	}
	stdout.Close()
	stderr.Close()
	records, _, err = readLogRecords(path, 0)
	if err != nil || len(records) != 2 || records[1].sequence != 2 || string(records[1].data) != "next" {
		t.Fatalf("recovered = %+v, %v", records, err)
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

type cleanupTaskFixture struct {
	containerd.Task
	killError   error
	deleteError error
	calls       []string
	all         bool
}

func (task *cleanupTaskFixture) Kill(ctx context.Context, signal syscall.Signal, options ...containerd.KillOpts) error {
	task.calls = append(task.calls, "kill")
	info := containerd.KillInfo{}
	for _, option := range options {
		if err := option(ctx, &info); err != nil {
			return err
		}
	}
	task.all = info.All && signal == syscall.SIGKILL
	return task.killError
}
func (task *cleanupTaskFixture) Delete(ctx context.Context, options ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	task.calls = append(task.calls, "delete")
	return nil, task.deleteError
}
func TestDeleteTaskConfirmsAllWritersBeforeDelete(t *testing.T) {
	for _, failure := range []string{"", "kill", "delete"} {
		t.Run(failure, func(t *testing.T) {
			task := &cleanupTaskFixture{}
			expected := errors.New("unknown cleanup")
			if failure == "kill" {
				task.killError = expected
			}
			if failure == "delete" {
				task.deleteError = expected
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "delete" {
				cancel()
			}
			err := deleteTask(ctx, task)
			if failure == "" && err != nil || failure != "" && !errors.Is(err, expected) {
				t.Fatalf("cleanup result: %v", err)
			}
			if !task.all {
				t.Fatal("cleanup did not kill all task writers")
			}
			expectedCalls := []string{"kill", "delete"}
			if failure == "kill" {
				expectedCalls = []string{"kill"}
			}
			if !reflect.DeepEqual(task.calls, expectedCalls) {
				t.Fatalf("calls: %v", task.calls)
			}
		})
	}
}

func TestExecAdmissionCountsActiveAndBoundsCompletedReplay(t *testing.T) {
	server := &Server{}
	for i := 0; i < 80; i++ {
		key := fmt.Sprintf("completed-%d", i)
		record, leader, err := server.beginExec(key, key)
		if err != nil || !leader {
			t.Fatalf("sequential execution %d: leader=%v err=%v", i, leader, err)
		}
		server.finishExec(record, 0, nil)
	}
	cached, leader, err := server.beginExec("completed-0", "completed-0")
	if err != nil || leader || cached.exitCode != 0 {
		t.Fatalf("completed replay: leader=%v err=%v", leader, err)
	}
	if _, _, err := server.beginExec("completed-0", "changed"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed replay: %v", err)
	}
	cached.completed = time.Now().Add(-dedupeTTL - time.Second)
	if _, _, err := server.beginExec("new", "new"); err != nil {
		t.Fatal(err)
	}
	if server.executions["completed-0"] != nil {
		t.Fatal("expired completed record was retained")
	}
	for i := 1; i < 64; i++ {
		key := fmt.Sprintf("active-%d", i)
		if _, _, err := server.beginExec(key, key); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := server.beginExec("overflow", "overflow"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("active capacity classification: %v", err)
	}
	for _, record := range server.executions {
		if record.completed.IsZero() {
			server.finishExec(record, 0, nil)
		}
	}
	pinned, _, err := server.beginExec("pinned", "pinned")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < dedupeLimit+80; i++ {
		key := fmt.Sprintf("retained-%d", i)
		record, _, err := server.beginExec(key, key)
		if err != nil {
			t.Fatal(err)
		}
		server.finishExec(record, 2, nil)
	}
	if server.executions["pinned"] != pinned || !pinned.completed.IsZero() {
		t.Fatal("cache pressure evicted an active execution")
	}
	if len(server.executions) > dedupeLimit {
		t.Fatalf("completed cache grew to %d", len(server.executions))
	}
	last := fmt.Sprintf("retained-%d", dedupeLimit+79)
	if record, leader, err := server.beginExec(last, last); err != nil || leader || record.exitCode != 2 {
		t.Fatalf("latest result replay: leader=%v err=%v", leader, err)
	}
}

// hotImportSnapshots represents the already committed snapshot; content and
// image requests still use containerd's real client and Unix RPC fixture.
type hotImportSnapshots struct {
	snapshotsapi.Snapshotter
	missing bool
}

func (s *hotImportSnapshots) Stat(context.Context, string) (snapshotsapi.Info, error) {
	if s.missing {
		return snapshotsapi.Info{}, errdefs.ErrNotFound
	}
	return snapshotsapi.Info{Kind: snapshotsapi.KindCommitted}, nil
}

func TestMaterializedImportAcknowledgesBeforeOccupiedWriterSlot(t *testing.T) {
	ctx := context.Background()
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(opencontainers.Image{Platform: opencontainers.Platform{OS: "linux", Architecture: "arm64"}, RootFS: opencontainers.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromString("layer")}}})
	configDescriptor := opencontainers.Descriptor{MediaType: opencontainers.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	manifest, _ := json.Marshal(opencontainers.Manifest{Versioned: specsVersion.Versioned{SchemaVersion: 2}, Config: configDescriptor})
	target := opencontainers.Descriptor{MediaType: opencontainers.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	for name, blob := range map[string][]byte{"config": config, "manifest": manifest} {
		if err := content.WriteBlob(ctx, store, name, bytes.NewReader(blob), opencontainers.Descriptor{Digest: digest.FromBytes(blob), Size: int64(len(blob))}); err != nil {
			t.Fatal(err)
		}
	}
	namespace, reference := "system.hot", "registry.example/appliance:hot"
	physical := runtimeImageReference(target.Digest)
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{namespace: {reference: {Name: reference, Target: &types.Descriptor{MediaType: target.MediaType, Digest: target.Digest.String(), Size: target.Size}}}, runtimeContainerdNamespace: {physical: {Name: physical, Target: &types.Descriptor{MediaType: target.MediaType, Digest: target.Digest.String(), Size: target.Size}}}}, containers: map[string]*containersapi.Container{}}
	facts := &operationNamespaces{labels: map[string]map[string]string{namespace: {}, "system.cold": {}}}
	snapshots := &hotImportSnapshots{}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithContentStore(store), containerd.WithLeasesService(&operationLeases{}), containerd.WithSnapshotters(map[string]snapshotsapi.Snapshotter{containerSnapshotter: snapshots})))
	index := newRuntimeInventoryIndex()
	index.ensureNamespace(namespace)
	index.ensureNamespace("system.cold")
	runtime := &Runtime{client: client, inventory: index, materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	err = runtime.materializationSlot.Acquire(ctx, 1)
	unlock := func() { runtime.materializationSlot.Release(1) }
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	header := &vmmdv1.ImportImageHeader{RequestId: "hot-request", ActorNamespaceId: namespace, ImageReference: reference, ManifestDigest: target.Digest.String(), SystemImport: true, SystemRetention: &vmmdv1.SystemImageRetention{InstallationRevision: 1, ManifestDigests: []string{target.Digest.String()}}}
	server := &Server{runtime: runtime}
	timeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if image, hot, err := server.acknowledgeMaterializedImport(timeout, header, reference, target.Digest); err != nil || !hot || image.Digest != target.Digest.String() {
		t.Fatalf("hot acknowledgement = %v %v %v", image, hot, err)
	}
	// Durable request identity, including retention, survives a server restart.
	conflict := proto.Clone(header).(*vmmdv1.ImportImageHeader)
	conflict.ArchiveSize++
	if _, _, err := (&Server{runtime: runtime}).acknowledgeMaterializedImport(ctx, conflict, reference, target.Digest); status.Code(err) != codes.Aborted {
		t.Fatalf("conflicting receipt replay = %v", err)
	}
	cold := proto.Clone(header).(*vmmdv1.ImportImageHeader)
	cold.ActorNamespaceId = "system.cold"
	if _, hot, err := server.acknowledgeMaterializedImport(ctx, cold, reference, target.Digest); err != nil || hot {
		t.Fatalf("cross-namespace bypass = %v %v", hot, err)
	}
	facts.labels["source"] = namespaceLabels(&vmmdv1.NamespaceLimits{DiskBytes: 1 << 30}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10021, "10.42.21.0/24")
	facts.labels["target"] = namespaceLabels(&vmmdv1.NamespaceLimits{DiskBytes: 1 << 30}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 10022, "10.42.22.0/24")
	fixture.images["source"] = map[string]*imagesapi.Image{reference: {Name: reference, Target: fixture.images[namespace][reference].Target}}
	index.ensureNamespace("source")
	index.ensureNamespace("target")
	copyRequest := &vmmdv1.CopyImageRequest{RequestId: "warm-copy", SourceNamespaceId: "source", TargetNamespaceId: "target", ImageReference: reference, ManifestDigest: target.Digest.String(), ControllerRequestSha256: fmt.Sprintf("%x", sha256.Sum256([]byte("warm-copy")))}
	copied, err := server.CopyImage(timeout, copyRequest)
	if err != nil || copied.Digest != target.Digest.String() || copied.CopyReceipt.RequestId != copyRequest.RequestId {
		t.Fatalf("occupied-slot warm copy=%v err=%v", copied, err)
	}
	observed, err := runtime.imageOperation(timeout, "target", copyRequest.RequestId, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY)
	if err != nil || observed.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED || !observed.WriterSettled {
		t.Fatalf("settled warm copy receipt=%v err=%v", observed, err)
	}
	if _, err := server.CopyImage(timeout, copyRequest); err != nil {
		t.Fatalf("occupied-slot copy replay: %v", err)
	}
	if runtime.materializationSlot.TryAcquire(1) {
		runtime.materializationSlot.Release(1)
		t.Fatal("warm copy released cold/GC exclusion")
	}
	snapshots.missing = true
	if _, hot, err := server.acknowledgeMaterializedImport(ctx, header, reference, target.Digest); err != nil || hot {
		t.Fatalf("cold snapshot bypass = %v %v", hot, err)
	}
	coldCopy := proto.Clone(copyRequest).(*vmmdv1.CopyImageRequest)
	coldCopy.RequestId = "cold-copy"
	queued, cancelQueued := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelQueued()
	if _, err := server.CopyImage(queued, coldCopy); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("cold copy bypassed occupied slot until its deadline: %v", err)
	}
	stopped, err := runtime.imageOperation(timeout, "target", coldCopy.RequestId, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY)
	if err != nil || stopped.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED || !stopped.WriterSettled || stopped.GetStatus().GetCode() != int32(codes.ResourceExhausted) {
		t.Fatalf("cold admission did not settle copy pin at its deadline: %v %v", stopped, err)
	}
	if _, err := runtime.lockImportSlot(queued); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("cold writer admission = %v", err)
	}
}

func TestRunnerRequestIdentitiesPreserveMutationAndExecReplay(t *testing.T) {
	owner := "VwpTUla9HPAJRBo1iOjwO"
	for _, request := range []proto.Message{
		&vmmdv1.CreateVolumeRequest{RequestId: owner + ":jobs", ActorNamespaceId: "runner", VolumeId: "jobs"},
		&vmmdv1.CreateContainerRequest{RequestId: owner + ":container", ActorNamespaceId: "runner", ContainerId: "workload"},
		&vmmdv1.StartContainerRequest{RequestId: owner + ":start", ActorNamespaceId: "runner", ContainerId: "workload"},
	} {
		server := new(Server)
		calls := 0
		work := func() (any, error) { calls++; return "original", nil }
		for range 2 {
			value, err := server.mutate(context.Background(), "runner", request, work)
			if err != nil || value != "original" {
				t.Fatalf("runner mutation: value=%v err=%v", value, err)
			}
		}
		changed := proto.Clone(request)
		field := changed.ProtoReflect().Descriptor().Fields().ByName("actor_namespace_id")
		changed.ProtoReflect().Set(field, protoreflect.ValueOfString("other"))
		if _, err := server.mutate(context.Background(), "runner", changed, work); err != nil || calls != 2 {
			t.Fatalf("independent runner mutation: calls=%d err=%v", calls, err)
		}
		if server.events.events[0].GetRequestId() != mutationRequestID(request) {
			t.Fatal("original receipt identity changed")
		}
	}
	server := new(Server)
	for _, phase := range []string{"register", "daemon"} {
		start := &vmmdv1.ExecStart{RequestId: owner + ":" + phase, ActorNamespaceId: "runner", ContainerId: "workload", Argv: []string{"/bin/sh"}}
		if err := validateExecStart(start); err != nil {
			t.Fatal(err)
		}
		key := start.GetActorNamespaceId() + "/" + start.GetContainerId() + "/" + start.GetRequestId()
		record, leader, err := server.beginExec(key, phase)
		if err != nil || !leader {
			t.Fatalf("exec admission: %v %v", leader, err)
		}
		if replay, leader, err := server.beginExec(key, phase); err != nil || leader || replay != record {
			t.Fatal("active replay created another execution")
		}
		server.finishExec(record, 17, nil)
		if replay, leader, err := server.beginExec(key, phase); err != nil || leader || replay.exitCode != 17 {
			t.Fatal("completed replay lost original execution")
		}
		if _, _, err := server.beginExec(key, "changed"); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("conflicting exec: %v", err)
		}
	}
}

func TestDeleteContainerReplayAcknowledgesOriginalGenerationAfterReplacement(t *testing.T) {
	ctx := context.Background()
	const namespace, id = "service", "worker"
	key := runtimeContainerKey(namespace, id)
	created := time.Now().Add(-time.Second)
	process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"worker"}})
	if err != nil {
		t.Fatal(err)
	}
	record := &containersapi.Container{ID: key, CreatedAt: timestamppb.New(created), Labels: map[string]string{
		runtimeOwnerLabel: namespace, runtimeExternalIDLabel: id, runtimeImageLabel: "image", containerStateLabel: "stopped",
	}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
	fixture := &systemImageFixture{containers: map[string]*containersapi.Container{key: record}, tasksAbsent: true}
	index := newRuntimeInventoryIndex()
	index.ensureNamespace(namespace)
	if err := index.addContainer(namespace, id); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{client: systemImageClient(t, fixture), inventory: index, logger: slog.Default().With("component", "guest-control")}
	server := NewServer(runtime, HealthState{}, nil)
	request := &vmmdv1.DeleteContainerRequest{RequestId: "delete-original", ActorNamespaceId: namespace, ContainerId: id, ExpectedGenerationId: containerGenerationID(key, created)}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := server.DeleteContainer(canceled, request); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled deletion result: %v", err)
	}
	fixture.mu.Lock()
	retained := fixture.containers[key] != nil
	fixture.mu.Unlock()
	if !retained {
		t.Fatal("canceled deletion removed the container")
	}
	if _, err := server.DeleteContainer(ctx, request); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	record.CreatedAt = timestamppb.New(created.Add(time.Second))
	fixture.containers[key] = record
	fixture.mu.Unlock()
	if err := index.addContainer(namespace, id); err != nil {
		t.Fatal(err)
	}
	if _, err := server.DeleteContainer(ctx, request); err != nil {
		t.Fatalf("original successful receipt did not replay: %v", err)
	}
	observed, err := runtime.InspectContainer(ctx, namespace, id)
	if err != nil || observed.GetGenerationId() == request.ExpectedGenerationId {
		t.Fatalf("old receipt deleted replacement: %v %v", observed, err)
	}
	for _, mode := range []string{"expired-receipt", "restarted-receiver"} {
		t.Run(mode, func(t *testing.T) {
			receiver := server
			if mode == "expired-receipt" {
				server.dedupe.mu.Lock()
				for key, receipt := range server.dedupe.done {
					receipt.completed = time.Now().Add(-dedupeTTL)
					server.dedupe.done[key] = receipt
				}
				server.dedupe.mu.Unlock()
			} else {
				receiver = NewServer(runtime, HealthState{}, nil)
			}
			if _, err := receiver.DeleteContainer(ctx, request); status.Code(err) != codes.Aborted {
				t.Fatalf("old generation replay without cached receipt: %v", err)
			}
			current, err := runtime.InspectContainer(ctx, namespace, id)
			if err != nil || current.GetGenerationId() != observed.GetGenerationId() {
				t.Fatalf("old request affected replacement: %v %v", current, err)
			}
		})
	}
}

func TestStartReplayObservesRunningBeforeWithdrawnShareAdmission(t *testing.T) {
	ctx := context.Background()
	const namespace, id = "service", "web"
	key := runtimeContainerKey(namespace, id)
	created := time.Now().Add(-time.Second)
	process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"web"}})
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := encodeContainerMounts([]*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_HostShare{HostShare: "withdrawn"}, Destination: "/media"}})
	if err != nil {
		t.Fatal(err)
	}
	record := &containersapi.Container{ID: key, CreatedAt: timestamppb.New(created), Labels: map[string]string{
		runtimeOwnerLabel: namespace, runtimeExternalIDLabel: id, runtimeImageLabel: "image", containerStateLabel: "running", containerInstanceLabel: "original", containerVolumeLabel: mounts,
	}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
	fixture := &systemImageFixture{containers: map[string]*containersapi.Container{key: record}}
	runtime := &Runtime{client: systemImageClient(t, fixture), logger: slog.Default().With("component", "guest-control")}
	generation := containerGenerationID(key, created)
	request := &vmmdv1.StartContainerRequest{ActorNamespaceId: namespace, ContainerId: id, ExpectedGenerationId: generation}
	result, err := runtime.startContainer(ctx, namespace, id, false, request)
	if err != nil || result.GetInstanceId() != "original" || result.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("running replay=%v %v", result, err)
	}
	request.ExpectedGenerationId = "other"
	if _, err := runtime.startContainer(ctx, namespace, id, false, request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("generation mismatch=%v", err)
	}
	request.ExpectedGenerationId = generation
	fixture.mu.Lock()
	record.Labels[containerStateLabel] = "stopped"
	fixture.mu.Unlock()
	if _, err := runtime.startContainer(ctx, namespace, id, false, request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("new dispatch bypassed withdrawn share=%v", err)
	}
}

func mustReadLog(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type quotaRecordFile struct{ *os.File }

func (file quotaRecordFile) Write(data []byte) (int, error) {
	count, err := file.File.Write(data[:min(5, len(data))])
	if err != nil {
		return count, err
	}
	return count, syscall.EDQUOT
}

func TestStructuredLogQuotaFailurePreservesNextRecordGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.log")
	stdout, stderr, err := newRecordLog(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()
	defer stderr.Close()
	log := stdout.(*recordWriter).log
	file := log.file.(*os.File)
	log.file = quotaRecordFile{file}
	if count, err := stdout.Write([]byte("discarded")); err != nil || count != 9 {
		t.Fatalf("quota write=%d %v", count, err)
	}
	log.file = file
	if count, err := stdout.Write([]byte("kept")); err != nil || count != 4 {
		t.Fatalf("next write=%d %v", count, err)
	}
	records, offset, err := readLogRecords(path, 0)
	if err != nil || len(records) != 1 || records[0].sequence != 2 || string(records[0].data) != "kept" || offset != logHeaderBytes+4 {
		t.Fatalf("records=%+v offset=%d err=%v", records, offset, err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	records, next, err := readLogRecords(path, offset)
	if err != nil || len(records) != 0 || next != offset {
		t.Fatalf("partial tail advanced: %+v %d %v", records, next, err)
	}
}

func TestRuntimeLocksAreCancellableAndContainerScoped(t *testing.T) {
	runtime := new(Runtime)
	release, err := runtime.lockNamespace(context.Background(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.lockNamespace(ctx, "owner"); !errors.Is(err, context.Canceled) {
		t.Fatalf("namespace wait=%v", err)
	}
	containerRelease, err := runtime.lockContainer(context.Background(), "owner", "first")
	if err != nil {
		t.Fatal(err)
	}
	defer containerRelease()
	otherRelease, err := runtime.lockContainer(context.Background(), "owner", "second")
	if err != nil {
		t.Fatal(err)
	}
	otherRelease()
}

func TestMutationScopeSeparatesOwnersWithSharedRequestID(t *testing.T) {
	server := new(Server)
	calls := 0
	for _, owner := range []string{"first-owner", "second-owner"} {
		request := &vmmdv1.CreateVolumeRequest{RequestId: "shared", ActorNamespaceId: owner, VolumeId: "data"}
		if _, err := server.mutate(context.Background(), "volume", request, func() (any, error) { calls++; return owner, nil }); err != nil {
			t.Fatalf("owner %s conflicted: %v", owner, err)
		}
	}
	if calls != 2 {
		t.Fatalf("owners shared a receipt: %d", calls)
	}
}
