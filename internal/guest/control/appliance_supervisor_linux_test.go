//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	guestnetwork "github.com/AFK-surf/agent-vmm/internal/guest/network"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ocimaterializer"
	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestSystemApplianceSpecIsNonRootReadOnlyAndDeviceFree(t *testing.T) {
	spec := &oci.Spec{Root: &specs.Root{}, Process: &specs.Process{}, Linux: &specs.Linux{}}
	if err := systemSpecOption("/run/system-a", "/data/system-a", "/state/content-projections/system-a")(context.Background(), nil, nil, spec); err != nil {
		t.Fatal(err)
	}
	if !spec.Root.Readonly || spec.Process.User.UID != systemContainerUID || spec.Process.User.GID != systemContainerUID || !spec.Process.NoNewPrivileges {
		t.Fatalf("system process identity = %+v, root = %+v", spec.Process, spec.Root)
	}
	if !reflect.DeepEqual(spec.Process.Capabilities, &specs.LinuxCapabilities{}) || len(spec.Linux.Devices) != 0 {
		t.Fatalf("system appliance retained capabilities or devices: %+v %+v", spec.Process.Capabilities, spec.Linux.Devices)
	}
	if len(spec.Process.Rlimits) != 1 || spec.Process.Rlimits[0] != (specs.POSIXRlimit{Type: "RLIMIT_CORE", Hard: 0, Soft: 0}) {
		t.Fatalf("core rlimit = %+v", spec.Process.Rlimits)
	}
	wantMounts := []specs.Mount{
		{Destination: "/run/agent-vmm", Type: "bind", Source: "/run/system-a", Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
		{Destination: supervisorPath, Type: "bind", Source: supervisorHostPath, Options: []string{"bind", "ro", "nosuid", "nodev", "exec"}},
		{Destination: "/var/lib/agent-vmm", Type: "bind", Source: "/data/system-a", Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
	}
	wantMounts = append(wantMounts, specs.Mount{Destination: "/opt/agent-vmm/content", Type: "bind", Source: "/state/content-projections/system-a", Options: []string{"rbind", "ro", "nosuid", "nodev", "noexec"}})
	if !reflect.DeepEqual(spec.Mounts, wantMounts) {
		t.Fatalf("system mounts = %+v, want %+v", spec.Mounts, wantMounts)
	}
	if got := spec.Linux.Resources.Devices; len(got) != 1 || got[0].Allow || got[0].Access != "rwm" {
		t.Fatalf("device cgroup = %+v", got)
	}
}

func TestDeleteSystemApplianceDataRejectsActiveInstallation(t *testing.T) {
	supervisor := newApplianceSupervisor(nil, "boot")
	supervisor.records["controller-a"] = &systemApplianceRecord{}
	supervisor.order.ReplaceOrInsert("controller-a")
	if err := supervisor.deleteData(context.Background(), "controller-a", "volume-a"); err == nil {
		t.Fatal("active appliance data was deleted")
	}
}

func TestFailedSystemApplianceIsRemovedFromListIndex(t *testing.T) {
	for _, fail := range []struct {
		name string
		run  func(*applianceSupervisor)
	}{
		{name: "launch", run: func(supervisor *applianceSupervisor) { supervisor.fail("controller-a", errors.New("failed")) }},
		{name: "sealed", run: func(supervisor *applianceSupervisor) { supervisor.seal("controller-a", "sealed") }},
	} {
		t.Run(fail.name, func(t *testing.T) {
			supervisor := newApplianceSupervisor(nil, "boot")
			supervisor.records["controller-a"] = &systemApplianceRecord{status: &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "controller-a"}}}
			supervisor.order.ReplaceOrInsert("controller-a")
			fail.run(supervisor)
			appliances, next := supervisor.list("", 32)
			if len(appliances) != 0 || next != "" {
				t.Fatalf("failed appliance remains in list: %+v, next=%q", appliances, next)
			}
		})
	}
}

func TestGetSystemApplianceUsesExactInstallation(t *testing.T) {
	supervisor := newApplianceSupervisor(nil, "boot")
	for _, installationID := range []string{"controller-a", "controller-b"} {
		supervisor.records[installationID] = &systemApplianceRecord{status: &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: installationID}}}
		supervisor.order.ReplaceOrInsert(installationID)
	}
	appliance, err := supervisor.get("controller-b")
	if err != nil || appliance.GetIdentity().GetInstallationId() != "controller-b" {
		t.Fatalf("get exact appliance = %+v, %v", appliance, err)
	}
	if _, err := supervisor.get("controller-c"); err == nil {
		t.Fatal("missing appliance returned without error")
	}
}

func systemStreamFixture() (*applianceSupervisor, *vmmdv1.SystemStreamHeader) {
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-a", BootId: "boot-a",
		NamespaceId: "system.installation-a", ContainerId: "payload", ContainerInstanceId: "instance-a",
		ImageManifestDigest: "sha256:image", BootNonce: make([]byte, 32)}
	supervisor := &applianceSupervisor{bootID: "boot-a", records: map[string]*systemApplianceRecord{
		"installation-a": {status: &vmmdv1.SystemAppliance{Identity: identity, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}, streams: make(map[string]uint32), inFlightNonces: make(map[string]struct{}), runtimePath: "/run/private-a"},
	}}
	header := &vmmdv1.SystemStreamHeader{Identity: cloneSystemIdentity(identity), Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP,
		Nonce: make([]byte, 32), MaxFrameBytes: bootstrapFrameLimit, MaxTotalBytes: bootstrapTotalLimit, IdleSeconds: 15, MaxConcurrentStreams: 1}
	return supervisor, header
}

func TestSystemStreamFencesExactIdentityAndSingleUseNonce(t *testing.T) {
	supervisor, header := systemStreamFixture()
	identity := header.GetIdentity()
	path, _, release, err := supervisor.acquireStream(header)
	if err != nil || path != "/run/private-a/bootstrap.sock" {
		t.Fatalf("acquire = %q, %v", path, err)
	}
	release()
	retry := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
	retry.Nonce = bytesOf(1, 32)
	_, retryCommit, retryRelease, err := supervisor.acquireStream(retry)
	if err != nil {
		t.Fatalf("bootstrap reservation did not roll back: %v", err)
	}
	if err := retryCommit(); err != nil {
		t.Fatal(err)
	}
	concurrent := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
	concurrent.Nonce = bytesOf(2, 32)
	if _, _, _, err := supervisor.acquireStream(concurrent); err == nil {
		t.Fatal("concurrent bootstrap was accepted")
	}
	retryRelease()
	if _, _, _, err := supervisor.acquireStream(retry); err == nil {
		t.Fatal("successful bootstrap nonce was reusable")
	}
	_, freshCommit, freshRelease, err := supervisor.acquireStream(header)
	if err != nil {
		t.Fatalf("fresh bootstrap nonce was rejected: %v", err)
	}
	if err := freshCommit(); err != nil {
		t.Fatal(err)
	}
	freshRelease()
	controllerHeader := &vmmdv1.SystemStreamHeader{Identity: cloneSystemIdentity(identity), Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER,
		Nonce: bytesOf(7, 32), MaxFrameBytes: frontendFrameLimit, IdleSeconds: 30, MaxConcurrentStreams: 1}
	_, controllerCommit, controllerRelease, err := supervisor.acquireStream(controllerHeader)
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerCommit(); err != nil {
		t.Fatal(err)
	}
	controllerRelease()
	if _, _, replayRelease, err := supervisor.acquireStream(controllerHeader); err != nil {
		t.Fatalf("controller stream did not release a failed transport: %v", err)
	} else {
		replayRelease()
	}

	for _, mutate := range []func(*vmmdv1.SystemStreamHeader){
		func(value *vmmdv1.SystemStreamHeader) { value.Identity.ContainerInstanceId = "wrong-instance" },
		func(value *vmmdv1.SystemStreamHeader) { value.Identity.BootId = "stale-boot" },
		func(value *vmmdv1.SystemStreamHeader) { value.Identity.BootNonce[0] = 1 },
		func(value *vmmdv1.SystemStreamHeader) { value.Nonce = value.Nonce[:31] },
	} {
		candidate := &vmmdv1.SystemStreamHeader{Identity: cloneSystemIdentity(identity), Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER,
			Nonce: make([]byte, 32), MaxFrameBytes: frontendFrameLimit, IdleSeconds: 30, MaxConcurrentStreams: 1}
		mutate(candidate)
		if _, _, _, err := supervisor.acquireStream(candidate); err == nil {
			t.Fatalf("mismatched stream was accepted: %+v", candidate)
		}
	}
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return result
}

func TestOrdinaryNamespaceCannotNameSystemRole(t *testing.T) {
	if err := validateID("namespace_id", "system.installation-a"); err == nil {
		t.Fatal("ordinary namespace accepted the reserved system role")
	}
	if err := validateSystemNamespace("system.installation-a"); err != nil {
		t.Fatal(err)
	}
	if err := validateSystemNamespace("ordinary-a"); err == nil {
		t.Fatal("system role accepted an ordinary namespace")
	}
}

func TestSystemLaunchErrorPreservesFirstStageAndBound(t *testing.T) {
	err := atRuntimeStage("snapshot_quota", errors.New(strings.Repeat("failure", 300)))
	err = atRuntimeStage("container_start", err)
	message := boundedSystemError(err)
	if !strings.HasPrefix(message, "system appliance launch failed: stage=snapshot_quota: failure") || len(message) != 1024 {
		t.Fatalf("bounded launch error lost its first stage or byte bound: %q", message)
	}
}

func TestSystemFrontendConnectionsReleaseNonces(t *testing.T) {
	for _, kind := range []vmmdv1.SystemStreamKind{vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND, vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER} {
		t.Run(kind.String(), func(t *testing.T) {
			supervisor, header := systemStreamFixture()
			header.Kind, header.MaxTotalBytes = kind, 0
			if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND {
				header.Name = "management"
			}
			for index := range 2048 {
				header.Nonce = make([]byte, 32)
				binary.LittleEndian.PutUint64(header.Nonce, uint64(index))
				_, commit, release, err := supervisor.acquireStream(header)
				if err != nil {
					t.Fatalf("stream %d: %v", index, err)
				}
				if _, _, _, err := supervisor.acquireStream(header); err == nil {
					t.Fatal("concurrent nonce was reusable")
				}
				if err := commit(); err != nil {
					t.Fatal(err)
				}
				release()
			}
			record := supervisor.records[header.GetIdentity().GetInstallationId()]
			if len(record.inFlightNonces) != 0 || len(record.bootstrapNonces) != 0 {
				t.Fatalf("retained ordinary stream nonce: %+v", record)
			}
		})
	}
}

func TestBootstrapStreamFailedDialReleasesReservation(t *testing.T) {
	supervisor, header := systemStreamFixture()
	supervisor.records[header.GetIdentity().GetInstallationId()].runtimePath = t.TempDir()
	listener := bufconn.Listen(1 << 20)
	returned := make(chan struct{})
	server := grpc.NewServer(grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		defer close(returned)
		return handler(service, stream)
	}))
	vmmdv1.RegisterVMMDServiceServer(server, &Server{appliances: supervisor})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bootstrap", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := vmmdv1.NewVMMDServiceClient(connection).OpenSystemStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Header{Header: header}}); err != nil {
		t.Fatal(err)
	}
	for {
		supervisor.mu.Lock()
		admitted := supervisor.records[header.GetIdentity().GetInstallationId()].streams[header.GetKind().String()+"/"] == 1
		supervisor.mu.Unlock()
		if admitted {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("bootstrap stream did not reserve its concurrency slot")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("missing bootstrap endpoint was accepted")
	}
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("failed dial handler did not return")
	}
	_, commit, release, err := supervisor.acquireStream(header)
	if err != nil {
		t.Fatalf("failed dial retained reservation: %v", err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	release()
	if _, _, _, err := supervisor.acquireStream(header); err == nil {
		t.Fatal("successful nonce was released after commit")
	}
}

func TestBootstrapStreamCommitRejectsReplacedInstance(t *testing.T) {
	supervisor, header := systemStreamFixture()
	_, commit, release, err := supervisor.acquireStream(header)
	if err != nil {
		t.Fatal(err)
	}
	record := supervisor.records[header.GetIdentity().GetInstallationId()]
	record.status.Identity.ContainerInstanceId = "new-instance"
	if err := commit(); err == nil {
		t.Fatal("stream committed after the running instance changed")
	}
	release()
	if _, _, _, err := supervisor.acquireStream(header); err == nil {
		t.Fatal("old instance opened a bootstrap stream")
	}
	current := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
	current.Identity = cloneSystemIdentity(record.status.GetIdentity())
	_, currentCommit, currentRelease, err := supervisor.acquireStream(current)
	if err != nil {
		t.Fatalf("failed old-instance commit leaked the concurrency slot: %v", err)
	}
	if err := currentCommit(); err != nil {
		t.Fatal(err)
	}
	currentRelease()
}

func TestBootstrapStreamFixedBounds(t *testing.T) {
	_, header := systemStreamFixture()
	for _, mutate := range []func(*vmmdv1.SystemStreamHeader){
		func(value *vmmdv1.SystemStreamHeader) { value.MaxFrameBytes++ },
		func(value *vmmdv1.SystemStreamHeader) { value.MaxTotalBytes++ },
		func(value *vmmdv1.SystemStreamHeader) { value.IdleSeconds++ },
		func(value *vmmdv1.SystemStreamHeader) { value.MaxConcurrentStreams++ },
		func(value *vmmdv1.SystemStreamHeader) { value.Name = "frontend" },
	} {
		candidate := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
		mutate(candidate)
		if err := validateSystemStreamBounds(candidate); err == nil {
			t.Fatal("bootstrap exceeded a fixed stream bound")
		}
	}
}

func TestBootstrapStreamConcurrentAdmissions(t *testing.T) {
	supervisor, header := systemStreamFixture()
	type result struct {
		commit  func() error
		release func()
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 8)
	for index := range 8 {
		candidate := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
		candidate.Nonce = bytesOf(byte(index), 32)
		go func() {
			<-start
			_, commit, release, err := supervisor.acquireStream(candidate)
			results <- result{commit, release, err}
		}()
	}
	close(start)
	var admitted []result
	for range 8 {
		value := <-results
		if value.err == nil {
			admitted = append(admitted, value)
		}
	}
	if len(admitted) != 1 {
		for _, value := range admitted {
			value.release()
		}
		t.Fatalf("concurrent admissions=%d want=1", len(admitted))
	}
	if err := admitted[0].commit(); err != nil {
		admitted[0].release()
		t.Fatal(err)
	}
	admitted[0].release()
}

func TestBootstrapStreamOldReleasePreservesNewInstanceReservation(t *testing.T) {
	supervisor, header := systemStreamFixture()
	_, oldCommit, oldRelease, err := supervisor.acquireStream(header)
	if err != nil {
		t.Fatal(err)
	}
	record := supervisor.records[header.GetIdentity().GetInstallationId()]
	record.status.Identity.ContainerInstanceId = "new-instance"
	record.streams = make(map[string]uint32)
	record.bootstrapNonces = make(map[string]struct{})
	record.inFlightNonces = make(map[string]struct{})
	current := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
	current.Identity = cloneSystemIdentity(record.status.GetIdentity())
	_, commit, release, err := supervisor.acquireStream(current)
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(); err != nil {
		release()
		t.Fatal(err)
	}
	if err := oldCommit(); err == nil {
		release()
		oldRelease()
		t.Fatal("old stream committed after replacement")
	}
	oldRelease()
	concurrent := proto.Clone(current).(*vmmdv1.SystemStreamHeader)
	concurrent.Nonce = bytesOf(9, 32)
	if _, _, otherRelease, err := supervisor.acquireStream(concurrent); err == nil {
		otherRelease()
		release()
		t.Fatal("old release freed the new instance concurrency slot")
	}
	release()
	if _, _, replayRelease, err := supervisor.acquireStream(current); err == nil {
		replayRelease()
		t.Fatal("old release removed the new instance consumed nonce")
	}
}

func TestFailedApplianceCleanupRetainsRecordUntilExactRetry(t *testing.T) {
	supervisor := newApplianceSupervisor(nil, "boot")
	record := &systemApplianceRecord{stopping: true, status: &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "controller-a"}, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED}, runtimePath: "/proc/version"}
	supervisor.records["controller-a"] = record
	supervisor.order.ReplaceOrInsert("controller-a")
	if err := supervisor.releaseRecord(context.Background(), record); err == nil {
		t.Fatal("discarded incomplete runtime cleanup")
	}
	if err := supervisor.deleteData(context.Background(), "controller-a", "volume-a"); err == nil {
		t.Fatal("deleted data with unresolved writer")
	}
	path := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	record.runtimePath = path
	if err := supervisor.releaseRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if supervisor.records["controller-a"] != nil {
		t.Fatal("successful cleanup retained record")
	}
	replacement := &systemApplianceRecord{status: record.status, runtimePath: path}
	supervisor.records["controller-a"] = replacement
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	supervisor.cleanupFailedRecord(record)
	if _, err := os.Stat(path); err != nil {
		t.Fatal("old cleanup touched replacement")
	}
}

func TestEnsureRPCRepeatedPartialCleanupUsesFreshExactRequest(t *testing.T) {
	request := &vmmdv1.EnsureSystemApplianceRequest{RequestId: "launch-retry", Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "controller-a", BootId: "boot-a", NamespaceId: "system.controller-a", ContainerId: "payload", ImageManifestDigest: "sha256:image", BootNonce: make([]byte, 32)}, ConfigRevision: 1, ImageReference: "image", DataVolumeId: "volume-a", DataVolumeBytes: 1, Limits: &vmmdv1.NamespaceLimits{}, EgressMode: vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newApplianceSupervisor(nil, "boot-a")
	record := &systemApplianceRecord{status: &vmmdv1.SystemAppliance{Identity: cloneSystemIdentity(request.Identity), State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED}, launchFingerprint: sha256.Sum256(encoded), stopping: true, runtimePath: "/proc/version"}
	supervisor.records["controller-a"] = record
	supervisor.order.ReplaceOrInsert("controller-a")
	for range 2 {
		if _, err := supervisor.ensure(context.Background(), request); err == nil || !strings.Contains(err.Error(), "cleanup incomplete") {
			t.Fatalf("cleanup escaped: %v", err)
		}
	}
	changed := proto.Clone(request).(*vmmdv1.EnsureSystemApplianceRequest)
	changed.ConfigRevision++
	if _, err := supervisor.ensure(context.Background(), changed); err == nil {
		t.Fatal("different payload entered cleanup")
	}
	record.runtimePath = ""
	if _, err := supervisor.ensure(context.Background(), request); err == nil || !strings.Contains(err.Error(), "data volume size is invalid") {
		t.Fatalf("exact retry did not reach prepare: %v", err)
	}
}

func TestStoppingApplianceRejectsStreamReservationAndCommit(t *testing.T) {
	supervisor, header := systemStreamFixture()
	_, commit, release, err := supervisor.acquireStream(header)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	record := supervisor.records[header.GetIdentity().GetInstallationId()]
	record.stopping = true
	if err := commit(); err == nil {
		t.Fatal("stream committed after stop began")
	}
	if _, _, release, err := supervisor.acquireStream(header); err == nil {
		release()
		t.Fatal("stopping appliance accepted stream")
	}
}

func TestSystemImageMountpointsAreDirectAndValidationDoesNotWrite(t *testing.T) {
	for _, invalid := range []string{"", "opt/agent-vmm/content", "opt/agent-vmm", "run/agent-vmm", "var/lib/agent-vmm", "etc/resolv.conf", ".agent-vmm-supervisor", "run", "var/lib", "etc"} {
		t.Run(invalid, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, ".agent-vmm-supervisor"), nil, 0755); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"opt/agent-vmm/content", "run/agent-vmm", "var/lib/agent-vmm", "etc"} {
				if err := os.MkdirAll(filepath.Join(root, path), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, "etc/resolv.conf"), []byte("unchanged"), 0644); err != nil {
				t.Fatal(err)
			}
			if invalid != "" {
				target := filepath.Join(root, invalid)
				if err := os.RemoveAll(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), target); err != nil {
					t.Fatal(err)
				}
			}
			before := map[string]os.FileMode{}
			if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err == nil {
					before[path] = info.Mode()
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			err := validateSystemImageMountpoints(root)
			if (err == nil) != (invalid == "") {
				t.Fatalf("validation = %v", err)
			}
			after := map[string]os.FileMode{}
			if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err == nil {
					after[path] = info.Mode()
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("validation modified image")
			}
		})
	}
	for _, path := range []string{"run/agent-vmm", "var/lib/agent-vmm", "etc/resolv.conf", ".agent-vmm-supervisor"} {
		t.Run("missing-"+path, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, ".agent-vmm-supervisor"), nil, 0755); err != nil {
				t.Fatal(err)
			}
			for _, directory := range []string{"opt/agent-vmm/content", "run/agent-vmm", "var/lib/agent-vmm", "etc"} {
				if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, "etc/resolv.conf"), nil, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(filepath.Join(root, path)); err != nil {
				t.Fatal(err)
			}
			if err := validateSystemImageMountpoints(root); err == nil {
				t.Fatal("missing mountpoint accepted")
			}
		})
	}
}

func TestSystemApplianceRemovalReleasesQuotaAfterCleanup(t *testing.T) {
	device := os.Getenv("AGENT_VMM_CONTROL_BUDGET_DEVICE")
	if device == "" {
		t.Skip("requires disposable /control ext4 project quota mount")
	}
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "namespaces.db")
	database, err := bolt.Open(databasePath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	facts := &diskOperationNamespaces{db: database}
	namespace := systemInstallationNamespace("removed")
	for i, name := range []string{namespace, "system.sibling", applianceBuildNamespace} {
		limits := &vmmdv1.NamespaceLimits{Pids: 64, DiskBytes: 2 << 30}
		if name == applianceBuildNamespace {
			limits = applianceBuildLimits
		}
		project := uint32(13000 + i)
		if err := facts.Create(ctx, name, namespaceLabels(limits, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, project, "10.42.1.0/24")); err != nil {
			t.Fatal(err)
		}
		path := volumePath(name, "")
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := assignProject(path, project); err != nil {
			t.Fatal(err)
		}
		if err := setProjectQuota(device, project, limits.GetDiskBytes()); err != nil {
			t.Fatal(err)
		}
	}
	fixture := &systemImageFixture{images: map[string]map[string]*imagesapi.Image{}, containers: map[string]*containersapi.Container{}}
	client := systemImageClient(t, fixture, containerd.WithServices(containerd.WithNamespaceService(facts), containerd.WithLeasesService(&operationLeases{})))
	network, err := guestnetwork.NewManager()
	if err != nil {
		t.Fatal(err)
	}
	defer network.Close()
	index := newRuntimeInventoryIndex()
	for _, name := range []string{namespace, "system.sibling", applianceBuildNamespace} {
		index.ensureNamespace(name)
	}
	runtime := &Runtime{client: client, controlDevice: device, inventory: index, diskBudget: new(guestdisk.Budget), materializer: new(ocimaterializer.Manager), network: network}
	runtime.builder = guestbuild.NewBuilder(client, network, nil)
	buildLabels, err := facts.Labels(ctx, applianceBuildNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.admitNamespaceDiskGrowth(ctx, applianceBuildNamespace, 0, buildLabels); err != nil {
		t.Fatalf("full appliance budget blocked internal builder metadata: %v", err)
	}
	buildQuota, err := guestquota.Read(device, 13002)
	if err != nil || buildQuota.HardLimitBytes != 0 || buildQuota.UsageBytes == 0 {
		t.Fatalf("builder metadata quota=%v %v", buildQuota, err)
	}
	supervisor := newApplianceSupervisor(runtime, "boot")
	supervisor.volumes = newApplianceVolumeManager(device)
	supervisor.volumes.root = filepath.Join(controlRoot, "removal-data")
	volume, err := supervisor.volumes.prepare("removed", "data", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volume.mountPath, "durable"), []byte("keep until cleanup succeeds"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.admitNamespaceDiskGrowth(ctx, "system.new", 2<<30, nil); !errors.Is(err, guestdisk.ErrCapacityUnavailable) || !strings.Contains(err.Error(), "committed_bytes=6442450944") || !strings.Contains(err.Error(), "budget_bytes=4294967296") || status.Code(rpcError(err)) != codes.ResourceExhausted {
		t.Fatalf("missing system budget diagnosis: %v", err)
	}
	spec, err := typeurl.MarshalAny(&specs.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	process, err := typeurl.MarshalAny(&specs.Process{Args: []string{"controller"}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.containers["writer"] = &containersapi.Container{ID: "writer", Spec: &anypb.Any{TypeUrl: spec.GetTypeUrl(), Value: spec.GetValue()}, Labels: map[string]string{runtimeOwnerLabel: namespace, runtimeExternalIDLabel: "payload", runtimeImageLabel: "old", containerStateLabel: "running", containerInstanceLabel: "instance"}, Extensions: map[string]*anypb.Any{containerProcess: {TypeUrl: process.GetTypeUrl(), Value: process.GetValue()}}}
	if err := index.addContainer(namespace, "payload"); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.deleteData(ctx, "removed", "data"); err == nil {
		t.Fatal("untracked running payload lost its data")
	}
	fixture.mu.Lock()
	fixture.containers["writer"].Labels[containerStateLabel] = "stopped"
	fixture.containers["writer"].ID = "stopped-container"
	fixture.containers["stopped-container"] = fixture.containers["writer"]
	delete(fixture.containers, "writer")
	fixture.mu.Unlock()
	originalProject := supervisor.volumes.project
	supervisor.volumes.project = func(string) (uint32, error) { return 0, fmt.Errorf("data cleanup unavailable") }
	if err := supervisor.deleteData(ctx, "removed", "data"); err == nil {
		t.Fatal("failed data cleanup released quota")
	}
	before, err := guestquota.Read(device, 13000)
	if err != nil || before.HardLimitBytes != 2<<30 {
		t.Fatalf("failed cleanup quota=%v %v", before, err)
	}
	if _, err := os.Stat(filepath.Join(volume.mountPath, "durable")); err != nil {
		t.Fatal(err)
	}
	supervisor.volumes.project = originalProject
	for range 2 {
		if err := supervisor.deleteData(ctx, "removed", "data"); err != nil {
			t.Fatal(err)
		}
	}
	after, err := guestquota.Read(device, 13000)
	if err != nil || after.HardLimitBytes != 0 {
		t.Fatalf("removed quota=%v %v", after, err)
	}
	if index.hasNamespace(namespace) {
		t.Fatal("removed namespace remains indexed")
	}
	if _, err := os.Stat(volume.mountPath); !os.IsNotExist(err) {
		t.Fatalf("data remains: %v", err)
	}
	names, err := facts.List(ctx)
	if err != nil || len(names) != 2 {
		t.Fatalf("namespace facts=%v %v", names, err)
	}
	if err := runtime.admitNamespaceDiskGrowth(ctx, "system.new", 2<<30, nil); err != nil {
		t.Fatalf("successful removal retained commitment: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = bolt.Open(databasePath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	facts.db = database
	restarted := &Runtime{client: client, controlDevice: device}
	if err := restarted.admitNamespaceDiskGrowth(ctx, "system.new", 2<<30, nil); err != nil {
		t.Fatalf("restart restored removed commitment: %v", err)
	}
}

func TestDeleteSystemApplianceDataBusyDoesNotHoldSupervisor(t *testing.T) {
	runtime := &Runtime{materializer: new(ocimaterializer.Manager)}
	supervisor := newApplianceSupervisor(runtime, "boot")
	unlock, ok := runtime.materializer.TryLockBuild()
	if !ok {
		t.Fatal("fresh materializer occupied")
	}
	defer unlock()
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- supervisor.deleteData(context.Background(), "installation", "volume") }()
	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("busy cleanup = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("busy cleanup waited for supervisor mutex")
	}
}
