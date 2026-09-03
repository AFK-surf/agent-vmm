//go:build linux

package control

import (
	"context"
	"reflect"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/containerd/v2/pkg/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/proto"
)

func TestSystemApplianceSpecIsNonRootReadOnlyAndDeviceFree(t *testing.T) {
	spec := &oci.Spec{Root: &specs.Root{}, Process: &specs.Process{}, Linux: &specs.Linux{}}
	if err := systemSpecOption("/run/system-a", "/data/system-a")(context.Background(), nil, nil, spec); err != nil {
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
		{Destination: "/var/lib/agent-vmm", Type: "bind", Source: "/data/system-a", Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
	}
	if !reflect.DeepEqual(spec.Mounts, wantMounts) {
		t.Fatalf("system mounts = %+v, want %+v", spec.Mounts, wantMounts)
	}
	if got := spec.Linux.Resources.Devices; len(got) != 1 || got[0].Allow || got[0].Access != "rwm" {
		t.Fatalf("device cgroup = %+v", got)
	}
}

func TestSystemStreamFencesExactIdentityAndSingleUseBootstrap(t *testing.T) {
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-a", BootId: "boot-a",
		NamespaceId: "system.installation-a", ContainerId: "payload", ContainerInstanceId: "instance-a",
		ImageManifestDigest: "sha256:image", BootNonce: make([]byte, 32)}
	supervisor := &applianceSupervisor{bootID: "boot-a", records: map[string]*systemApplianceRecord{
		"installation-a": {status: &vmmdv1.SystemAppliance{Identity: identity, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}, streams: make(map[string]uint32), usedNonces: make(map[string]struct{}), runtimePath: "/run/private-a"},
	}}
	header := &vmmdv1.SystemStreamHeader{Identity: cloneSystemIdentity(identity), Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP,
		Nonce: make([]byte, 32), MaxFrameBytes: bootstrapFrameLimit, MaxTotalBytes: bootstrapTotalLimit, IdleSeconds: 15, MaxConcurrentStreams: 1, MaxUseCount: 1}
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
	retryRelease()
	if _, _, _, err := supervisor.acquireStream(header); err == nil {
		t.Fatal("bootstrap stream was reusable")
	}
	controllerHeader := &vmmdv1.SystemStreamHeader{Identity: cloneSystemIdentity(identity), Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER,
		Nonce: bytesOf(7, 32), MaxFrameBytes: frontendFrameLimit, MaxTotalBytes: frontendTotalLimit, IdleSeconds: 30, MaxConcurrentStreams: 1, MaxUseCount: 1}
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
			Nonce: make([]byte, 32), MaxFrameBytes: frontendFrameLimit, MaxTotalBytes: frontendTotalLimit, IdleSeconds: 30, MaxConcurrentStreams: 1, MaxUseCount: 1}
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
