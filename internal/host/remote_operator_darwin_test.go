//go:build darwin

package host

import (
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestManagedRegistrationMatchBindsIdentityTrustScopeAndPolicy(t *testing.T) {
	cpu, memory, pids, disk := uint32(1000), uint64(1<<30), uint32(128), uint64(2<<30)
	managed := &hostv1.BeginRemoteEnrollmentRequest{
		RegistrationId: "registration-1", Endpoint: "gateway.example.test:7443", DisplayName: "Managed",
		ProtocolVersion: "1", TrustBundle: []byte("trust"), ScopeDigest: "scope", SourcePolicyRevision: 7,
		Pool: &hostv1.RemotePoolPolicy{
			PerEnvironmentLimits: &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk},
			MaxEgressMode:        hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StopGrace: durationpb.New(time.Second)},
	}
	stored := store.RemoteRegistration{ID: managed.RegistrationId, Endpoint: managed.Endpoint, DisplayName: managed.DisplayName,
		ProtocolVersion: managed.ProtocolVersion, TrustBundle: append([]byte(nil), managed.TrustBundle...), ScopeDigest: managed.ScopeDigest,
		SourcePolicyRevision: managed.SourcePolicyRevision, PerEnvPIDs: pids,
		PerEnvDiskBytes: disk, MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL), StopGraceSeconds: 1}
	capacity := store.Environment{CPUMillis: cpu, MemoryBytes: memory, PIDs: pids, DiskBytes: disk}

	if matches, err := managedRegistrationMatches(managed, stored, capacity); err != nil || !matches {
		t.Fatalf("exact managed registration did not match: matches=%v err=%v", matches, err)
	}
	stored.TrustBundle = []byte("other")
	if matches, err := managedRegistrationMatches(managed, stored, capacity); err != nil || matches {
		t.Fatalf("trust drift was accepted: matches=%v err=%v", matches, err)
	}
	*managed.Pool.PerEnvironmentLimits.CpuMillis = cpu + 1
	if _, err := registrationFromPool(managed.Pool, capacity); err != nil {
		t.Fatalf("CPU template value constrained shared capacity: %v", err)
	}
	*managed.Pool.PerEnvironmentLimits.Pids = pids + 1
	if _, err := registrationFromPool(managed.Pool, capacity); err == nil {
		t.Fatal("PID limit above host capacity was accepted")
	}
}
