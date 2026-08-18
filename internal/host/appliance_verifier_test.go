package host

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"strings"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func TestApplianceArtifactVerifierRejectsIdentityAndCapabilityChanges(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.PutAppliancePublisher(context.Background(), store.AppliancePublisher{KeyID: "publisher", PublicKey: publicKey, Revision: 1}); err != nil {
		t.Fatal(err)
	}
	manifest := verifierManifest()
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(privateKey, canonical)
	grant := verifierGrant()
	verifier := applianceArtifactVerifier{store: database}
	if _, _, err := verifier.verify(context.Background(), manifest, signature, grant); err != nil {
		t.Fatalf("valid artifact: %v", err)
	}

	tests := []struct {
		name      string
		manifest  *appliancev1.ControllerApplianceManifest
		signature []byte
		grant     *appliancev1.ApprovedGrant
	}{
		{name: "signature", manifest: manifest, signature: append([]byte(nil), signature[:63]...), grant: grant},
		{name: "platform", manifest: func() *appliancev1.ControllerApplianceManifest {
			value := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
			value.Platform = "linux/amd64"
			return value
		}(), signature: signature, grant: grant},
		{name: "capacity expansion", manifest: manifest, signature: signature, grant: func() *appliancev1.ApprovedGrant {
			value := proto.Clone(grant).(*appliancev1.ApprovedGrant)
			value.Capacity.MaxEnvironments++
			return value
		}()},
		{name: "frontend expansion", manifest: manifest, signature: signature, grant: func() *appliancev1.ApprovedGrant {
			value := proto.Clone(grant).(*appliancev1.ApprovedGrant)
			value.Frontends[0].Bounds.MaxFrameBytes++
			return value
		}()},
		{name: "dynamic frontend expansion", manifest: manifest, signature: signature, grant: func() *appliancev1.ApprovedGrant {
			value := proto.Clone(grant).(*appliancev1.ApprovedGrant)
			value.DynamicHostLoopback.MaxActiveLeases += 2
			return value
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := verifier.verify(context.Background(), test.manifest, test.signature, test.grant); err == nil {
				t.Fatal("invalid artifact was accepted")
			}
		})
	}
	oversize := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
	oversize.Release = strings.Repeat("x", (1<<20)+1)
	oversizeCanonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(oversize)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifier.verify(context.Background(), oversize, ed25519.Sign(privateKey, oversizeCanonical), grant); err == nil {
		t.Fatal("oversize canonical manifest was accepted")
	}
	unknownPublisher := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
	unknownPublisher.PublisherKeyId = "unknown"
	unknownCanonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(unknownPublisher)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifier.verify(context.Background(), unknownPublisher, ed25519.Sign(privateKey, unknownCanonical), grant); err == nil {
		t.Fatal("unknown publisher was accepted")
	}
}

func TestValidateApplianceCapacityIncludesSystemReservation(t *testing.T) {
	grant := verifierGrant()
	grant.SystemResources.Ceiling = verifierLimits(2)
	grant.Capacity.AggregateLimits = verifierLimits(3)
	if err := validateApplianceCapacity(grant, store.ResourceAmount{CPUMillis: 5, MemoryBytes: 5, PIDs: 5, DiskBytes: 5}); err != nil {
		t.Fatalf("grant that fits host rejected: %v", err)
	}
	if err := validateApplianceCapacity(grant, store.ResourceAmount{CPUMillis: 4, MemoryBytes: 5, PIDs: 5, DiskBytes: 5}); err == nil {
		t.Fatal("grant whose system plus aggregate CPU exceeds host was accepted")
	}
}

func verifierManifest() *appliancev1.ControllerApplianceManifest {
	return &appliancev1.ControllerApplianceManifest{
		ContractVersion: "1", ProductId: "product", PublisherKeyId: "publisher", Release: "1",
		ImageReference: "registry.example/product", ImageManifestDigest: "sha256:" + strings.Repeat("0", 64), Platform: "linux/arm64",
		BootstrapProtocolVersion: "1", ControllerProtocolVersion: "1", DataVolumeBytes: 1 << 30,
		StartupDeadlineSeconds: 30, ProbationSeconds: 1, HealthTimeoutSeconds: 10,
		MinimumApplianceRuntime:      "1",
		SystemResources:              &appliancev1.ResourceRange{Floor: verifierLimits(1), Ceiling: verifierLimits(4)},
		RequestedCapacity:            &appliancev1.CapacityGrant{MaxEnvironments: 2, AggregateLimits: verifierLimits(4), PerEnvironmentLimits: verifierLimits(2), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 10},
		RequestedEgressMode:          hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		RequestedFrontends:           []*appliancev1.ControllerFrontend{{Name: "control", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM, Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, Bounds: &appliancev1.StreamBounds{MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2, MaxUseCount: 4}}},
		RequestedDynamicHostLoopback: verifierDynamicGrant(4),
	}
}

func verifierGrant() *appliancev1.ApprovedGrant {
	return &appliancev1.ApprovedGrant{
		SystemResources:     &appliancev1.ResourceRange{Floor: verifierLimits(1), Ceiling: verifierLimits(3)},
		Capacity:            &appliancev1.CapacityGrant{MaxEnvironments: 2, AggregateLimits: verifierLimits(3), PerEnvironmentLimits: verifierLimits(2), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, MaxLeaseSeconds: 60, DisconnectGraceSeconds: 10},
		EgressMode:          hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		Frontends:           []*appliancev1.ControllerFrontend{{Name: "control", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM, Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, Bounds: &appliancev1.StreamBounds{MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2, MaxUseCount: 4}}},
		DynamicHostLoopback: verifierDynamicGrant(3),
	}
}

func verifierDynamicGrant(maxLeases uint32) *appliancev1.DynamicHostLoopbackGrant {
	return &appliancev1.DynamicHostLoopbackGrant{MaxActiveLeases: maxLeases, MaxActiveLeasesPerAllocation: 2, MaxLeaseSeconds: 60,
		MaxConcurrentConnections: 4, MaxConcurrentConnectionsPerLease: 2,
		ConnectionBounds: &appliancev1.StreamBounds{MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2, MaxUseCount: 4}}
}

func verifierLimits(value uint64) *hostv1.ResourceLimits {
	cpu, pids := uint32(value), uint32(value)
	memory, disk := value, value
	return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
}
