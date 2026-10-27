package appliance

import (
	"strings"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestApplianceArtifactVerifierRejectsIdentityAndCapabilityChanges(t *testing.T) {
	manifest := verifierManifest()
	grant := verifierGrant()
	verifier := applianceArtifactVerifier{}
	if _, _, err := verifier.verify(manifest, grant); err != nil {
		t.Fatalf("valid artifact: %v", err)
	}

	legacy, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	legacy = protowire.AppendBytes(protowire.AppendTag(legacy, 19, protowire.BytesType), []byte{8, 4})
	restored := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(legacy, restored); err != nil {
		t.Fatal(err)
	}
	if err := validateApprovedGrant(restored, grant); err != nil {
		t.Fatalf("legacy dynamic grant field: %v", err)
	}

	tests := []struct {
		name     string
		manifest *appliancev1.ControllerApplianceManifest
		grant    *appliancev1.ApprovedGrant
	}{
		{name: "platform", manifest: func() *appliancev1.ControllerApplianceManifest {
			value := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
			value.Platform = "linux/amd64"
			return value
		}(), grant: grant},
		{name: "capacity expansion", manifest: manifest, grant: func() *appliancev1.ApprovedGrant {
			value := proto.Clone(grant).(*appliancev1.ApprovedGrant)
			pids := value.Capacity.PerEnvironmentLimits.GetPids() + 1
			value.Capacity.PerEnvironmentLimits.Pids = &pids
			return value
		}()},
		{name: "frontend expansion", manifest: manifest, grant: func() *appliancev1.ApprovedGrant {
			value := proto.Clone(grant).(*appliancev1.ApprovedGrant)
			value.Frontends[0].Bounds.MaxFrameBytes++
			return value
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := verifier.verify(test.manifest, test.grant); err == nil {
				t.Fatal("invalid artifact was accepted")
			}
		})
	}
	oversize := proto.Clone(manifest).(*appliancev1.ControllerApplianceManifest)
	oversize.Release = strings.Repeat("x", (1<<20)+1)
	if _, _, err := verifier.verify(oversize, grant); err == nil {
		t.Fatal("oversize canonical manifest was accepted")
	}
}

func TestValidateApplianceIsolationLimitsRejectsValuesAboveHost(t *testing.T) {
	grant := verifierGrant()
	grant.SystemLimits = verifierLimits(2)
	grant.Capacity.PerEnvironmentLimits = verifierLimits(3)
	if err := validateApplianceIsolationLimits(grant, store.ResourceAmount{PIDs: 3, DiskBytes: 3}, store.ResourceAmount{PIDs: 3, DiskBytes: 3}); err != nil {
		t.Fatalf("grant that fits host rejected: %v", err)
	}
	if err := validateApplianceIsolationLimits(grant, store.ResourceAmount{PIDs: 2, DiskBytes: 3}, store.ResourceAmount{PIDs: 2, DiskBytes: 3}); err == nil {
		t.Fatal("per-environment PID limit above host capacity was accepted")
	}
	grant.Capacity.PerEnvironmentLimits = verifierLimits(1)
	grant.SystemLimits = verifierLimits(4)
	if err := validateApplianceIsolationLimits(grant, store.ResourceAmount{PIDs: 3, DiskBytes: 3}, store.ResourceAmount{PIDs: 3, DiskBytes: 3}); err == nil {
		t.Fatal("system PID limit above host capacity was accepted")
	}
}

func verifierManifest() *appliancev1.ControllerApplianceManifest {
	return &appliancev1.ControllerApplianceManifest{
		ContractVersion: "1", ProductId: "product", Release: "1",
		ImageReference: "agent-vmm.local/mcp@sha256:" + strings.Repeat("0", 64), ImageManifestDigest: "sha256:" + strings.Repeat("0", 64), Platform: "linux/arm64",
		DataVolumeBytes:        1 << 30,
		StartupDeadlineSeconds: 30,
		SystemLimits:           verifierLimits(4),
		RequestedCapacity:      &appliancev1.CapacityGrant{PerEnvironmentLimits: verifierLimits(2), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		RequestedEgressMode:    hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		RequestedFrontends:     []*appliancev1.ControllerFrontend{{Name: "control", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM, Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, Bounds: &appliancev1.StreamBounds{MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2}}},
	}
}

func verifierGrant() *appliancev1.ApprovedGrant {
	return &appliancev1.ApprovedGrant{
		SystemLimits: verifierLimits(3),
		Capacity:     &appliancev1.CapacityGrant{PerEnvironmentLimits: verifierLimits(2), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL},
		EgressMode:   hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		Frontends:    []*appliancev1.ControllerFrontend{{Name: "control", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM, Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, Bounds: &appliancev1.StreamBounds{MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2}}},
	}
}

func verifierLimits(value uint64) *hostv1.ResourceLimits {
	pids, disk := uint32(value), value
	return &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk}
}

func TestLANCapabilitiesRequireIndependentOperatorApproval(t *testing.T) {
	manifest, grant := verifierManifest(), verifierGrant()
	grant.LanEgress = true
	if err := validateApprovedGrant(manifest, grant); err == nil {
		t.Fatal("unrequested LAN grant accepted")
	}
	manifest.RequestedLanEgress = true
	if err := validateApprovedGrant(manifest, grant); err != nil {
		t.Fatal(err)
	}
	manifest.RequestedFrontends[0].Exposure = appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP
	grant.Frontends[0].Exposure = appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP
	if err := validateApprovedGrant(manifest, grant); err == nil {
		t.Fatal("LAN byte stream frontend accepted")
	}
	manifest.RequestedFrontends[0].Transport = appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP
	grant.Frontends[0].Transport = appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP
	if err := validateApprovedGrant(manifest, grant); err != nil {
		t.Fatal(err)
	}
	grant.Frontends[0].Exposure = appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK
	if err := validateApprovedGrant(manifest, grant); err == nil {
		t.Fatal("frontend exposure substitution accepted")
	}
}

func TestHostTCPEndpointsRequireExactOperatorGrant(t *testing.T) {
	manifest, grant := verifierManifest(), verifierGrant()
	grant.HostTcpEndpoints = []*appliancev1.HostTcpEndpoint{{Ip: "192.168.1.10", Port: 3000}}
	if err := validateApprovedGrant(manifest, grant); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []*appliancev1.HostTcpEndpoint{{Ip: "localhost", Port: 3000}, {Ip: "127.0.0.1", Port: 3000}, {Ip: "169.254.169.254", Port: 3000}, {Ip: "0.0.0.0", Port: 3000}, {Ip: "192.168.1.10", Port: 0}, {Ip: "192.168.1.10", Port: 65536}} {
		grant.HostTcpEndpoints = []*appliancev1.HostTcpEndpoint{endpoint}
		if err := validateApprovedGrant(manifest, grant); err == nil {
			t.Fatalf("invalid endpoint admitted: %+v", endpoint)
		}
	}
	grant.HostTcpEndpoints = []*appliancev1.HostTcpEndpoint{{Ip: "192.168.1.10", Port: 3000}, {Ip: "192.168.1.10", Port: 3000}}
	if err := validateApprovedGrant(manifest, grant); err == nil {
		t.Fatal("duplicate endpoint admitted")
	}
}

func TestFrontendGrantTotalByteOrder(t *testing.T) {
	for _, test := range []struct {
		name                string
		requested, approved uint64
		allowed             bool
	}{
		{"finite to unlimited", 4096, 0, false},
		{"unlimited to finite", 0, 4096, true},
		{"unlimited to unlimited", 0, 0, true},
		{"finite reduction", 4096, 2048, true},
		{"finite expansion", 2048, 4096, false},
		{"above old ceiling", 0, 2 << 30, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requested, approved := verifierManifest().RequestedFrontends[0], verifierGrant().Frontends[0]
			requested.Bounds.MaxTotalBytes, approved.Bounds.MaxTotalBytes = test.requested, test.approved
			if allowed := frontendWithin(approved, requested); allowed != test.allowed {
				t.Fatalf("allowed=%v, expected=%v", allowed, test.allowed)
			}
		})
	}
}
