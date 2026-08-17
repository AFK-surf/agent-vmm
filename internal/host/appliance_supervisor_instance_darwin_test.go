//go:build darwin

package host

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestBootstrapReceiptUsesExactSingleUseIdentity(t *testing.T) {
	receipt := &appliancev1.BootstrapReceipt{InstallationId: "installation", BootstrapId: "bootstrap", Nonce: []byte("nonce")}
	if !validateBootstrapReceipt(receipt, "installation", "bootstrap", []byte("nonce")) {
		t.Fatal("exact bootstrap receipt was rejected")
	}
	for _, changed := range []*appliancev1.BootstrapReceipt{
		{InstallationId: "peer", BootstrapId: "bootstrap", Nonce: []byte("nonce")},
		{InstallationId: "installation", BootstrapId: "stale", Nonce: []byte("nonce")},
		{InstallationId: "installation", BootstrapId: "bootstrap", Nonce: []byte("other")},
	} {
		if validateBootstrapReceipt(changed, "installation", "bootstrap", []byte("nonce")) {
			t.Fatalf("stale receipt was accepted: %+v", changed)
		}
	}
}

func TestBootstrapFailureReasonIsBoundedAndStable(t *testing.T) {
	for _, test := range []struct{ message, want string }{
		{"rpc error: launch claim lifetime exceeds the fixed bound", "BOOTSTRAP_CLAIM_LIFETIME_INVALID"},
		{"rpc error: bootstrap envelope identity is invalid", "BOOTSTRAP_ENVELOPE_IDENTITY_INVALID"},
		{"transport failed with an implementation detail", "BOOTSTRAP_FAILED"},
	} {
		if got := bootstrapFailureReason(errors.New(test.message)); got != test.want {
			t.Fatalf("bootstrapFailureReason(%q) = %q, want %q", test.message, got, test.want)
		}
	}
}

func TestApplianceReconcileRejectsStaleObservedInstance(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, store.ApplianceArtifact{
		ID: "artifact", CanonicalHash: []byte("canonical"), ImageDigest: "sha256:digest", ImageReference: "registry.example/appliance@sha256:digest",
		Platform: "linux/arm64", PublisherKeyID: "publisher", ProductID: "product", Release: "1", Verified: true,
		Manifest: []byte("manifest"), PublisherSignature: []byte("signature"),
	}, store.ApplianceOperation{ID: "stage-operation", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := database.InstallApplianceDomain(ctx,
		store.ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID, PublisherKeyID: artifact.PublisherKeyID, ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1, EncryptedVolumeID: "volume", EncryptedVolumeIdentity: "volume-identity", DataVolumeBytes: 1 << 20},
		store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}, MaxEnvironments: 1, Aggregate: store.ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}, MaxEgressMode: 1, MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1},
		store.ResourceReservation{ID: "reservation", OwnerID: "installation", OwnerKind: store.ReservationOwnerAppliance, ResourceID: "system", ResourceKind: store.ReservationResourceSystem, Amount: store.ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}},
		store.ResourceAmount{CPUMillis: 2, MemoryBytes: 2, PIDs: 2, DiskBytes: 2},
		store.RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1", MaxEnvironments: 1, AggregateCPUMillis: 1, AggregateMemoryBytes: 1, AggregatePIDs: 1, AggregateDiskBytes: 1, PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "installation"},
		nil, store.ApplianceOperation{ID: "install-operation", RequestID: "install", State: 2, Phase: 5}); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	stale := store.ApplianceInstallation{ID: "installation", State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING), Revision: 1, DesiredEnabled: true}
	runtime := &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "installation", BootId: "new-boot", ContainerInstanceId: "new-instance"}, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}
	if err := controller.adoptObservedAppliance(ctx, nil, stale, runtime); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetApplianceInstallation(ctx, "installation")
	if err != nil {
		t.Fatal(err)
	}
	if current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || current.CurrentContainerInstance != "" {
		t.Fatalf("stale observation mutated installation: %+v", current)
	}
}
