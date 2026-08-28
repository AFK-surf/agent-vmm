//go:build darwin

package host

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type failingApplianceImportClient struct {
	vmmdv1.VMMDServiceClient
	imports int
}

func (client *failingApplianceImportClient) ImportImage(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[vmmdv1.ImportImageRequest, vmmdv1.Image], error) {
	client.imports++
	return nil, errors.New("guest import unavailable")
}

func TestBundledApplianceArtifactReplacesFailedInstalledArtifact(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	oldManifest := verifierManifest()
	oldManifest.ProductId = "agent-vmm-mcp"
	oldManifest.ImageReference = "agent-vmm.local/mcp@" + oldManifest.GetImageManifestDigest()
	oldCanonical, oldHash, err := (applianceArtifactVerifier{}).verify(oldManifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldArtifact, _, err := database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "old-artifact", CanonicalHash: oldHash,
		ImageDigest: oldManifest.GetImageManifestDigest(), ImageReference: oldManifest.GetImageReference(), Platform: oldManifest.GetPlatform(),
		ProductID: oldManifest.GetProductId(), Release: oldManifest.GetRelease(), Verified: true, Manifest: oldCanonical},
		store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	approved := verifierGrant()
	grant := grantFromProto("agent-vmm-mcp", 3, approved)
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "agent-vmm-mcp", ProductID: oldArtifact.ProductID,
		ArtifactID: oldArtifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
		EncryptedVolumeID: "volume", EncryptedVolumeIdentity: "volume-identity", DataVolumeBytes: 1 << 30}, grant,
		store.ResourceReservation{ID: "reservation", OwnerID: "agent-vmm-mcp", OwnerKind: store.ReservationOwnerAppliance,
			ResourceID: "system", ResourceKind: store.ReservationResourceSystem, Amount: grant.System},
		store.ResourceAmount{CPUMillis: 100, MemoryBytes: 100, PIDs: 100, DiskBytes: 100},
		store.RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1", MaxEnvironments: 2,
			AggregateCPUMillis: 3, AggregateMemoryBytes: 3, AggregatePIDs: 3, AggregateDiskBytes: 3,
			PerEnvCPUMillis: 2, PerEnvMemoryBytes: 2, PerEnvPIDs: 2, PerEnvDiskBytes: 2, MaxEgressMode: 1,
			MaxLeaseSeconds: 60, DisconnectGraceSeconds: 10, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "agent-vmm-mcp"},
		frontendsFromProto(approved.GetFrontends()), store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED)
	installation.CurrentBootID = "old-boot"
	installation.CurrentContainerInstance = "old-instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	newManifest := proto.Clone(oldManifest).(*appliancev1.ControllerApplianceManifest)
	newManifest.Release = "2"
	newManifest.ImageManifestDigest = "sha256:" + strings.Repeat("f", 64)
	newManifest.ImageReference = "agent-vmm.local/mcp@" + newManifest.GetImageManifestDigest()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(newManifest)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	before, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.reconcileBundledApplianceArtifactManifest(ctx, []store.ApplianceInstallation{installation}, encoded, "sha256:"+strings.Repeat("e", 64)); err == nil {
		t.Fatal("manifest with a different Guest image lock was accepted")
	}
	afterRejectedLock, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil || afterRejectedLock.ArtifactID != before.ArtifactID || afterRejectedLock.Revision != before.Revision {
		t.Fatalf("rejected lock changed installation: before=%+v after=%+v err=%v", before, afterRejectedLock, err)
	}
	duplicate := installation
	duplicate.ID = "another-installation"
	if err := controller.reconcileBundledApplianceArtifactManifest(ctx, []store.ApplianceInstallation{installation, duplicate}, encoded, newManifest.GetImageManifestDigest()); err == nil {
		t.Fatal("multiple bundled product installations were accepted")
	}
	afterMultiple, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil || afterMultiple.ArtifactID != before.ArtifactID || afterMultiple.Revision != before.Revision {
		t.Fatalf("ambiguous candidates changed installation: before=%+v after=%+v err=%v", before, afterMultiple, err)
	}
	if err := controller.reconcileBundledApplianceArtifactManifest(ctx, []store.ApplianceInstallation{installation}, encoded, newManifest.GetImageManifestDigest()); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := database.GetApplianceArtifact(ctx, current.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ImageDigest != newManifest.GetImageManifestDigest() || current.Revision != installation.Revision+1 ||
		current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || current.CurrentContainerInstance != "" ||
		current.RegistrationID != installation.RegistrationID || current.EncryptedVolumeID != installation.EncryptedVolumeID {
		t.Fatalf("bundle upgrade did not preserve and fence installation: installation=%+v artifact=%+v", current, artifact)
	}
}

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
		Platform: "linux/arm64", ProductID: "product", Release: "1", Verified: true,
		Manifest: []byte("manifest"),
	}, store.ApplianceOperation{ID: "stage-operation", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := database.InstallApplianceDomain(ctx,
		store.ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID, ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1, EncryptedVolumeID: "volume", EncryptedVolumeIdentity: "volume-identity", DataVolumeBytes: 1 << 20},
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

func TestPrepareMissingApplianceRecoveryAdvancesFenceOnce(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, store.ApplianceArtifact{
		ID: "artifact", CanonicalHash: []byte("canonical"), ImageDigest: "sha256:digest", ImageReference: "cue.local/mcp@sha256:digest",
		Platform: "linux/arm64", ProductID: "product", Release: "1", Verified: true, Manifest: []byte("manifest"),
	}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	amount := store.ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID,
		ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
		EncryptedVolumeID: "volume", EncryptedVolumeIdentity: "volume-identity", DataVolumeBytes: 1 << 30},
		store.ApplianceGrant{Revision: 1, System: amount, MaxEnvironments: 1, Aggregate: amount, PerEnvironment: amount,
			MaxEgressMode: 1, MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1},
		store.ResourceReservation{ID: "reservation", OwnerID: "installation", OwnerKind: store.ReservationOwnerAppliance,
			ResourceID: "system", ResourceKind: store.ReservationResourceSystem, Amount: amount},
		store.ResourceAmount{CPUMillis: 2, MemoryBytes: 2, PIDs: 2, DiskBytes: 2},
		store.RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1", MaxEnvironments: 1,
			AggregateCPUMillis: 1, AggregateMemoryBytes: 1, AggregatePIDs: 1, AggregateDiskBytes: 1,
			PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
			MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "installation"},
		nil, store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	installation.CurrentBootID = "missing-boot"
	installation.CurrentContainerInstance = "missing-instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	support := t.TempDir()
	controller := &Controller{store: database, supportDirectory: support, applianceHealth: make(map[string]*appliancev1.ApplianceHealth), remoteLeases: make(map[string]*remoteLease)}
	prepared, recover, err := controller.prepareApplianceRecovery(ctx, installation)
	if err != nil {
		t.Fatal(err)
	}
	if !recover || prepared.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || prepared.Revision != installation.Revision+1 ||
		prepared.CurrentBootID != "" || prepared.CurrentContainerInstance != "" || prepared.ArtifactID != installation.ArtifactID ||
		prepared.RegistrationID != installation.RegistrationID || prepared.EncryptedVolumeID != installation.EncryptedVolumeID {
		t.Fatalf("missing runtime was not prepared for bounded recovery: before=%+v after=%+v recover=%t", installation, prepared, recover)
	}
	again, recover, err := controller.prepareApplianceRecovery(ctx, installation)
	if err != nil || recover || again.Revision != prepared.Revision {
		t.Fatalf("stale reconcile advanced recovery fence twice: after=%+v recover=%t err=%v", again, recover, err)
	}
	archivePath := controller.applianceArchivePath(artifact)
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, []byte("not-needed-after-import-open"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := new(failingApplianceImportClient)
	if err := controller.recoverApplianceRuntime(ctx, client, prepared, nil); err == nil {
		t.Fatal("bounded recovery unexpectedly succeeded")
	}
	failed, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if client.imports != 1 || failed.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED) || applianceRecoveryState(failed.State) {
		t.Fatalf("recovery did not attempt once and fail closed: imports=%d installation=%+v", client.imports, failed)
	}
}
