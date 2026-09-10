package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

func TestInstallApplianceDomainLinksRegistrationBeforeInstallation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, ApplianceArtifact{
		ID: "artifact", CanonicalHash: []byte("canonical"), ImageDigest: "sha256:digest",
		ImageReference: "registry.example/appliance@sha256:digest", Platform: "linux/arm64",
		ProductID: "product", Release: "1", Verified: true, Manifest: []byte("manifest"),
	}, ApplianceOperation{ID: "stage-operation", RequestID: "stage", State: 2, Phase: 5, ExpectedRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	installation, registration, operation, err := database.InstallApplianceDomain(ctx,
		ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID,
			ArtifactID: artifact.ID, State: 2, Revision: 1, EncryptedVolumeID: "volume", EncryptedVolumeIdentity: "volume-identity", DataVolumeBytes: 1 << 20, BootstrapID: "bootstrap"},
		ApplianceGrant{Revision: 1, System: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1},
			MaxEnvironments: 1, PerEnvironment: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1,
			MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1},
		ResourceReservation{ID: "reservation", OwnerID: "installation", OwnerKind: ReservationOwnerAppliance,
			ResourceID: "system", ResourceKind: ReservationResourceSystem, Amount: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}},
		ResourceAmount{CPUMillis: 2, MemoryBytes: 2, PIDs: 2, DiskBytes: 2},
		RemoteRegistration{ID: "registration", Endpoint: "", DisplayName: "private", ProtocolVersion: "1",
			MaxEnvironments: 1, PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
			MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: "installation"},
		nil, ApplianceOperation{ID: "install-operation", RequestID: "install", State: 1, Phase: 2, ExpectedRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	if installation.ID != "installation" || installation.RegistrationID != registration.ID || operation.ID != "install-operation" {
		t.Fatalf("installed=%+v registration=%+v operation=%+v", installation, registration, operation)
	}
	installation.DesiredEnabled = true
	installation.State = 3
	if _, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{
		ID: "install-operation", InstallationID: installation.ID, RequestID: "activate", State: 1, Phase: 2,
	}); err == nil {
		t.Fatal("duplicate operation receipt unexpectedly succeeded")
	}
	unchanged, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Revision != installation.Revision || unchanged.DesiredEnabled {
		t.Fatalf("installation fence changed after receipt failure: %+v", unchanged)
	}
	if _, err := database.BeginApplianceRemove(ctx, ApplianceRemove{
		InstallationID: installation.ID, OperationID: "remove-operation", ExpectedRevision: installation.Revision + 1,
		ExpectedState: installation.State, PendingState: 4, ExpectedPhase: 2,
		Operation: ApplianceOperation{ID: "remove-operation", InstallationID: installation.ID, RequestID: "remove", State: 1, Phase: 2, ExpectedRevision: installation.Revision + 1},
	}); !errors.Is(err, ErrInstallationRevisionConflict) {
		t.Fatalf("expected remove fence conflict, got %v", err)
	}
	if _, err := database.GetApplianceOperation(ctx, "remove-operation"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rolled-back remove operation is present: %v", err)
	}
}

func TestCommitSealedRepairFencesInstallationAndReceiptAtomically(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, ApplianceArtifact{
		ID: "sealed-artifact", CanonicalHash: []byte("canonical"), ImageDigest: "sha256:digest",
		ImageReference: "registry.example/appliance@sha256:digest", Platform: "linux/arm64",
		ProductID: "product", Release: "1", Verified: true, Manifest: []byte("manifest"),
	}, ApplianceOperation{ID: "sealed-stage-operation", RequestID: "sealed-stage", State: 2, Phase: 5, ExpectedRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx,
		ApplianceInstallation{ID: "sealed-installation", ProductID: artifact.ProductID,
			ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
			EncryptedVolumeID: "sealed-volume", EncryptedVolumeIdentity: "sealed-volume-identity", DataVolumeBytes: 1 << 20, BootstrapID: ""},
		ApplianceGrant{Revision: 1, System: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1},
			MaxEnvironments: 1, PerEnvironment: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1,
			MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1},
		ResourceReservation{ID: "sealed-reservation", OwnerID: "sealed-installation", OwnerKind: ReservationOwnerAppliance,
			ResourceID: "sealed-system", ResourceKind: ReservationResourceSystem, Amount: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}},
		ResourceAmount{CPUMillis: 2, MemoryBytes: 2, PIDs: 2, DiskBytes: 2},
		RemoteRegistration{ID: "sealed-registration", Endpoint: "", DisplayName: "sealed", ProtocolVersion: "1",
			MaxEnvironments: 1, PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
			MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: "sealed-installation"},
		nil, ApplianceOperation{ID: "sealed-install-operation", RequestID: "sealed-install", State: 2, Phase: 5, ExpectedRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
	installation.DesiredEnabled = false
	sealed, err := database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := database.CreateApplianceOperation(ctx, ApplianceOperation{
		ID: "sealed-repair-operation", InstallationID: sealed.ID, RequestID: "sealed-repair",
		State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED), ExpectedRevision: sealed.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := database.CommitSealedRepair(ctx, sealed.ID, sealed.Revision, operation.ID, []byte("receipt"))
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED) ||
		committed.Phase != int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL) {
		t.Fatalf("committed operation=%+v", committed)
	}
	updated, err := database.GetApplianceInstallation(ctx, sealed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || updated.Revision != sealed.Revision+1 || updated.CurrentContainerInstance != "" {
		t.Fatalf("updated installation=%+v", updated)
	}
}

func TestNewInstallationReplacementAndRemovalLeaveOtherDomainIntact(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	artifact, _, err := database.StageApplianceArtifact(ctx, ApplianceArtifact{
		ID: "replacement-artifact", CanonicalHash: []byte("replacement-canonical"), ImageDigest: "sha256:replacement",
		ImageReference: "registry.example/appliance@sha256:replacement", Platform: "linux/arm64",
		ProductID: "product", Release: "1", Verified: true, Manifest: []byte("manifest"),
	}, ApplianceOperation{ID: "replacement-stage-operation", RequestID: "replacement-stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	install := func(id string) (ApplianceInstallation, RemoteRegistration, ResourceReservation) {
		t.Helper()
		reservation := ResourceReservation{ID: id + "-reservation", OwnerID: id, OwnerKind: ReservationOwnerAppliance,
			ResourceID: id + "-system", ResourceKind: ReservationResourceSystem,
			Amount: ResourceAmount{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}}
		installation, registration, _, installErr := database.InstallApplianceDomain(ctx,
			ApplianceInstallation{ID: id, ProductID: artifact.ProductID,
				ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
				EncryptedVolumeID: id + "-volume", EncryptedVolumeIdentity: id + "-volume-identity", DataVolumeBytes: 1 << 20},
			ApplianceGrant{Revision: 1, System: reservation.Amount, MaxEnvironments: 1,
				PerEnvironment: reservation.Amount, EgressMode: 1, MaxEgressMode: 1, MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1},
			reservation, ResourceAmount{CPUMillis: 10, MemoryBytes: 10, PIDs: 10, DiskBytes: 10},
			RemoteRegistration{ID: id + "-registration", DisplayName: id, ProtocolVersion: "1", MaxEnvironments: 1,
				PerEnvCPUMillis: 1, PerEnvMemoryBytes: 1, PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
				MaxLeaseSeconds: 1, DisconnectGraceSeconds: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
				EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: id}, nil,
			ApplianceOperation{ID: id + "-install-operation", RequestID: id + "-install", State: 2, Phase: 5})
		if installErr != nil {
			t.Fatal(installErr)
		}
		return installation, registration, reservation
	}
	old, oldRegistration, oldReservation := install("old")
	replacement, replacementRegistration, _ := install("replacement")
	installations, err := database.ListApplianceInstallations(ctx)
	if err != nil || len(installations) != 2 {
		t.Fatalf("installations=%+v err=%v", installations, err)
	}
	if old.RegistrationID != oldRegistration.ID || replacement.RegistrationID != replacementRegistration.ID {
		t.Fatalf("registration bindings old=%+v replacement=%+v", old, replacement)
	}
	pending, err := database.BeginApplianceRemove(ctx, ApplianceRemove{
		InstallationID: old.ID, RegistrationID: oldRegistration.ID, ReservationID: oldReservation.ID,
		OperationID: "old-remove-operation", ExpectedRevision: old.Revision, ExpectedState: old.State,
		PendingState: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPING), ExpectedPhase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
		Receipt: []byte("pending-destroy:old"), Operation: ApplianceOperation{ID: "old-remove-operation", InstallationID: old.ID,
			RequestID: "old-remove", State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED), ExpectedRevision: old.Revision},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pending.Revision != old.Revision+1 {
		t.Fatalf("pending removal revision=%d", pending.Revision)
	}
	if _, err := database.CommitApplianceRemove(ctx, ApplianceRemove{
		InstallationID: old.ID, RegistrationID: oldRegistration.ID, ReservationID: oldReservation.ID,
		OperationID: "old-remove-operation", CommitRevision: pending.Revision, Receipt: []byte("removed"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetApplianceInstallation(ctx, old.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old installation still exists: %v", err)
	}
	if _, err := database.GetRemoteRegistration(ctx, oldRegistration.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old registration still exists: %v", err)
	}
	current, err := database.GetApplianceInstallation(ctx, replacement.ID)
	if err != nil || current.RegistrationID != replacementRegistration.ID || current.ArtifactID != artifact.ID {
		t.Fatalf("replacement changed by old removal: %+v err=%v", current, err)
	}
}

func TestSwitchBundledApplianceArtifactPreservesExactDomainAndIsIdempotent(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	oldArtifact, _, err := database.StageApplianceArtifact(ctx, ApplianceArtifact{
		ID: "old-artifact", CanonicalHash: []byte("old-canonical"), ImageDigest: "sha256:old",
		ImageReference: "cue.local/mcp@sha256:old", Platform: "linux/arm64",
		ProductID: "agent-vmm-mcp", Release: "old", Verified: true, Manifest: []byte("old-manifest"),
	}, ApplianceOperation{ID: "stage-old", RequestID: "stage-old", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	grant := ApplianceGrant{Revision: 7, System: ResourceAmount{CPUMillis: 1, MemoryBytes: 2, PIDs: 3, DiskBytes: 4},
		MaxEnvironments: 1, PerEnvironment: ResourceAmount{CPUMillis: 5, MemoryBytes: 6, PIDs: 7, DiskBytes: 8}, EgressMode: 1, MaxEgressMode: 2,
		MaxLeaseSeconds: 9, DisconnectGraceSeconds: 10}
	installation, _, _, err := database.InstallApplianceDomain(ctx,
		ApplianceInstallation{ID: "installation", ProductID: oldArtifact.ProductID, ArtifactID: oldArtifact.ID,
			State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
			RegistrationID: "registration", EncryptedVolumeID: "volume", EncryptedVolumeIdentity: "volume-identity",
			DataVolumeBytes: 1 << 30},
		grant, ResourceReservation{ID: "reservation", OwnerID: "installation", OwnerKind: ReservationOwnerAppliance,
			ResourceID: "system", ResourceKind: ReservationResourceSystem, Amount: grant.System},
		ResourceAmount{CPUMillis: 20, MemoryBytes: 20, PIDs: 20, DiskBytes: 20},
		RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1", MaxEnvironments: 1,
			PerEnvCPUMillis: 5, PerEnvMemoryBytes: 6, PerEnvPIDs: 7, PerEnvDiskBytes: 8, MaxEgressMode: 1,
			MaxLeaseSeconds: 9, DisconnectGraceSeconds: 10, StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: "installation"},
		nil, ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
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
	newArtifact := ApplianceArtifact{ID: "new-artifact", CanonicalHash: []byte("new-canonical"), ImageDigest: "sha256:new",
		ImageReference: "cue.local/mcp@sha256:new", Platform: "linux/arm64", ProductID: oldArtifact.ProductID,
		Release: "new", Verified: true, Manifest: []byte("new-manifest")}
	switched, err := database.SwitchBundledApplianceArtifact(ctx, installation.ID, installation.Revision, installation.ArtifactID,
		installation.State, int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), newArtifact)
	if err != nil {
		t.Fatal(err)
	}
	current := switched
	if current.ID != installation.ID || current.ArtifactID != newArtifact.ID || current.Revision != installation.Revision+1 ||
		current.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) || current.CurrentBootID != "" || current.CurrentContainerInstance != "" ||
		current.RegistrationID != installation.RegistrationID || current.EncryptedVolumeID != installation.EncryptedVolumeID ||
		current.EncryptedVolumeIdentity != installation.EncryptedVolumeIdentity || !current.DesiredEnabled {
		t.Fatalf("installation not atomically switched: before=%+v after=%+v", installation, current)
	}
	keptGrant, err := database.GetApplianceGrant(ctx, installation.ID)
	if err != nil || keptGrant.Revision != grant.Revision || keptGrant.System != grant.System ||
		keptGrant.EgressMode != grant.EgressMode || keptGrant.MaxEgressMode != grant.MaxEgressMode {
		t.Fatalf("grant changed: %+v err=%v", keptGrant, err)
	}
	volume, err := database.GetApplianceVolume(ctx, installation.ID)
	if err != nil || volume.VolumeID != installation.EncryptedVolumeID || volume.Identity != installation.EncryptedVolumeIdentity {
		t.Fatalf("volume changed: %+v err=%v", volume, err)
	}
	again, err := database.SwitchBundledApplianceArtifact(ctx, current.ID, current.Revision, current.ArtifactID,
		current.State, int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), newArtifact)
	if err != nil {
		t.Fatalf("idempotent switch=%+v err=%v", again, err)
	}
	unchanged, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil || unchanged.Revision != current.Revision {
		t.Fatalf("idempotent switch advanced fence: %+v err=%v", unchanged, err)
	}
	unchanged.DesiredEnabled = false
	unchanged.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
	sealed, err := database.UpdateApplianceInstallation(ctx, unchanged, unchanged.Revision)
	if err != nil {
		t.Fatal(err)
	}
	rejectedArtifact := ApplianceArtifact{ID: "rejected-artifact", CanonicalHash: []byte("rejected-canonical"), ImageDigest: "sha256:rejected",
		ImageReference: "agent-vmm.local/mcp@sha256:rejected", Platform: "linux/arm64", ProductID: sealed.ProductID,
		Release: "rejected", Verified: true, Manifest: []byte("rejected-manifest")}
	if _, err := database.SwitchBundledApplianceArtifact(ctx, sealed.ID, sealed.Revision, sealed.ArtifactID, sealed.State,
		int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), rejectedArtifact); !errors.Is(err, ErrBundledInstallationConflict) {
		t.Fatalf("sealed installation switch error=%v", err)
	}
	stillSealed, err := database.GetApplianceInstallation(ctx, sealed.ID)
	if err != nil || stillSealed.State != sealed.State || stillSealed.ArtifactID != sealed.ArtifactID || stillSealed.Revision != sealed.Revision {
		t.Fatalf("sealed installation changed: before=%+v after=%+v err=%v", sealed, stillSealed, err)
	}
	if _, err := database.GetApplianceArtifactByCanonicalHash(ctx, rejectedArtifact.CanonicalHash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected artifact was persisted: %v", err)
	}
}
