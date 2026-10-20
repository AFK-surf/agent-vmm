package store

import (
	"context"
	"database/sql"
	"errors"
	"net/netip"
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
		ProductID: "product", Release: "1", Manifest: []byte("manifest"),
	}, ApplianceOperation{ID: "stage-operation", RequestID: "stage", State: 2, Phase: 5, ExpectedRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	installation, registration, operation, err := database.InstallApplianceDomain(ctx,
		ApplianceInstallation{ID: "installation", ProductID: artifact.ProductID,
			ArtifactID: artifact.ID, State: 2, Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1 << 20, BootstrapID: "bootstrap"},
		ApplianceGrant{Revision: 1, LANEgress: true, HostTCPEndpoints: HostTCPEndpoints{netip.MustParseAddrPort("192.168.1.10:3000")}, System: ResourceAmount{PIDs: 1, DiskBytes: 1},
			PerEnvironment: ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1,
		},
		RemoteRegistration{ID: "registration", Endpoint: "", DisplayName: "private", ProtocolVersion: "1",
			PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
			StopGraceSeconds: 1, State: 1, ConnectionState: 1,
			EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: "installation"},
		[]ApplianceFrontend{{Name: "webhook", Transport: 2, Exposure: 3, MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 2}}, ApplianceOperation{ID: "install-operation", RequestID: "install", State: 1, Phase: 2, ExpectedRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	if installation.ID != "installation" || installation.RegistrationID != registration.ID || operation.ID != "install-operation" || installation.DataVolumeID != "volume" || installation.DataVolumeBytes != 1<<20 {
		t.Fatalf("installed=%+v registration=%+v operation=%+v", installation, registration, operation)
	}
	frontends, err := database.ListApplianceFrontends(ctx, installation.ID)
	if err != nil || len(frontends) != 1 || frontends[0].Exposure != 3 || frontends[0].Transport != 2 {
		t.Fatalf("stored LAN frontend = %+v, %v", frontends, err)
	}
	storedGrant, err := database.GetApplianceGrant(ctx, installation.ID)
	if err != nil || !storedGrant.LANEgress || storedGrant.Revision != 1 || len(storedGrant.HostTCPEndpoints) != 1 || storedGrant.HostTCPEndpoints[0] != netip.MustParseAddrPort("192.168.1.10:3000") {
		t.Fatalf("stored LAN approval = %+v, %v", storedGrant, err)
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
		ProductID: "product", Release: "1", Manifest: []byte("manifest"),
	}, ApplianceOperation{ID: "replacement-stage-operation", RequestID: "replacement-stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	install := func(id string) (ApplianceInstallation, RemoteRegistration) {
		t.Helper()
		amount := ResourceAmount{PIDs: 1, DiskBytes: 1}
		installation, registration, _, installErr := database.InstallApplianceDomain(ctx,
			ApplianceInstallation{ID: id, ProductID: artifact.ProductID,
				ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1,
				DataVolumeID: id + "-volume", DataVolumeBytes: 1 << 20},
			ApplianceGrant{Revision: 1, System: amount,
				PerEnvironment: amount, EgressMode: 1, MaxEgressMode: 1},
			RemoteRegistration{ID: id + "-registration", DisplayName: id, ProtocolVersion: "1",
				PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1,
				StopGraceSeconds: 1, State: 1, ConnectionState: 1,
				EnrollmentState: 3, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: id}, nil,
			ApplianceOperation{ID: id + "-install-operation", RequestID: id + "-install", State: 2, Phase: 5})
		if installErr != nil {
			t.Fatal(installErr)
		}
		return installation, registration
	}
	old, oldRegistration := install("old")
	replacement, replacementRegistration := install("replacement")
	installations, err := database.ListApplianceInstallations(ctx)
	if err != nil || len(installations) != 2 {
		t.Fatalf("installations=%+v err=%v", installations, err)
	}
	if old.RegistrationID != oldRegistration.ID || replacement.RegistrationID != replacementRegistration.ID {
		t.Fatalf("registration bindings old=%+v replacement=%+v", old, replacement)
	}
	pending, err := database.BeginApplianceRemove(ctx, ApplianceRemove{
		InstallationID: old.ID, RegistrationID: oldRegistration.ID,
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
		InstallationID: old.ID, RegistrationID: oldRegistration.ID,
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
