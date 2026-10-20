package store

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"testing"
	"time"
)

func updateStoreFixture(t *testing.T) (*Store, ApplianceInstallation, ApplianceGrant) {
	t.Helper()
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	ctx := context.Background()
	for _, id := range []string{"source", "target"} {
		_, _, err = database.StageApplianceArtifact(ctx, ApplianceArtifact{ID: id, CanonicalHash: []byte(id), ImageDigest: id, ImageReference: id, Platform: "linux/arm64", ProductID: "product", Release: id, Manifest: []byte(id)}, ApplianceOperation{ID: "stage-" + id, RequestID: "stage-" + id, State: 2, Phase: 5})
		if err != nil {
			t.Fatal(err)
		}
	}
	grant := ApplianceGrant{Revision: 1, System: ResourceAmount{PIDs: 1, DiskBytes: 2}, PerEnvironment: ResourceAmount{PIDs: 3, DiskBytes: 4}, EgressMode: 1, MaxEgressMode: 1}
	installation, _, _, err := database.InstallApplianceDomain(ctx, ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "source", State: 1, Revision: 1, RegistrationID: "registration", DataVolumeID: "volume", DataVolumeBytes: 1024}, grant, RemoteRegistration{ID: "registration", DisplayName: "private", ProtocolVersion: "1", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 2, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1, OwnerKind: RemoteRegistrationPrivate, InstallationID: "installation"}, nil, ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	return database, installation, grant
}

func TestApplianceUpdateSwitchIsAtomicAndRecoverKeepsPhase(t *testing.T) {
	for _, target := range []string{"source", "target"} {
		t.Run(target, func(t *testing.T) {
			database, installation, grant := updateStoreFixture(t)
			ctx := context.Background()
			var err error
			installation.DesiredEnabled = false
			installation.State = 4
			installation.CurrentBootID = "source-boot"
			installation.CurrentContainerInstance = "source-instance"
			operation, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{ID: "update", InstallationID: installation.ID, RequestID: "update", Kind: 9, State: 1, Phase: 9, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Receipt: []byte("original-target-and-grant")})
			if err != nil {
				t.Fatal(err)
			}
			before, err := database.GetApplianceInstallation(ctx, installation.ID)
			if err != nil {
				t.Fatal(err)
			}
			grant.Revision++
			grant.PerEnvironment.PIDs = 7
			grant.HostTCPEndpoints = HostTCPEndpoints{netip.MustParseAddrPort("192.168.1.10:3000")}
			if _, err = database.CommitApplianceUpdateSwitch(ctx, operation, "source", before.Revision+1, target, grant, nil); !errors.Is(err, ErrInstallationRevisionConflict) {
				t.Fatalf("stale fence: %v", err)
			}
			unchanged, _ := database.GetApplianceInstallation(ctx, installation.ID)
			if unchanged.ArtifactID != "source" || unchanged.Revision != before.Revision {
				t.Fatal("failed switch changed installation")
			}
			switched, err := database.CommitApplianceUpdateSwitch(ctx, operation, "source", before.Revision, target, grant, []ApplianceFrontend{{Name: "http", Transport: 1, Exposure: 1, MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 2, MaxConcurrentStreams: 1}})
			if err != nil {
				t.Fatal(err)
			}
			current, err := database.GetApplianceInstallation(ctx, installation.ID)
			if err != nil || current.ArtifactID != target || current.Revision != before.Revision+1 || current.State != 2 || !current.DesiredEnabled || current.CurrentBootID != "" || current.DataVolumeID != before.DataVolumeID || current.RegistrationID != before.RegistrationID || switched.Phase != 10 {
				t.Fatalf("switch=%+v operation=%+v err=%v", current, switched, err)
			}
			kept, _ := database.GetApplianceGrant(ctx, installation.ID)
			registration, _ := database.GetRemoteRegistration(ctx, installation.RegistrationID)
			if kept.Revision != grant.Revision || len(kept.HostTCPEndpoints) != 1 || kept.HostTCPEndpoints[0] != grant.HostTCPEndpoints[0] || registration.PerEnvPIDs != 7 || registration.State != 2 {
				t.Fatalf("grant/registration not committed: %+v %+v", kept, registration)
			}
			failed, err := database.TransitionApplianceOperation(ctx, operation.ID, ApplianceOperationTransition{ExpectedState: 1, ExpectedPhase: 10, State: 3, Phase: 10, Stage: "activating", Receipt: switched.Receipt, ErrorReason: "HEALTH_FAILED"})
			if err != nil {
				t.Fatal(err)
			}
			originalDeadline := failed.DeadlineUnixMillis
			failed.DeadlineUnixMillis += 60000
			if _, err = database.ResumeApplianceUpdate(ctx, failed, failed.UpdatedAt.Add(-time.Nanosecond)); !errors.Is(err, ErrOperationReceiptConflict) {
				t.Fatalf("stale recovery=%v", err)
			}
			resumed, err := database.ResumeApplianceUpdate(ctx, failed, failed.UpdatedAt)
			if err != nil || resumed.Phase != 10 || resumed.State != 1 || resumed.DeadlineUnixMillis != originalDeadline+60000 || string(resumed.Receipt) != string(operation.Receipt) {
				t.Fatalf("recovery=%+v err=%v", resumed, err)
			}
		})
	}
}

func TestUnfinishedApplianceUpdateCanExitThroughExplicitLifecycle(t *testing.T) {
	for _, state := range []int32{3, 5, 6} {
		for _, kind := range []int32{4, 6, 7} {
			t.Run(fmt.Sprintf("state=%d/kind=%d", state, kind), func(t *testing.T) {
				database, installation, _ := updateStoreFixture(t)
				ctx := context.Background()
				installation.State = state
				installation.DesiredEnabled = false
				installation, err := database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
				if err != nil {
					t.Fatal(err)
				}
				old, err := database.CreateApplianceOperation(ctx, ApplianceOperation{ID: "failed-update", InstallationID: installation.ID, RequestID: "failed-update", Kind: 9, State: map[int32]int32{3: 1, 5: 3, 6: 3}[state], Phase: 10, Receipt: []byte("original target and grant"), ErrorReason: "TARGET_HEALTH_FAILED", ErrorMessage: "failed health"})
				if err != nil {
					t.Fatal(err)
				}
				action := ApplianceOperation{ID: "operator-action", InstallationID: installation.ID, RequestID: "operator-action", Kind: kind, State: 1, Phase: 1}
				if kind == 7 {
					_, err = database.BeginApplianceRemove(ctx, ApplianceRemove{InstallationID: installation.ID, OperationID: action.ID, Operation: action, ExpectedRevision: installation.Revision, ExpectedState: state, PendingState: 4, ExpectedPhase: 1, Receipt: []byte("remove-confirmation")})
				} else {
					_, err = database.BeginApplianceStateMutation(ctx, installation, installation.Revision, action)
				}
				if err != nil {
					t.Fatal(err)
				}
				interrupted, err := database.GetApplianceOperation(ctx, old.ID)
				if err != nil || interrupted.State != 4 || interrupted.Phase != 10 || interrupted.TerminalReason == 0 || string(interrupted.Receipt) != string(old.Receipt) || interrupted.ErrorReason != old.ErrorReason {
					t.Fatalf("lost failed update evidence: %+v err=%v", interrupted, err)
				}
				current, err := database.GetApplianceInstallation(ctx, installation.ID)
				if err != nil || current.ArtifactID != installation.ArtifactID || current.DataVolumeID != installation.DataVolumeID || current.Revision != installation.Revision+1 {
					t.Fatalf("operator exit changed data/target: %+v %v", current, err)
				}
				pending, err := database.HasPendingApplianceUpdate(ctx, installation.ID)
				if err != nil || pending {
					t.Fatalf("interrupted update still excludes administration: %t %v", pending, err)
				}
			})
		}
	}
}

func TestApplianceUpdateRecoverAndStopSerializeOnFailedReceipt(t *testing.T) {
	database, installation, _ := updateStoreFixture(t)
	ctx := context.Background()
	failed, err := database.CreateApplianceOperation(ctx, ApplianceOperation{ID: "update", InstallationID: installation.ID, RequestID: "update", Kind: 9, State: 3, Phase: 10, Receipt: []byte("target/grant"), DeadlineUnixMillis: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	resumed := failed
	resumed.DeadlineUnixMillis += 60000
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := database.ResumeApplianceUpdate(ctx, resumed, failed.UpdatedAt)
		results <- err
	}()
	go func() {
		<-start
		_, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{ID: "stop", InstallationID: installation.ID, RequestID: "stop", Kind: 4, State: 1, Phase: 1})
		results <- err
	}()
	close(start)
	first, second := <-results, <-results
	if first != nil && second != nil {
		t.Fatalf("neither recovery nor stop succeeded: %v / %v", first, second)
	}
	interrupted, err := database.GetApplianceOperation(ctx, failed.ID)
	if err != nil || interrupted.State != 4 {
		t.Fatalf("explicit stop did not settle update: %+v %v", interrupted, err)
	}
	operation, err := database.GetApplianceOperation(ctx, failed.ID)
	if err != nil || (operation.State != 1 && operation.State != 4) || operation.Phase != failed.Phase || string(operation.Receipt) != string(failed.Receipt) {
		t.Fatalf("race lost phase/receipt: %+v %v", operation, err)
	}
	if operation.State == 1 {
		if _, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{ID: "retry-stop", InstallationID: installation.ID, RequestID: "retry-stop", Kind: 4, State: 1, Phase: 1}); !errors.Is(err, ErrApplianceOperationInProgress) {
			t.Fatalf("running update admitted stop: %v", err)
		}
	} else {
		if _, err := database.ResumeApplianceUpdate(ctx, resumed, failed.UpdatedAt); !errors.Is(err, ErrOperationReceiptConflict) {
			t.Fatalf("terminated update resumed: %v", err)
		}
	}
}

func TestApplianceUpdatePreparationKeepsSourceUntilAtomicDrain(t *testing.T) {
	database, installation, _ := updateStoreFixture(t)
	ctx := context.Background()
	installation.DesiredEnabled = true
	installation.State = 3
	installation.CurrentBootID = "old-boot"
	installation.CurrentContainerInstance = "old-instance"
	installation, err := database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{ID: "prepare-update", InstallationID: installation.ID, RequestID: "prepare-update", Kind: 9, State: 1, Phase: 6, Receipt: []byte("preparing")})
	if err != nil {
		t.Fatal(err)
	}
	preparing, err := database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil || !preparing.DesiredEnabled || preparing.State != 3 || preparing.CurrentBootID != installation.CurrentBootID || preparing.CurrentContainerInstance != installation.CurrentContainerInstance || preparing.Revision != installation.Revision+1 {
		t.Fatalf("preparation withdrew source: %+v %v", preparing, err)
	}
	operation.Receipt = []byte("draining")
	if _, err := database.CommitApplianceUpdatePreparation(ctx, operation, installation.ArtifactID, preparing.Revision+1); !errors.Is(err, ErrInstallationRevisionConflict) {
		t.Fatalf("stale preparation: %v", err)
	}
	unchanged, _ := database.GetApplianceOperation(ctx, operation.ID)
	if unchanged.Phase != 6 || string(unchanged.Receipt) != "preparing" {
		t.Fatalf("failed transaction advanced operation: %+v", unchanged)
	}
	drained, err := database.CommitApplianceUpdatePreparation(ctx, operation, installation.ArtifactID, preparing.Revision)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := database.GetApplianceInstallation(ctx, installation.ID)
	if drained.Phase != 7 || string(drained.Receipt) != "draining" || current.DesiredEnabled || current.State != 4 || current.Revision != preparing.Revision+1 {
		t.Fatalf("drain not atomic: %+v %+v", current, drained)
	}
}
