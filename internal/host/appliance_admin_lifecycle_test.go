//go:build darwin || linux

package host

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func TestRestartAppliancePersistsLifecycleFailure(t *testing.T) {
	for _, stage := range []string{"stop", "activate"} {
		t.Run(stage, func(t *testing.T) {
			controller, installation := applianceLifecycleFixture(t)

			if stage == "stop" {
				applianceSessionFixture(t, controller, installation.RegistrationID)
			}
			server := controller.appliances
			request := &appliancev1.RestartApplianceRequest{InstallationId: installation.ID, RequestId: "restart-failure", ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
			if _, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.RestartApplianceResponse, error) {
				return server.RestartAppliance(ctx, request)
			}); err == nil {
				t.Fatal("restart discarded lifecycle failure")
			}
			operation, err := controller.store.ApplianceOperationByRequest(context.Background(), request.GetRequestId())
			if err != nil {
				t.Fatal(err)
			}
			if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED) || operation.Stage != "restart-failed" || operation.ErrorReason != "APPLIANCE_RESTART_FAILED" {
				t.Fatalf("restart failure receipt = %+v", operation)
			}
		})
	}
}

func TestHostRestartResumesAdmittedActivationUnderOriginalOperation(t *testing.T) {
	for _, test := range []struct {
		kind   appliancev1.ApplianceOperationKind
		reason string
	}{
		{appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE, "APPLIANCE_START_FAILED"},
		{appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART, "APPLIANCE_RESTART_FAILED"},
	} {
		t.Run(test.kind.String(), func(t *testing.T) {
			controller, installation := applianceLifecycleFixture(t)

			installation.DesiredEnabled = true
			installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING)
			admitted, err := controller.store.BeginApplianceStateMutation(t.Context(), installation, installation.Revision, store.ApplianceOperation{
				ID: "interrupted-operation", InstallationID: installation.ID, RequestID: "interrupted", Kind: int32(test.kind),
				State:            int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
				Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
				ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Stage: "activation-pending",
			})
			if err != nil {
				t.Fatal(err)
			}
			// The resumed activation reaches the lifecycle owner, which rejects
			// it here; the result settles the original operation.
			if err := controller.appliances.RecoverInterruptedApplianceAdminOperations(context.Background()); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				operation, err := controller.store.ApplianceOperationByRequest(context.Background(), "interrupted")
				if err != nil {
					t.Fatal(err)
				}
				if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
					if operation.ID != admitted.ID || operation.ErrorReason != test.reason {
						t.Fatalf("resumed operation = %+v", operation)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Fatal("interrupted activation was not resumed")
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestRevokeFailureRetainsOriginalReceiptWhileBackgroundCleanupSettles(t *testing.T) {
	controller, installation := applianceLifecycleFixture(t)
	registrationBefore, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.SetRemoteEnrollment(context.Background(), registrationBefore.ID, registrationBefore.Revision, registrationBefore.EnrollmentState, int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", ""); err != nil {
		t.Fatal(err)
	}

	applianceSessionFixture(t, controller, installation.RegistrationID)
	server := controller.appliances
	request := &appliancev1.RevokeApplianceRequest{InstallationId: installation.ID, RequestId: "revoke-failure", ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
	_, err = callAsOperator(t, func(ctx context.Context) (*appliancev1.RevokeApplianceResponse, error) {
		return server.RevokeAppliance(ctx, request)
	})
	if err == nil {
		t.Fatal("Revoke discarded cleanup failure")
	}
	operation, err := controller.store.ApplianceOperationByRequest(context.Background(), request.GetRequestId())
	if err != nil || operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED) {
		t.Fatalf("failure receipt: %+v %v", operation, err)
	}
	if err := controller.appliances.RevokePrivateControllerDomain(context.Background(), installation.ID); err != nil {
		t.Fatal(err)
	}
	registration, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		t.Fatalf("background registration fence: %+v %v", registration, err)
	}
	replay, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.RevokeApplianceResponse, error) {
		return server.RevokeAppliance(ctx, request)
	})
	if err != nil || replay.GetOperation().GetOperationId() != operation.ID || replay.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED {
		t.Fatalf("original failed operation replay: %v %v", replay, err)
	}
}

func TestApplianceStatusDoesNotConfirmWriterStopWithoutGuestObservation(t *testing.T) {
	controller, installation := applianceLifecycleFixture(t)
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	var err error
	installation, err = controller.store.UpdateApplianceInstallation(context.Background(), installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.GetApplianceStatusResponse, error) {
		return controller.appliances.GetApplianceStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: installation.ID})
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus().RuntimeStopped != nil {
		t.Fatal("stored STOPPED or a stopped VM was used as Guest writer-stop evidence")
	}
}

func applianceSessionFixture(t *testing.T, controller *Controller, registrationID string) {
	t.Helper()
	const environmentID = "missing-session-environment"
	if _, err := controller.store.CreateEnvironment(t.Context(), store.Environment{ID: environmentID, NamespaceID: "fixture", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	controller.sessions = session.New(testSessionConfig(t, controller))
	t.Cleanup(controller.sessions.Close)
	controller.appliances.BindRuntime(controller.services, controller.sessions, controller.workloads)
	createTestSession(t, controller, environmentID, "", 0, auth.Identity{RegistrationID: registrationID})
	// Remove the durable receipt while the session still has live authority.
	if err := controller.store.DiscardEnvironment(t.Context(), environmentID); err != nil {
		t.Fatal(err)
	}
}

func applianceLifecycleFixture(t *testing.T) (*Controller, store.ApplianceInstallation) {
	t.Helper()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	_, _, err = database.StageApplianceArtifact(t.Context(), store.ApplianceArtifact{ID: "artifact", ProductID: "product", CanonicalHash: []byte("hash"), ImageDigest: "digest", ImageReference: "image", Platform: "linux/arm64", Release: "1"}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(t.Context(),
		store.ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", DesiredEnabled: true, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING), Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1 << 20, CurrentBootID: "boot", CurrentContainerInstance: "instance"},
		store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1},
		store.RemoteRegistration{ID: "registration", InstallationID: "installation", OwnerKind: store.RemoteRegistrationPrivate, DisplayName: "private", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1}, nil,
		store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestControllerWithWorkloads(t, &Controller{store: database, manager: &vmm.Manager{}}, testWorkloadState{workloadRunsRecovered: true})
	controller.vmLifecycle = closedTestLifecycle(t, controller)
	controller.appliances.BindLifecycle(controller.vmLifecycle)
	return controller, installation
}
