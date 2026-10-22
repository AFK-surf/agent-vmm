//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRestartAppliancePersistsLifecycleFailure(t *testing.T) {
	for _, stage := range []string{"stop", "activate"} {
		t.Run(stage, func(t *testing.T) {
			controller, installation, _, _, _, _ := renewalHost(t, time.Now())
			controller.egress = egress.New()
			controller.manager = &vmm.Manager{}
			controller.vmLifecycle = &VMLifecycleOwner{closing: true}
			if stage == "stop" {
				controller.sessions = map[string]*sessionServer{"missing-session": {registrationID: installation.RegistrationID}}
			}
			server := &applianceAdminServer{controller: controller}
			request := &appliancev1.RestartApplianceRequest{InstallationId: installation.ID, RequestId: "restart-failure", ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
			if _, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
				return server.Restart(ctx, request)
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
			controller, installation, _, _, _, _ := renewalHost(t, time.Now())
			controller.egress = egress.New()
			controller.manager = &vmm.Manager{}
			server := &applianceAdminServer{controller: controller}
			mutation := applianceMutation{installation.ID, "interrupted", installation.Revision, time.Now().Add(time.Minute).UnixMilli()}
			admitted, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
				return server.setDesiredState(ctx, mutation, installation.Revision, true,
					appliancev1.ApplianceState_APPLIANCE_STATE_STARTING, test.kind, "activation-pending")
			})
			if err != nil {
				t.Fatal(err)
			}
			// The resumed activation reaches the lifecycle owner, which rejects
			// it here; the result settles the original operation.
			controller.vmLifecycle = &VMLifecycleOwner{closing: true}
			if err := controller.recoverInterruptedApplianceAdminOperations(context.Background()); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				operation, err := controller.store.ApplianceOperationByRequest(context.Background(), mutation.requestID)
				if err != nil {
					t.Fatal(err)
				}
				if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
					if operation.ID != admitted.GetOperation().GetOperationId() || operation.ErrorReason != test.reason {
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
	controller, installation, _, _, _, _ := renewalHost(t, time.Now())
	registrationBefore, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.SetRemoteEnrollment(context.Background(), registrationBefore.ID, registrationBefore.Revision, registrationBefore.EnrollmentState, int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", ""); err != nil {
		t.Fatal(err)
	}
	controller.manager = &vmm.Manager{}
	controller.egress = egress.New()
	controller.sessions = map[string]*sessionServer{"missing-session": {registrationID: installation.RegistrationID}}
	server := &applianceAdminServer{controller: controller}
	request := &appliancev1.RevokeApplianceRequest{InstallationId: installation.ID, RequestId: "revoke-failure", ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli()}
	_, err = callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.Revoke(ctx, request)
	})
	if err == nil {
		t.Fatal("Revoke discarded cleanup failure")
	}
	operation, err := controller.store.ApplianceOperationByRequest(context.Background(), request.GetRequestId())
	if err != nil || operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED) {
		t.Fatalf("failure receipt: %+v %v", operation, err)
	}
	controller.sessions = nil
	current, err := controller.store.GetApplianceInstallation(context.Background(), installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.reconcileDisabledAppliance(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	registration, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
	if err != nil || registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		t.Fatalf("background registration fence: %+v %v", registration, err)
	}
	replay, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperationResponse, error) {
		return server.Revoke(ctx, request)
	})
	if err != nil || replay.GetOperation().GetOperationId() != operation.ID || replay.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED {
		t.Fatalf("original failed operation replay: %v %v", replay, err)
	}
}

type applianceObservationGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	response *vmmdv1.SystemApplianceResponse
	err      error
}

func (guest *applianceObservationGuest) GetSystemAppliance(context.Context, *vmmdv1.GetSystemApplianceRequest) (*vmmdv1.SystemApplianceResponse, error) {
	return guest.response, guest.err
}

func TestGuestApplianceObservationDistinguishesAbsentStoppedAndUnknown(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *vmmdv1.SystemApplianceResponse
		err      error
		absent   bool
		state    vmmdv1.SystemApplianceState
		failed   bool
	}{
		{name: "absent", err: status.Error(codes.NotFound, "absent"), absent: true},
		{name: "unavailable", err: status.Error(codes.Unavailable, "offline"), failed: true},
		{name: "missing observation", response: &vmmdv1.SystemApplianceResponse{}, failed: true},
		{name: "running", response: &vmmdv1.SystemApplianceResponse{Appliance: &vmmdv1.SystemAppliance{State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}}, state: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING},
		{name: "stopped", response: &vmmdv1.SystemApplianceResponse{Appliance: &vmmdv1.SystemAppliance{State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED}}, state: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := workloadGuestClient(t, &applianceObservationGuest{response: test.response, err: test.err})
			observation, err := getSystemAppliance(context.Background(), client, "source")
			if (err != nil) != test.failed {
				t.Fatalf("observation error=%v", err)
			}
			if !test.failed && ((observation == nil) != test.absent || observation.GetState() != test.state) {
				t.Fatalf("observation=%v", observation)
			}
		})
	}
}

func TestApplianceFailureHelpersRevokeSourceBeforeCleanup(t *testing.T) {
	for _, action := range []string{"failed", "sealed", "invalid-instance", "failed-publication"} {
		t.Run(action, func(t *testing.T) {
			controller, installation, _, _, _, _ := renewalHost(t, time.Now())
			controller.egress = egress.New()
			installation.RegistrationID = ""
			var err error
			installation, err = controller.store.UpdateApplianceInstallation(context.Background(), installation, installation.Revision)
			if err != nil {
				t.Fatal(err)
			}
			source := egress.Source{EnvironmentID: "appliance:" + installation.ID, NamespaceID: "system.test", Subnet: netip.MustParsePrefix("10.64.1.0/24"), LANEgress: true, InstanceID: "instance", GrantRevision: 1, HostTCPEndpoints: []netip.AddrPort{netip.MustParseAddrPort("192.168.1.10:3000")}}
			if err := controller.egress.Policies.Publish(egress.Snapshot{BootID: "boot", Revision: 1, Sources: []egress.Source{source}}); err != nil {
				t.Fatal(err)
			}
			flow, cancel := controller.egress.Policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), source)
			defer cancel()
			switch action {
			case "failed":
				_ = controller.failAppliance(context.Background(), installation, "TEST_FAILED", errors.New("test failure"))
			case "sealed":
				_ = controller.sealAppliance(context.Background(), installation, "TEST_SEALED")
			case "invalid-instance":
				_ = controller.invalidateApplianceRuntime(context.Background(), installation, nil, "TEST_INVALID")
			case "failed-publication":
				controller.revokeFailedAppliancePublication(context.Background(), installation.ID)
			}
			if _, allowed := controller.egress.Policies.Resolve("boot", source.Subnet.Addr().Next()); allowed {
				t.Fatal("invalidated installation retained egress admission")
			}
			select {
			case <-flow.Done():
			case <-time.After(time.Second):
				t.Fatal("invalidated installation retained its existing flow")
			}
		})
	}
}

// A transient publication failure, such as a bootstrap EOF while an adopted
// appliance starts, must not retire its managed authority: the appliance
// presents the same bundle again and an active authority accepts it.
func TestFailedPublicationKeepsManagedTrust(t *testing.T) {
	controller, installation, _, _, _, _ := renewalHost(t, time.Now())
	controller.egress = egress.New()
	controller.revokeFailedAppliancePublication(context.Background(), installation.ID)
	if _, err := controller.store.GetApplianceMembershipCredential(context.Background(), installation.ID); err != nil {
		t.Fatalf("failed publication retired managed trust: %v", err)
	}
}

func TestApplianceStatusDoesNotConfirmWriterStopWithoutGuestObservation(t *testing.T) {
	controller, installation, _, _, _, _ := renewalHost(t, time.Now())
	controller.manager = &vmm.Manager{}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	var err error
	installation, err = controller.store.UpdateApplianceInstallation(context.Background(), installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceStatusResponse, error) {
		return (&applianceAdminServer{controller: controller}).GetStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: installation.ID})
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus().RuntimeStopped != nil {
		t.Fatal("stored STOPPED or a stopped VM was used as Guest writer-stop evidence")
	}
}

func TestPrivateRegistrationPreparationResumesWithoutRevivingRevocation(t *testing.T) {
	for _, state := range []hostv1.RemoteEnrollmentState{hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL, hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE, hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY, hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING} {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			controller, installation, _, _, _, _ := renewalHost(t, time.Now())
			registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
			if err != nil {
				t.Fatal(err)
			}
			registration, err = controller.store.SetRemoteEnrollment(ctx, registration.ID, registration.Revision, registration.EnrollmentState, int32(state), "INTERRUPTED", "interrupted setup")
			if err != nil {
				t.Fatal(err)
			}
			if state == hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_REVOKING {
				if err := controller.preparePrivateRegistration(ctx, installation); err == nil {
					t.Fatal("revocation resumed enrollment")
				}
				current, err := controller.store.GetRemoteRegistration(ctx, registration.ID)
				if err != nil || current.Revision != registration.Revision || current.EnrollmentState != int32(state) {
					t.Fatalf("revocation changed: %+v %v", current, err)
				}
				return
			}
			if err := controller.preparePrivateRegistration(ctx, installation); err != nil {
				t.Fatal(err)
			}
			current, err := controller.store.GetRemoteRegistration(ctx, registration.ID)
			if err != nil || current.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) || current.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) || (state != hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY && (current.EnrollmentErrorCode != "" || current.EnrollmentErrorMessage != "")) {
				t.Fatalf("prepared registration=%+v %v", current, err)
			}
			if err := controller.preparePrivateRegistration(ctx, installation); err != nil {
				t.Fatal(err)
			}
			replay, err := controller.store.GetRemoteRegistration(ctx, registration.ID)
			if err != nil || replay.Revision != current.Revision {
				t.Fatalf("prepare replay changed identity: %+v %v", replay, err)
			}
			foreign := installation
			foreign.ID = "other-installation"
			if err := controller.preparePrivateRegistration(ctx, foreign); err == nil {
				t.Fatal("another installation prepared the registration")
			}
			revoking, err := controller.store.BeginRemoteRevocation(ctx, current.ID, current.Revision)
			if err != nil {
				t.Fatal(err)
			}
			revoked, err := controller.store.CompleteRemoteRevocation(ctx, revoking.ID, revoking.Revision)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.preparePrivateRegistration(ctx, installation); err == nil {
				t.Fatal("revoked registration resumed enrollment")
			}
			final, err := controller.store.GetRemoteRegistration(ctx, revoked.ID)
			if err != nil || final.Revision != revoked.Revision {
				t.Fatalf("revoked registration changed: %+v %v", final, err)
			}
		})
	}
}
