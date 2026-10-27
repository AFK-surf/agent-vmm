//go:build darwin || linux

package appliance

import (
	"context"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

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
