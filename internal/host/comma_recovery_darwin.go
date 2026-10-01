//go:build darwin

package host

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/comma_recovery"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

func (server *operatorServer) SignCommaRecovery(ctx context.Context, request *hostv1.SignCommaRecoveryRequest) (*hostv1.SignCommaRecoveryResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	challenge, _, err := comma_recovery.Parse(request.GetChallengeJson(), time.Now())
	if err != nil {
		return nil, invalidField("challenge_json", "a live dedicated Comma recovery challenge is required")
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseAdmission()
	if err := server.controller.requireUndisposedRegistration(ctx, challenge.RegistrationID, ""); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, challenge.RegistrationID)
	if err != nil {
		return nil, disposalReadError(err, challenge.RegistrationID)
	}
	if registration.OwnerKind != store.RemoteRegistrationExternal || registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) || registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		return nil, resourceError(codes.FailedPrecondition, "RECOVERY_TARGET_UNAVAILABLE", "remote_registration", challenge.RegistrationID, "the original local registration is not recoverable")
	}
	if server.controller.trust == nil {
		return nil, resourceError(codes.Unavailable, "DEVICE_IDENTITY_UNAVAILABLE", "device", "local", "device signing is unavailable")
	}
	identity, signature, err := server.controller.trust.SignCommaRecovery(ctx, request.GetChallengeJson())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "RECOVERY_PROOF_UNAVAILABLE", "remote_registration", challenge.RegistrationID, "the original device cannot sign this recovery challenge")
	}
	return &hostv1.SignCommaRecoveryResponse{Identity: identity, Signature: signature}, nil
}
