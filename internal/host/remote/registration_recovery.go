//go:build darwin || linux

package remote

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) SignRegistrationRecovery(ctx context.Context, request *hostv1.SignRegistrationRecoveryRequest) (*hostv1.SignRegistrationRecoveryResponse, error) {
	challenge, _, err := trust.Parse(request.GetChallengeJson(), time.Now())
	if err != nil {
		return nil, hostv1.InvalidField("challenge_json", "a live registration recovery challenge is required")
	}
	ctx, releaseInventory, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	releaseAdmission := server.LockAdmission()
	defer releaseAdmission()
	if err := server.cleanup.RequireUndisposedRegistration(ctx, challenge.RegistrationID, ""); err != nil {
		return nil, err
	}
	registration, err := server.store.GetRemoteRegistration(ctx, challenge.RegistrationID)
	if err != nil {
		return nil, cleanup.ReadError(err)
	}
	if registration.OwnerKind != store.RemoteRegistrationExternal || registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		return nil, status.Error(codes.FailedPrecondition, "the original local registration is not recoverable")
	}
	identity, signature, err := server.trust.SignRegistrationRecovery(ctx, request.GetChallengeJson())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "the original device cannot sign this recovery challenge")
	}
	return &hostv1.SignRegistrationRecoveryResponse{Identity: identity, Signature: signature}, nil
}
