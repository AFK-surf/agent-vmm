//go:build darwin || linux

package cleanup

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) DiscardRegistrationEnvironments(ctx context.Context, client vmmdv1.VMMDServiceClient, parentID, registrationID string) error {
	environments, err := server.store.ListRemoteEnvironments(ctx, registrationID)
	if err != nil {
		return err
	}
	for _, environment := range environments {
		plan, err := server.planCleanupWithClient(ctx, client, environment.ID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD, true)
		if err != nil {
			return err
		}
		suffix, err := opreceipt.NewID()
		if err != nil {
			return err
		}
		requestID := "remove-" + parentID + "-" + environment.ID + "-" + suffix
		child, err := server.executeWithClient(ctx, client, cleanupActor{remoteOwner: true, confirmedVolumes: cleanupPlanVolumeIDs(plan)}, cleanupRequest{id: requestID, plan: plan})
		if err != nil {
			return err
		}
		if child.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
			return status.Error(codes.Unavailable, child.ErrorMessage)
		}
	}
	return nil
}
