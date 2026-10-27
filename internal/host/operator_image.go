//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *operatorServer) DeleteEnvironmentImage(ctx context.Context, request *hostv1.DeleteEnvironmentImageRequest) (*hostv1.DeleteEnvironmentImageResponse, error) {
	if request.GetRequestId() == "" || request.GetEnvironmentId() == "" || request.GetReference() == "" || request.GetExpectedGenerationId() == "" || request.GetExpectedDigest() == "" {
		return nil, hostv1.InvalidField("request_id/environment_id/reference/expected_generation_id/expected_digest", "environment, image identity, and request ID are required")
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, request.GetEnvironmentId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "agent environment does not exist")
	}
	if err := requireLocalOperatorEnvironment(environment); err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	_, err = client.DeleteImage(ctx, &vmmdv1.DeleteImageRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, Reference: request.GetReference(),
		ExpectedGenerationId: request.GetExpectedGenerationId(), ExpectedDigest: request.GetExpectedDigest(),
	})
	if err != nil {
		return nil, err
	}
	return &hostv1.DeleteEnvironmentImageResponse{}, nil
}
