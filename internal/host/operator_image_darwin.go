//go:build darwin

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (server *operatorServer) DeleteEnvironmentImage(ctx context.Context, request *hostv1.DeleteEnvironmentImageRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetEnvironmentId() == "" || request.GetReference() == "" || request.GetExpectedGenerationId() == "" || request.GetExpectedDigest() == "" {
		return nil, invalidField("request_id/environment_id/reference/expected_generation_id/expected_digest", "environment, image identity, and request ID are required")
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	unlock := server.controller.lockEnvironment(request.GetEnvironmentId())
	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", request.GetEnvironmentId(), "agent environment does not exist")
	}
	if err := requireLocalOperatorEnvironment(environment); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	_, err = client.DeleteImage(ctx, &vmmdv1.DeleteImageRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, Reference: request.GetReference(),
		ExpectedGenerationId: request.GetExpectedGenerationId(), ExpectedDigest: request.GetExpectedDigest(),
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
