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

func (server *operatorServer) StopContainer(ctx context.Context, request *hostv1.OperatorStopContainerRequest) (*hostv1.ContainerResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
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
	response, err := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId(), TimeoutSeconds: request.TimeoutSeconds})
	if err != nil {
		return nil, err
	}
	return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
}

func (server *operatorServer) ContinueContainer(ctx context.Context, request *hostv1.OperatorContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
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
	response, err := client.StartContainer(ctx, &vmmdv1.StartContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
}

func (server *operatorServer) SignalContainer(ctx context.Context, request *hostv1.OperatorSignalContainerRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
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
	if _, err := client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId(), Signal: vmmdv1.ProcessSignal(request.GetSignal())}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *operatorServer) DeleteContainer(ctx context.Context, request *hostv1.OperatorDeleteContainerRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetExpectedGenerationId() == "" {
		return nil, invalidField("expected_generation_id", "container generation is required")
	}
	unlock := server.controller.lockEnvironment(request.GetEnvironmentId())
	defer unlock()
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
	_, err = client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID,
		ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId(),
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
