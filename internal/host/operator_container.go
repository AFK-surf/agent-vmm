//go:build darwin || linux

package host

import (
	"context"
	"errors"

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
	if request.GetRequestId() == "" || request.GetContainerId() == "" {
		return nil, invalidField("request_id/container_id", "request and container IDs are required")
	}
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", request.GetEnvironmentId(), "agent environment does not exist")
	}
	if err := requireLocalOperatorEnvironment(environment); err != nil {
		return nil, err
	}
	if request.ExpectedIntegrationRevision != nil || request.IntegrationId != nil {
		if request.GetIntegrationId() == "" || request.ExpectedIntegrationRevision == nil || request.GetExpectedIntegrationRevision() == 0 {
			return nil, invalidField("integration_id/expected_integration_revision", "exact integration intent is required")
		}
		unlock, err := server.controller.lockIntegrationRead(ctx, request.GetIntegrationId())
		if err != nil {
			return nil, err
		}
		defer unlock()
		integration, err := server.controller.store.GetAgentIntegration(ctx, request.GetIntegrationId())
		if err != nil || integration.EnvironmentID != environment.ID || integration.Revision != request.GetExpectedIntegrationRevision() || integration.DesiredState != 1 {
			return nil, integrationConflict(request.GetIntegrationId(), errors.New("stop belongs to an ended integration intent"))
		}
	}
	unlock := server.controller.lockEnvironment(environment.ID)
	defer unlock()
	pending := func(cause error) error {
		intent, err := server.controller.store.GetManagedContainer(ctx, environment.ID, request.GetContainerId())
		if err != nil || intent.DesiredRunning {
			return cause
		}
		return resourceErrorWithMetadata(codes.Unavailable, "SERVICE_STOP_PENDING", "container", request.GetContainerId(), "stop intent was saved; actual stop is not confirmed: "+cause.Error(), map[string]string{"desired_running": "false", "actual_state": "unknown"})
	}
	guestRequest := &vmmdv1.StopContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId(), TimeoutSeconds: request.TimeoutSeconds}
	if err := server.controller.acceptWorkloadStopLocked(ctx, environment.ID, guestRequest); err != nil {
		return nil, pending(err)
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, pending(err)
	}
	defer connection.Close()
	response, err := server.controller.stopWorkloadContainerLocked(ctx, client, environment.ID, guestRequest)
	if err != nil {
		return nil, pending(err)
	}
	if state := response.GetContainer().GetState(); state != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && state != vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
		return nil, pending(errors.New("Guest has not observed a stopped container"))
	}
	current := containerFromGuest(response.GetContainer())
	if err := server.controller.projectManagedContainerIntent(ctx, environment.ID, current); err != nil {
		return nil, err
	}
	return &hostv1.ContainerResponse{Container: current}, nil
}

func (server *operatorServer) ContinueContainer(ctx context.Context, request *hostv1.OperatorContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.requireUndisposedEnvironment(ctx, request.GetEnvironmentId()); err != nil {
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
	response, err := server.controller.startWorkloadContainer(ctx, client, environment.ID, &vmmdv1.StartContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId()})
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
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
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
	if err := server.controller.deleteWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.DeleteContainerRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID,
		ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId(),
	}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
