//go:build darwin || linux

package host

import (
	"context"
	"errors"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *operatorServer) StopContainer(ctx context.Context, request *hostv1.OperatorServiceStopContainerRequest) (*hostv1.OperatorServiceStopContainerResponse, error) {
	if request.GetRequestId() == "" || request.GetContainerId() == "" {
		return nil, hostv1.InvalidField("request_id/container_id", "request and container IDs are required")
	}
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "agent environment does not exist")
	}
	if err := requireLocalOperatorEnvironment(environment); err != nil {
		return nil, err
	}
	if request.ExpectedIntegrationRevision != nil || request.IntegrationId != nil {
		if request.GetIntegrationId() == "" || request.ExpectedIntegrationRevision == nil || request.GetExpectedIntegrationRevision() == 0 {
			return nil, hostv1.InvalidField("integration_id/expected_integration_revision", "exact integration intent is required")
		}
		unlock, err := server.controller.sessions.LockIntegrationRead(ctx, request.GetIntegrationId())
		if err != nil {
			return nil, err
		}
		defer unlock()
		integration, err := server.controller.store.GetAgentIntegration(ctx, request.GetIntegrationId())
		if err != nil || integration.EnvironmentID != environment.ID || integration.Revision != request.GetExpectedIntegrationRevision() || integration.DesiredState != 1 {
			return nil, integrationConflict(errors.New("stop belongs to an ended integration intent"))
		}
	}
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, environment.ID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	pending := func(cause error) error {
		intent, err := server.controller.store.GetManagedContainer(ctx, environment.ID, request.GetContainerId())
		if err != nil || intent.DesiredRunning {
			return cause
		}
		return hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SERVICE_STOP_PENDING, "stop intent was saved; actual stop is not confirmed: "+cause.Error(), hostv1.WithResource("container", request.GetContainerId()), hostv1.WithMetadata(map[string]string{"desired_running": "false", "actual_state": "unknown"}))
	}
	guestRequest := &vmmdv1.StopContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId(), TimeoutSeconds: request.TimeoutSeconds}
	if err := server.controller.workloads.AcceptWorkloadStopLocked(ctx, environment.ID, guestRequest); err != nil {
		return nil, pending(err)
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, pending(err)
	}

	response, err := server.controller.workloads.StopWorkloadContainerLocked(ctx, client, environment.ID, guestRequest)
	if err != nil {
		return nil, pending(err)
	}
	if state := response.GetContainer().GetState(); state != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && state != vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
		return nil, pending(errors.New("Guest has not observed a stopped container"))
	}
	current := guestconn.Container(response.GetContainer())
	if err := server.controller.workloads.ProjectManagedContainerIntent(ctx, environment.ID, current); err != nil {
		return nil, err
	}
	return &hostv1.OperatorServiceStopContainerResponse{Container: current}, nil
}

func (server *operatorServer) ContinueContainer(ctx context.Context, request *hostv1.ContinueContainerRequest) (*hostv1.ContinueContainerResponse, error) {
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, request.GetEnvironmentId()); err != nil {
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

	response, err := server.controller.workloads.StartWorkloadContainer(ctx, client, environment.ID, &vmmdv1.StartContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	return &hostv1.ContinueContainerResponse{Container: guestconn.Container(response.GetContainer())}, nil
}

func (server *operatorServer) SignalContainer(ctx context.Context, request *hostv1.OperatorServiceSignalContainerRequest) (*hostv1.OperatorServiceSignalContainerResponse, error) {
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

	if _, err := client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID, ContainerId: request.GetContainerId(), Signal: vmmdv1.ProcessSignal(request.GetSignal())}); err != nil {
		return nil, err
	}
	return &hostv1.OperatorServiceSignalContainerResponse{}, nil
}

func (server *operatorServer) DeleteContainer(ctx context.Context, request *hostv1.OperatorServiceDeleteContainerRequest) (*hostv1.OperatorServiceDeleteContainerResponse, error) {
	if request.GetExpectedGenerationId() == "" {
		return nil, hostv1.InvalidField("expected_generation_id", "container generation is required")
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

	if err := server.controller.workloads.DeleteWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.DeleteContainerRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: environment.NamespaceID,
		ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId(),
	}); err != nil {
		return nil, err
	}
	return &hostv1.OperatorServiceDeleteContainerResponse{}, nil
}
