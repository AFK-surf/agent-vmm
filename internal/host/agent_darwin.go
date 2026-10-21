//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type agentServer struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	controller     *Controller
	buildLifetime  context.Context
	openGuestExec  func(context.Context) (vmmdv1.VMMDService_ExecClient, io.Closer, error)
	openGuestBuild func(context.Context) (vmmdv1.VMMDService_BuildImageClient, io.Closer, error)
}

func (server *agentServer) GetEnvironment(ctx context.Context, _ *hostv1.GetOwnEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", identity.EnvironmentID, "agent environment does not exist")
	}
	snapshot := server.controller.environmentSnapshot(ctx, environment)
	snapshot.Id = ""
	snapshot.NamespaceId = ""
	snapshot.Owner = nil
	return &hostv1.GetEnvironmentResponse{Environment: snapshot}, nil
}

func (server *agentServer) PullImage(request *hostv1.PullImageRequest, stream hostv1.AgentRuntimeService_PullImageServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	if request.GetRequestId() == "" || request.GetReference() == "" {
		return invalidField("request_id/reference", "request ID and image reference are required")
	}
	deadline, ok := stream.Context().Deadline()
	if !ok || deadline.After(time.Now().Add(10*time.Minute)) {
		deadline = time.Now().Add(10 * time.Minute)
	}
	lease, err := server.controller.egress.Leases.Acquire(deadline)
	if err != nil {
		return retryable("INFRASTRUCTURE_EGRESS_LIMIT", err.Error(), time.Second)
	}
	defer lease.Release()
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	pull := func() (vmmdv1.VMMDService_PullImageClient, error) {
		return client.PullImage(stream.Context(), &vmmdv1.PullImageRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, Reference: request.GetReference()})
	}
	guest, err := pull()
	if err != nil {
		return err
	}
	imported := false
	for {
		response, err := guest.Recv()
		if err == io.EOF {
			return nil
		}
		// A cold image is fetched and imported once, then resolved again.
		if !imported && importRequired(err) {
			imported = true
			if err := stream.Send(&hostv1.PullImageResponse{Status: "downloading from registry"}); err != nil {
				return err
			}
			if err := server.importRegistryImage(stream.Context(), identity, request.GetRequestId(), request.GetReference()); err != nil {
				return err
			}
			if guest, err = pull(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&hostv1.PullImageResponse{Status: response.GetStatus(), CanonicalReference: response.GetCanonicalReference(), Digest: response.Digest, LogicalBytes: response.GetLogicalBytes()}); err != nil {
			return err
		}
	}
}

func (server *agentServer) ListImages(ctx context.Context, request *hostv1.ListImagesRequest) (*hostv1.ListImagesResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	scope, err := server.controllerImageScope(ctx, identity, request.Origin)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.ListImages(ctx, &vmmdv1.ListImagesRequest{ActorNamespaceId: identity.NamespaceID, ControllerCacheScope: scope})
	if err != nil {
		return nil, err
	}
	result := &hostv1.ListImagesResponse{}
	for _, image := range response.GetImages() {
		result.Images = append(result.Images, imageFromGuest(image))
	}
	return result, nil
}

func (server *agentServer) DeleteImage(ctx context.Context, request *hostv1.DeleteImageRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetReference() == "" || request.GetExpectedGenerationId() == "" || request.GetExpectedDigest() == "" {
		return nil, invalidField("request_id/reference/expected_generation_id/expected_digest", "image identity and request ID are required")
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	if err := server.validateAgentInventoryTarget(ctx, identity.EnvironmentID, identity.NamespaceID); err != nil {
		return nil, err
	}
	scope, err := server.controllerImageScope(ctx, identity, request.Origin)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	_, err = client.DeleteImage(ctx, &vmmdv1.DeleteImageRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, Reference: request.GetReference(),
		ExpectedGenerationId: request.GetExpectedGenerationId(), ExpectedDigest: request.GetExpectedDigest(), ControllerCacheScope: scope,
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *agentServer) validateAgentInventoryTarget(ctx context.Context, environmentID, namespaceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", environmentID, "agent environment does not exist")
	}
	if environment.NamespaceID != namespaceID {
		return resourceError(codes.Aborted, "ENVIRONMENT_TARGET_CHANGED", "environment", environmentID, "agent environment namespace changed; reconnect and retry")
	}
	return nil
}

func (server *agentServer) StopContainer(ctx context.Context, request *hostv1.StopContainerRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	if err := server.controller.validateExplicitStop(identity, request); err != nil {
		return nil, err
	}
	guestRequest := &vmmdv1.StopContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), TimeoutSeconds: request.TimeoutSeconds}
	if err := server.controller.acceptWorkloadStopLocked(ctx, identity.EnvironmentID, guestRequest); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := server.controller.stopWorkloadContainerLocked(ctx, client, identity.EnvironmentID, guestRequest)
	if err != nil {
		return nil, err
	}
	if response.GetContainer().GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
		if err := server.controller.invalidateStaleServiceExports(ctx, client, identity.EnvironmentID, request.GetContainerId(), ""); err != nil {
			return nil, err
		}
	}
	return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
}

func (server *agentServer) WaitContainer(ctx context.Context, request *hostv1.ContainerRequest) (*hostv1.WaitContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.WaitContainer(ctx, &vmmdv1.WaitContainerRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	// Waiting observes the terminal generation.  Revoke any routes that still
	// reference that generation before reporting the exit to the caller.
	if err := server.controller.invalidateStaleServiceExports(ctx, client, identity.EnvironmentID, request.GetContainerId(), ""); err != nil {
		return nil, err
	}
	return &hostv1.WaitContainerResponse{ExitCode: response.GetExitCode()}, nil
}

func (server *agentServer) DeleteContainer(ctx context.Context, request *hostv1.ContainerMutationRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	if request.GetExpectedGenerationId() == "" {
		return nil, invalidField("expected_generation_id", "container generation is required")
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	if err := server.controller.deleteWorkloadContainerLocked(ctx, client, identity.EnvironmentID, &vmmdv1.DeleteContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId()}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *agentServer) GetContainer(ctx context.Context, request *hostv1.ContainerRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	container := containerConfigurationObservation(response.GetContainer(), request.GetExpectedConfiguration())
	if err := server.controller.projectManagedContainerIntent(ctx, identity.EnvironmentID, container); err != nil {
		return nil, err
	}
	return &hostv1.ContainerResponse{Container: container}, nil
}

func (server *agentServer) ListContainers(ctx context.Context, _ *hostv1.ListContainersRequest) (*hostv1.ListContainersResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: identity.NamespaceID})
	if err != nil {
		return nil, err
	}
	result := &hostv1.ListContainersResponse{}
	for _, container := range response.GetContainers() {
		observed := containerFromGuest(container)
		if err := server.controller.projectManagedContainerIntent(ctx, identity.EnvironmentID, observed); err != nil {
			return nil, err
		}
		result.Containers = append(result.Containers, observed)
	}
	return result, nil
}

func (controller *Controller) projectManagedContainerIntent(ctx context.Context, environment string, container *hostv1.Container) error {
	intent, err := controller.store.GetManagedContainer(ctx, environment, container.GetId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	container.DesiredRunning = &intent.DesiredRunning
	return nil
}

func (server *agentServer) Logs(request *hostv1.LogsRequest, stream hostv1.AgentRuntimeService_LogsServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guestRequest := &vmmdv1.LogsRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), TailRecords: request.GetTailRecords(), AfterCursor: request.GetAfterCursor(), Follow: request.GetFollow()}
	if request.GetSince() != nil {
		guestRequest.SinceUnixNano = request.GetSince().AsTime().UnixNano()
	}
	guest, err := client.Logs(stream.Context(), guestRequest)
	if err != nil {
		return err
	}
	for {
		response, err := guest.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&hostv1.LogRecord{Cursor: response.GetCursor(), Timestamp: timestamppb.New(time.Unix(0, response.GetTimestampUnixNano())), Stream: hostv1.LogStream(response.GetStream()), Data: response.GetData(), Truncated: response.GetTruncated(), DroppedRecords: response.GetDroppedRecords()}); err != nil {
			return err
		}
	}
}

func (server *agentServer) SignalContainer(ctx context.Context, request *hostv1.SignalContainerRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	_, err = client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), Signal: vmmdv1.ProcessSignal(request.GetSignal())})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *agentServer) CreateVolume(ctx context.Context, request *hostv1.CreateVolumeRequest) (*hostv1.VolumeResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if err := server.controller.store.PutVolumeOwner(ctx, request.GetVolumeId(), identity.NamespaceID); err != nil {
		return nil, err
	}
	response, err := client.CreateVolume(ctx, &vmmdv1.CreateVolumeRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId(), OwnerUid: request.OwnerUid, OwnerGid: request.OwnerGid, Mode: request.Mode})
	if err != nil {
		_ = server.controller.store.DeleteVolumePolicy(ctx, request.GetVolumeId(), identity.NamespaceID)
		return nil, err
	}
	return &hostv1.VolumeResponse{Volume: volumeFromGuest(response.GetVolume())}, nil
}

func (server *agentServer) DeleteVolume(ctx context.Context, request *hostv1.DeleteVolumeRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId()}); err != nil {
		return nil, err
	}
	if err := server.controller.store.DeleteVolumePolicy(ctx, request.GetVolumeId(), identity.NamespaceID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *agentServer) GetVolume(ctx context.Context, request *hostv1.VolumeRequest) (*hostv1.VolumeResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.GetVolume(ctx, &vmmdv1.GetVolumeRequest{ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId()})
	if err != nil {
		return nil, err
	}
	return &hostv1.VolumeResponse{Volume: volumeFromGuest(response.GetVolume())}, nil
}

func (server *agentServer) ListVolumes(ctx context.Context, _ *hostv1.ListVolumesRequest) (*hostv1.ListVolumesResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: identity.NamespaceID})
	if err != nil {
		return nil, err
	}
	result := &hostv1.ListVolumesResponse{}
	for _, volume := range response.GetVolumes() {
		result.Volumes = append(result.Volumes, volumeFromGuest(volume))
	}
	return result, nil
}

func (server *agentServer) ReleaseSession(ctx context.Context, request *hostv1.ReleaseSessionRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	go server.controller.revokeSessionAfterResponse(identity.SessionID, false)
	return &emptypb.Empty{}, nil
}
