//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type agentServer struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	controller     *Controller
	buildLifetime  context.Context
	openGuestExec  func(context.Context) (vmmdv1.VMMDService_ExecClient, io.Closer, error)
	openGuestBuild func(context.Context) (vmmdv1.VMMDService_BuildImageClient, io.Closer, error)
}

func (server *agentServer) GetEnvironment(ctx context.Context, _ *hostv1.GetEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "agent environment does not exist")
	}
	snapshot := server.controller.services.EnvironmentSnapshot(ctx, environment)
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
		return hostv1.InvalidField("request_id/reference", "request ID and image reference are required")
	}
	deadline, ok := stream.Context().Deadline()
	if !ok || deadline.After(time.Now().Add(10*time.Minute)) {
		deadline = time.Now().Add(10 * time.Minute)
	}
	lease, err := server.controller.egress.Leases.Acquire(deadline)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	defer lease.Release()
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return err
	}

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
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := client.ListImages(ctx, &vmmdv1.ListImagesRequest{ActorNamespaceId: identity.NamespaceID, ControllerCacheScope: scope})
	if err != nil {
		return nil, err
	}
	result := &hostv1.ListImagesResponse{}
	for _, image := range response.GetImages() {
		result.Images = append(result.Images, guestconn.Image(image))
	}
	return result, nil
}

func (server *agentServer) DeleteImage(ctx context.Context, request *hostv1.DeleteImageRequest) (*hostv1.DeleteImageResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetReference() == "" || request.GetExpectedGenerationId() == "" || request.GetExpectedDigest() == "" {
		return nil, hostv1.InvalidField("request_id/reference/expected_generation_id/expected_digest", "image identity and request ID are required")
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := server.validateAgentInventoryTarget(ctx, identity.EnvironmentID, identity.NamespaceID); err != nil {
		return nil, err
	}
	scope, err := server.controllerImageScope(ctx, identity, request.Origin)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	_, err = client.DeleteImage(ctx, &vmmdv1.DeleteImageRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, Reference: request.GetReference(),
		ExpectedGenerationId: request.GetExpectedGenerationId(), ExpectedDigest: request.GetExpectedDigest(), ControllerCacheScope: scope,
	})
	if err != nil {
		return nil, err
	}
	return &hostv1.DeleteImageResponse{}, nil
}

func (server *agentServer) validateAgentInventoryTarget(ctx context.Context, environmentID, namespaceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return status.Error(codes.NotFound, "agent environment does not exist")
	}
	if environment.NamespaceID != namespaceID {
		return status.Error(codes.Aborted, "agent environment namespace changed; reconnect and retry")
	}
	return nil
}

func (server *agentServer) StopContainer(ctx context.Context, request *hostv1.StopContainerRequest) (*hostv1.StopContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := server.controller.workloads.ValidateExplicitStop(identity, request); err != nil {
		return nil, err
	}
	guestRequest := &vmmdv1.StopContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), TimeoutSeconds: request.TimeoutSeconds}
	if err := server.controller.workloads.AcceptWorkloadStopLocked(ctx, identity.EnvironmentID, guestRequest); err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := server.controller.workloads.StopWorkloadContainerLocked(ctx, client, identity.EnvironmentID, guestRequest)
	if err != nil {
		return nil, err
	}
	if response.GetContainer().GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
		if err := server.controller.services.InvalidateStaleServiceExports(ctx, client, identity.EnvironmentID, request.GetContainerId(), ""); err != nil {
			return nil, err
		}
	}
	return &hostv1.StopContainerResponse{Container: guestconn.Container(response.GetContainer())}, nil
}

func (server *agentServer) WaitContainer(ctx context.Context, request *hostv1.WaitContainerRequest) (*hostv1.WaitContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := client.WaitContainer(ctx, &vmmdv1.WaitContainerRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	// Waiting observes the terminal generation.  Revoke any routes that still
	// reference that generation before reporting the exit to the caller.
	if err := server.controller.services.InvalidateStaleServiceExports(ctx, client, identity.EnvironmentID, request.GetContainerId(), ""); err != nil {
		return nil, err
	}
	return &hostv1.WaitContainerResponse{ExitCode: response.GetExitCode()}, nil
}

func (server *agentServer) DeleteContainer(ctx context.Context, request *hostv1.DeleteContainerRequest) (*hostv1.DeleteContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	if request.GetExpectedGenerationId() == "" {
		return nil, hostv1.InvalidField("expected_generation_id", "container generation is required")
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := server.controller.workloads.DeleteWorkloadContainerLocked(ctx, client, identity.EnvironmentID, &vmmdv1.DeleteContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId()}); err != nil {
		return nil, err
	}
	return &hostv1.DeleteContainerResponse{}, nil
}

func (server *agentServer) GetContainer(ctx context.Context, request *hostv1.GetContainerRequest) (*hostv1.GetContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	container := containerConfigurationObservation(response.GetContainer(), request.GetExpectedConfiguration())
	if err := server.controller.workloads.ProjectManagedContainerIntent(ctx, identity.EnvironmentID, container); err != nil {
		return nil, err
	}
	return &hostv1.GetContainerResponse{Container: container}, nil
}

func (server *agentServer) ListContainers(ctx context.Context, _ *hostv1.ListContainersRequest) (*hostv1.ListContainersResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: identity.NamespaceID})
	if err != nil {
		return nil, err
	}
	result := &hostv1.ListContainersResponse{}
	for _, container := range response.GetContainers() {
		observed := guestconn.Container(container)
		if err := server.controller.workloads.ProjectManagedContainerIntent(ctx, identity.EnvironmentID, observed); err != nil {
			return nil, err
		}
		result.Containers = append(result.Containers, observed)
	}
	return result, nil
}

func (server *agentServer) Logs(request *hostv1.LogsRequest, stream hostv1.AgentRuntimeService_LogsServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return err
	}

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
		if err := stream.Send(&hostv1.LogsResponse{Cursor: response.GetCursor(), Timestamp: timestamppb.New(time.Unix(0, response.GetTimestampUnixNano())), Stream: hostv1.LogStream(response.GetStream()), Data: response.GetData(), Truncated: response.GetTruncated(), DroppedRecords: response.GetDroppedRecords()}); err != nil {
			return err
		}
	}
}

func (server *agentServer) SignalContainer(ctx context.Context, request *hostv1.SignalContainerRequest) (*hostv1.SignalContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	_, err = client.SignalContainer(ctx, &vmmdv1.SignalContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), Signal: vmmdv1.ProcessSignal(request.GetSignal())})
	if err != nil {
		return nil, err
	}
	return &hostv1.SignalContainerResponse{}, nil
}

func (server *agentServer) CreateVolume(ctx context.Context, request *hostv1.CreateVolumeRequest) (*hostv1.CreateVolumeResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	if err := server.controller.store.PutVolumeOwner(ctx, request.GetVolumeId(), identity.NamespaceID); err != nil {
		return nil, err
	}
	response, err := client.CreateVolume(ctx, &vmmdv1.CreateVolumeRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId(), OwnerUid: request.OwnerUid, OwnerGid: request.OwnerGid, Mode: request.Mode})
	if err != nil {
		_ = server.controller.store.DeleteVolumePolicy(ctx, request.GetVolumeId(), identity.NamespaceID)
		return nil, err
	}
	return &hostv1.CreateVolumeResponse{Volume: guestconn.Volume(response.GetVolume())}, nil
}

func (server *agentServer) DeleteVolume(ctx context.Context, request *hostv1.DeleteVolumeRequest) (*hostv1.DeleteVolumeResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId()}); err != nil {
		return nil, err
	}
	if err := server.controller.store.DeleteVolumePolicy(ctx, request.GetVolumeId(), identity.NamespaceID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return &hostv1.DeleteVolumeResponse{}, nil
}

func (server *agentServer) GetVolume(ctx context.Context, request *hostv1.GetVolumeRequest) (*hostv1.GetVolumeResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := client.GetVolume(ctx, &vmmdv1.GetVolumeRequest{ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId()})
	if err != nil {
		return nil, err
	}
	return &hostv1.GetVolumeResponse{Volume: guestconn.Volume(response.GetVolume())}, nil
}

func (server *agentServer) ListVolumes(ctx context.Context, _ *hostv1.ListVolumesRequest) (*hostv1.ListVolumesResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return nil, err
	}

	response, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: identity.NamespaceID})
	if err != nil {
		return nil, err
	}
	result := &hostv1.ListVolumesResponse{}
	for _, volume := range response.GetVolumes() {
		result.Volumes = append(result.Volumes, guestconn.Volume(volume))
	}
	return result, nil
}

func (server *agentServer) ReleaseSession(ctx context.Context, request *hostv1.ReleaseSessionRequest) (*hostv1.ReleaseSessionResponse, error) {
	_, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	session.EndAfterResponse(ctx)
	return &hostv1.ReleaseSessionResponse{}, nil
}
