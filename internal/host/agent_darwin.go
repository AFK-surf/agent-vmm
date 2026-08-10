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
	controller *Controller
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
	guest, err := client.PullImage(stream.Context(), &vmmdv1.PullImageRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, Reference: request.GetReference()})
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
		if err := stream.Send(&hostv1.PullImageResponse{Status: response.GetStatus(), Digest: response.Digest, LogicalBytes: response.GetLogicalBytes()}); err != nil {
			return err
		}
	}
}

func (server *agentServer) CreateContainer(ctx context.Context, request *hostv1.CreateContainerRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	guestRequest := &vmmdv1.CreateContainerRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), Image: request.GetImage(),
		Argv: request.GetArgv(), Env: request.GetEnv(), Workdir: request.Workdir, User: request.User, ReadOnlyRoot: request.GetReadOnlyRoot(), LogLimitBytes: request.GetLogLimitBytes(),
	}
	if limits := request.GetLimits(); limits != nil {
		guestRequest.Limits = &vmmdv1.ResourceLimits{CpuMillis: limits.CpuMillis, MemoryBytes: limits.MemoryBytes, Pids: limits.Pids}
	}
	for _, volume := range request.GetVolumes() {
		guestRequest.Volumes = append(guestRequest.Volumes, &vmmdv1.VolumeMount{VolumeId: volume.GetVolumeId(), Destination: volume.GetDestination(), ReadOnly: volume.GetReadOnly()})
	}
	response, err := client.CreateContainer(ctx, guestRequest)
	if err != nil {
		return nil, err
	}
	return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
}

func (server *agentServer) StartContainer(ctx context.Context, request *hostv1.ContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.StartContainer(ctx, &vmmdv1.StartContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId()})
	if err != nil {
		return nil, err
	}
	return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
}

func (server *agentServer) StopContainer(ctx context.Context, request *hostv1.StopContainerRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), TimeoutSeconds: request.TimeoutSeconds})
	if err != nil {
		return nil, err
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
	return &hostv1.WaitContainerResponse{ExitCode: response.GetExitCode()}, nil
}

func (server *agentServer) DeleteContainer(ctx context.Context, request *hostv1.ContainerMutationRequest) (*emptypb.Empty, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId()}); err != nil {
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
	return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
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
		result.Containers = append(result.Containers, containerFromGuest(container))
	}
	return result, nil
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
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if err := server.controller.store.PutVolumeOwner(ctx, request.GetVolumeId(), identity.NamespaceID); err != nil {
		return nil, err
	}
	response, err := client.CreateVolume(ctx, &vmmdv1.CreateVolumeRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, VolumeId: request.GetVolumeId()})
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
