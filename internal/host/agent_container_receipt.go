//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Authorization and the inventory mutation lock precede this acceptance record.
func (server *agentServer) containerMutationReceipt(ctx context.Context, identity auth.Identity, kind, requestID string, request proto.Message, guestRequest proto.Message, containerID string, execute func() (*hostv1.ContainerResponse, error), admission func(store.Operation) (store.Operation, bool, error)) (*hostv1.ContainerResponse, error) {
	if requestID == "" {
		return nil, resourceError(codes.InvalidArgument, "REQUEST_ID_REQUIRED", "container", containerID, "request ID is required")
	}
	key := sha256.Sum256([]byte(kind + "\x00" + requestID))
	payload := sha256.New()
	payload.Write([]byte(identity.EnvironmentID + "\x00" + identity.NamespaceID + "\x00" + identity.RegistrationID + "\x00" + kind + "\x00"))
	payload.Write(runtimeRequestHash(request))
	operation := store.Operation{
		ID: "runtime-" + hex.EncodeToString(key[:]), RequestID: "runtime-" + hex.EncodeToString(key[:]), Kind: kind,
		EnvironmentID: identity.EnvironmentID, RequestSHA256: payload.Sum(nil), State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "accepted",
	}
	original, err := server.controller.store.OperationByRequestID(ctx, operation.RequestID)
	inserted := false
	if err == nil {
		operation = original
	} else if errors.Is(err, sql.ErrNoRows) {
		operation, inserted, err = admission(operation)
	}
	if err != nil {
		return nil, err
	}
	if operation.EnvironmentID != identity.EnvironmentID || !bytes.Equal(operation.RequestSHA256, payload.Sum(nil)) {
		return nil, resourceError(codes.AlreadyExists, "REQUEST_ID_CONFLICT", "container", containerID, "request ID was accepted for a different owner or payload")
	}
	if !inserted {
		if operation.State == int32(hostv1.OperationState_OPERATION_STATE_FAILED) {
			failure := new(statuspb.Status)
			if err := proto.Unmarshal(operation.Receipt, failure); err != nil {
				return nil, err
			}
			return nil, status.FromProto(failure).Err()
		}
		if operation.State == int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
			response := &hostv1.ContainerResponse{}
			if err := proto.Unmarshal(operation.Receipt, response); err != nil {
				return nil, err
			}
			return response, nil
		}
		client, connection, err := server.controller.guestClient(ctx)
		if err != nil {
			return nil, err
		}
		defer connection.Close()
		return server.resolveContainerMutation(ctx, client, identity, kind, guestRequest, containerID, operation)
	}
	response, err := execute()
	if err != nil {
		if containerMutationAdmissionRejected(err) {
			failure, detailErr := status.Convert(err).WithDetails(&errdetails.ErrorInfo{Reason: "CONTAINER_MUTATION_REJECTED", Domain: "agentvmm.afk.surf", Metadata: map[string]string{"operation_id": operation.ID}})
			if detailErr != nil {
				return nil, detailErr
			}
			receipt, marshalErr := proto.Marshal(failure.Proto())
			if marshalErr != nil {
				return nil, marshalErr
			}
			operation.Receipt = receipt
			operation.State = int32(hostv1.OperationState_OPERATION_STATE_FAILED)
			operation.Stage = "rejected"
			operation.ErrorReason = "CONTAINER_MUTATION_REJECTED"
			operation.ErrorMessage = failure.Message()
			completion, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if updateErr := server.controller.store.UpdateOperation(completion, operation); updateErr != nil {
				return nil, updateErr
			}
			return nil, failure.Err()
		}
		return nil, err
	}
	return server.completeContainerMutation(operation, response)
}

// A restart or lost response never authorizes a second mutation.
func (server *agentServer) resolveContainerMutation(ctx context.Context, client vmmdv1.VMMDServiceClient, identity auth.Identity, kind string, guestRequest proto.Message, containerID string, operation store.Operation) (*hostv1.ContainerResponse, error) {
	observed, inspectErr := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: containerID})
	if inspectErr != nil {
		return nil, resourceError(codes.FailedPrecondition, "CONTAINER_MUTATION_UNKNOWN", "container", containerID, "accepted mutation is unresolved; inspect its exact container or use a new intent")
	}
	current := observed.GetContainer()
	expected := hex.EncodeToString(runtimeRequestHash(guestRequest))
	confirmed := kind == "runtime-create-container" && current.GetCreateRequestSha256() == expected
	if kind == "runtime-start-container" {
		confirmed = current.GetStartRequestSha256() == expected && current.GetStartRequestInstanceId() != "" && current.GetStartedAtUnixNano() != 0
	}
	if !confirmed {
		return nil, resourceError(codes.FailedPrecondition, "CONTAINER_MUTATION_UNKNOWN", "container", containerID, "accepted mutation has no matching Guest receipt; it will not be repeated")
	}
	response := &hostv1.ContainerResponse{Container: containerFromGuest(current)}
	if kind == "runtime-start-container" {
		response.Container.InstanceId = current.GetStartRequestInstanceId()
	}
	return server.completeContainerMutation(operation, response)
}

func containerMutationAdmissionRejected(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "agent-vmm" && info.Metadata["admission"] == "not_accepted" {
			return true
		}
	}
	return false
}

func (server *agentServer) completeContainerMutation(operation store.Operation, response *hostv1.ContainerResponse) (*hostv1.ContainerResponse, error) {
	current := response.GetContainer()
	response = &hostv1.ContainerResponse{Container: &hostv1.Container{
		Id: current.GetId(), Image: current.GetImage(), State: current.GetState(), ExitCode: current.ExitCode,
		InstanceId: current.GetInstanceId(), GenerationId: current.GetGenerationId(), CreatedAt: current.CreatedAt, StartedAt: current.StartedAt, FinishedAt: current.FinishedAt,
	}}
	receipt, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		return nil, err
	}
	operation.Receipt = receipt
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	operation.Stage = "complete"
	completion, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.controller.store.UpdateOperation(completion, operation); err != nil {
		return nil, err
	}
	return response, nil
}

func (server *agentServer) CreateContainer(ctx context.Context, request *hostv1.CreateContainerRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	var client vmmdv1.VMMDServiceClient
	var connection io.Closer
	defer func() {
		if connection != nil {
			connection.Close()
		}
	}()
	guestRequest := &vmmdv1.CreateContainerRequest{
		RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), Image: request.GetImage(),
		Env: request.GetEnv(), Workdir: request.Workdir, User: request.User, ReadOnlyRoot: request.GetReadOnlyRoot(), LogLimitBytes: request.GetLogLimitBytes(),
		ConfigurationFingerprint: optional(containerConfigurationFingerprint(request)),
	}
	if request.GetEntrypointOverride() != nil {
		guestRequest.EntrypointOverride = &vmmdv1.StringList{Values: append([]string(nil), request.GetEntrypointOverride().GetValues()...)}
	}
	if request.GetCommandOverride() != nil {
		guestRequest.CommandOverride = &vmmdv1.StringList{Values: append([]string(nil), request.GetCommandOverride().GetValues()...)}
	}
	if limits := request.GetLimits(); limits != nil {
		guestRequest.Limits = &vmmdv1.ResourceLimits{CpuMillis: limits.CpuMillis, MemoryBytes: limits.MemoryBytes, Pids: limits.Pids}
	}
	for _, volume := range request.GetVolumes() {
		mount := &vmmdv1.VolumeMount{Destination: volume.GetDestination(), ReadOnly: volume.GetReadOnly()}
		switch source := volume.Source.(type) {
		case *hostv1.VolumeMount_OwnedVolume:
			mount.Source = &vmmdv1.VolumeMount_OwnedVolume{OwnedVolume: source.OwnedVolume}
		case *hostv1.VolumeMount_HostShare:
			mount.Source = &vmmdv1.VolumeMount_HostShare{HostShare: source.HostShare}
		default:
			return nil, invalidField("volumes.source", "owned volume or host share is required")
		}
		guestRequest.Volumes = append(guestRequest.Volumes, mount)
	}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	return server.containerMutationReceipt(ctx, identity, "runtime-create-container", request.GetRequestId(), request, guestRequest, request.GetContainerId(), func() (*hostv1.ContainerResponse, error) {
		response, err := client.CreateContainer(ctx, guestRequest)
		if err != nil {
			return nil, err
		}
		return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
	}, func(operation store.Operation) (store.Operation, bool, error) {
		if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
			return store.Operation{}, false, err
		}
		client, connection, err = server.controller.guestClient(ctx)
		if err != nil {
			return store.Operation{}, false, err
		}
		if err := server.controller.authorizeHostShareMounts(ctx, identity.EnvironmentID, guestRequest.GetVolumes()); err != nil {
			return store.Operation{}, false, err
		}
		return server.controller.store.PutOperation(ctx, operation)
	})
}

func (server *agentServer) StartContainer(ctx context.Context, request *hostv1.ContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetExpectedGenerationId() == "" {
		return nil, invalidField("expected_generation_id", "exact container generation is required")
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	var client vmmdv1.VMMDServiceClient
	var connection io.Closer
	defer func() {
		if connection != nil {
			connection.Close()
		}
	}()
	guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId()}
	unlock := server.controller.lockEnvironment(identity.EnvironmentID)
	defer unlock()
	ctx, cancel := context.WithTimeout(ctx, workloadStartBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var run *workloadRun
	var observed *vmmdv1.Container
	return server.containerMutationReceipt(ctx, identity, "runtime-start-container", request.GetRequestId(), request, guestRequest, request.GetContainerId(), func() (*hostv1.ContainerResponse, error) {
		response, err := server.controller.sendWorkloadStartLocked(ctx, client, identity.EnvironmentID, guestRequest, run, observed, deadline)
		if err != nil {
			return nil, err
		}
		instanceID := response.GetContainer().GetInstanceId()
		if response.GetContainer().GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			instanceID = ""
		}
		if err := server.controller.invalidateStaleServiceExports(ctx, client, identity.EnvironmentID, request.GetContainerId(), instanceID); err != nil {
			return nil, err
		}
		return &hostv1.ContainerResponse{Container: containerFromGuest(response.GetContainer())}, nil
	}, func(operation store.Operation) (store.Operation, bool, error) {
		if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
			return store.Operation{}, false, err
		}
		client, connection, err = server.controller.guestClient(ctx)
		if err != nil {
			return store.Operation{}, false, err
		}
		run, observed, err = server.controller.prepareWorkloadStartLocked(ctx, client, identity.EnvironmentID, guestRequest, identity.RegistrationID)
		if err != nil {
			return store.Operation{}, false, err
		}
		stored, inserted, err := server.controller.store.PutWorkloadStartOperation(ctx, operation, request.GetContainerId(), observed.GetGenerationId())
		if err != nil || !inserted {
			server.controller.releaseUnsentWorkloadStart(run)
		}
		return stored, inserted, err
	})
}
