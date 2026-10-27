//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Authorization and the inventory mutation lock precede this acceptance record.
func (server *agentServer) containerMutationReceipt(ctx context.Context, identity auth.Identity, kind, requestID string, request proto.Message, guestRequest proto.Message, containerID string, execute func() (*hostv1.CreateContainerResponse, error), admission func(store.Operation) (store.Operation, bool, error)) (*hostv1.CreateContainerResponse, error) {
	if requestID == "" {
		return nil, status.Error(codes.InvalidArgument, "request ID is required")
	}
	key := sha256.Sum256([]byte(kind + "\x00" + identity.EnvironmentID + "\x00" + identity.NamespaceID + "\x00" + requestID))
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
	if !bytes.Equal(operation.RequestSHA256, payload.Sum(nil)) {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID was accepted for a different owner or payload", hostv1.WithResource("container", containerID))
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
			response := &hostv1.CreateContainerResponse{}
			if err := proto.Unmarshal(operation.Receipt, response); err != nil {
				return nil, err
			}
			return response, nil
		}
		client, err := server.controller.guestConnection.Client()
		if err != nil {
			return nil, err
		}
		return server.resolveContainerMutation(ctx, client, identity, kind, guestRequest, containerID, operation)
	}
	response, err := execute()
	if err != nil {
		if guestconn.MutationAdmissionRejected(err) {
			original := status.Convert(err)
			failure := status.Convert(hostv1.NewError(original.Code(), hostv1.ErrorReason_ERROR_REASON_CONTAINER_MUTATION_REJECTED, original.Message(), hostv1.WithMetadata(map[string]string{"operation_id": operation.ID})))
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
func (server *agentServer) resolveContainerMutation(ctx context.Context, client vmmdv1.VMMDServiceClient, identity auth.Identity, kind string, guestRequest proto.Message, containerID string, operation store.Operation) (*hostv1.CreateContainerResponse, error) {
	observed, inspectErr := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: identity.NamespaceID, ContainerId: containerID})
	if inspectErr != nil {
		return nil, status.Error(codes.FailedPrecondition, "accepted mutation is unresolved; inspect its exact container or use a new intent")
	}
	current := observed.GetContainer()
	expected := hex.EncodeToString(runtimeRequestHash(guestRequest))
	confirmed := kind == "runtime-create-container" && current.GetCreateRequestSha256() == expected
	if kind == "runtime-start-container" {
		confirmed = current.GetStartRequestSha256() == expected && current.GetStartRequestInstanceId() != "" && current.GetStartedAtUnixNano() != 0
	}
	if !confirmed {
		return nil, status.Error(codes.FailedPrecondition, "accepted mutation has no matching Guest receipt; it will not be repeated")
	}
	response := &hostv1.CreateContainerResponse{Container: guestconn.Container(current)}
	if kind == "runtime-start-container" {
		response.Container.InstanceId = current.GetStartRequestInstanceId()
	}
	return server.completeContainerMutation(operation, response)
}

func (server *agentServer) completeContainerMutation(operation store.Operation, response *hostv1.CreateContainerResponse) (*hostv1.CreateContainerResponse, error) {
	current := response.GetContainer()
	response = &hostv1.CreateContainerResponse{Container: &hostv1.Container{
		Id: current.GetId(), Image: current.GetImage(), State: current.GetState(), ExitCode: current.ExitCode,
		InstanceId: current.GetInstanceId(), GenerationId: current.GetGenerationId(), CreatedAt: current.CreatedAt, StartedAt: current.StartedAt, FinishedAt: current.FinishedAt,
		StartRequestInstanceId: current.GetStartRequestInstanceId(),
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

func (server *agentServer) CreateContainer(ctx context.Context, request *hostv1.CreateContainerRequest) (*hostv1.CreateContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	var client vmmdv1.VMMDServiceClient
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
		guestRequest.Limits = &vmmdv1.ResourceLimits{Pids: limits.Pids}
	}
	for _, volume := range request.GetVolumes() {
		mount := &vmmdv1.VolumeMount{Destination: volume.GetDestination(), ReadOnly: volume.GetReadOnly()}
		switch source := volume.Source.(type) {
		case *hostv1.VolumeMount_OwnedVolume:
			mount.Source = &vmmdv1.VolumeMount_OwnedVolume{OwnedVolume: source.OwnedVolume}
		case *hostv1.VolumeMount_HostShare:
			mount.Source = &vmmdv1.VolumeMount_HostShare{HostShare: source.HostShare}
		default:
			return nil, hostv1.InvalidField("volumes.source", "owned volume or host share is required")
		}
		guestRequest.Volumes = append(guestRequest.Volumes, mount)
	}
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	return server.containerMutationReceipt(ctx, identity, "runtime-create-container", request.GetRequestId(), request, guestRequest, request.GetContainerId(), func() (*hostv1.CreateContainerResponse, error) {
		response, err := client.CreateContainer(ctx, guestRequest)
		if err != nil {
			return nil, err
		}
		return &hostv1.CreateContainerResponse{Container: guestconn.Container(response.GetContainer())}, nil
	}, func(operation store.Operation) (store.Operation, bool, error) {
		if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
			return store.Operation{}, false, err
		}
		client, err = server.controller.guestConnection.Client()
		if err != nil {
			return store.Operation{}, false, err
		}
		if err := server.controller.hostShares.AuthorizeHostShareMounts(ctx, identity.EnvironmentID, guestRequest.GetVolumes()); err != nil {
			return store.Operation{}, false, err
		}
		return server.controller.store.PutOperation(ctx, operation)
	})
}

func (server *agentServer) StartContainer(ctx context.Context, request *hostv1.StartContainerRequest) (*hostv1.StartContainerResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if request.GetExpectedGenerationId() == "" {
		return nil, hostv1.InvalidField("expected_generation_id", "exact container generation is required")
	}
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	var client vmmdv1.VMMDServiceClient
	guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.NamespaceID, ContainerId: request.GetContainerId(), ExpectedGenerationId: request.GetExpectedGenerationId()}
	unlock, lockErr := server.controller.workloads.LockEnvironment(ctx, identity.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	ctx, cancel := context.WithTimeout(ctx, workload.StartBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var run *workload.Run
	var observed *vmmdv1.Container
	response, err := server.containerMutationReceipt(ctx, identity, "runtime-start-container", request.GetRequestId(), request, guestRequest, request.GetContainerId(), func() (*hostv1.CreateContainerResponse, error) {
		response, err := server.controller.workloads.SendWorkloadStartLocked(ctx, client, identity.EnvironmentID, guestRequest, run, observed, deadline)
		if err != nil {
			return nil, err
		}
		instanceID := response.GetContainer().GetInstanceId()
		if response.GetContainer().GetState() == vmmdv1.ContainerState_CONTAINER_STATE_STOPPED {
			instanceID = ""
		}
		if err := server.controller.services.InvalidateStaleServiceExports(ctx, client, identity.EnvironmentID, request.GetContainerId(), instanceID); err != nil {
			return nil, err
		}
		return &hostv1.CreateContainerResponse{Container: guestconn.Container(response.GetContainer())}, nil
	}, func(operation store.Operation) (store.Operation, bool, error) {
		if err := server.controller.cleanup.RequireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
			return store.Operation{}, false, err
		}
		client, err = server.controller.guestConnection.Client()
		if err != nil {
			return store.Operation{}, false, err
		}
		run, observed, err = server.controller.workloads.PrepareWorkloadStartLocked(ctx, client, identity.EnvironmentID, guestRequest, identity.RegistrationID)
		if err != nil {
			return store.Operation{}, false, err
		}
		stored, inserted, err := server.controller.store.PutWorkloadStartOperation(ctx, operation, request.GetContainerId(), observed.GetGenerationId())
		if err != nil || !inserted {
			server.controller.workloads.ReleaseUnsentWorkloadStart(run)
		}
		return stored, inserted, err
	})
	return &hostv1.StartContainerResponse{Container: response.GetContainer()}, err
}
