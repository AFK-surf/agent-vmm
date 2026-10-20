//go:build darwin

package host

import (
	"context"
	"io"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

func (server *agentServer) requireWorkspaceVolume(ctx context.Context, identity auth.Identity, volumeID string) error {
	if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return err
	}
	if volumeID == "" {
		return nil
	}
	owned, err := server.controller.store.HasVolumePolicy(ctx, volumeID, identity.NamespaceID)
	if err != nil {
		return err
	}
	if !owned {
		return status.Error(codes.PermissionDenied, "workspace volume is not owned by this namespace")
	}
	return nil
}

func (server *agentServer) StatWorkspace(ctx context.Context, request *hostv1.WorkspacePathRequest) (*hostv1.WorkspaceEntry, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	entry, err := client.StatWorkspace(ctx, &vmmdv1.WorkspacePathRequest{ActorNamespaceId: identity.NamespaceID, Path: request.GetPath()})
	if err != nil {
		return nil, err
	}
	return workspaceEntryFromGuest(entry), nil
}

func (server *agentServer) ListWorkspace(ctx context.Context, request *hostv1.ListWorkspaceRequest) (*hostv1.ListWorkspaceResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if err := server.requireWorkspaceVolume(ctx, identity, request.GetVolumeId()); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	guest, err := client.ListWorkspace(ctx, &vmmdv1.ListWorkspaceRequest{ActorNamespaceId: identity.NamespaceID, Path: request.GetPath(), Limit: request.GetLimit(), VolumeId: request.GetVolumeId()})
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListWorkspaceResponse{}
	for _, entry := range guest.GetEntries() {
		response.Entries = append(response.Entries, workspaceEntryFromGuest(entry))
	}
	return response, nil
}

func (server *agentServer) ReadWorkspace(request *hostv1.ReadWorkspaceRequest, stream hostv1.AgentRuntimeService_ReadWorkspaceServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	if err := server.requireWorkspaceVolume(stream.Context(), identity, request.GetVolumeId()); err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return workspaceGuestReadError(err, "start")
	}
	defer connection.Close()
	guest, err := client.ReadWorkspace(stream.Context(), &vmmdv1.ReadWorkspaceRequest{ActorNamespaceId: identity.NamespaceID,
		Path: request.GetPath(), Offset: request.GetOffset(), MaxBytes: request.GetMaxBytes(), VolumeId: request.GetVolumeId()})
	if err != nil {
		return workspaceGuestReadError(err, "start")
	}
	phase := "start"
	for {
		message, err := guest.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return workspaceGuestReadError(err, phase)
		}
		if err := stream.Send(&hostv1.ReadWorkspaceResponse{Data: message.GetData(), Offset: message.GetOffset(),
			Sha256: message.GetSha256(), Final: message.GetFinal()}); err != nil {
			return err
		}
		phase = "receive"
	}
}

func (server *agentServer) WriteWorkspace(stream hostv1.AgentRuntimeService_WriteWorkspaceServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.WriteWorkspace(stream.Context())
	if err != nil {
		return err
	}
	for {
		message, recvErr := stream.Recv()
		if recvErr == io.EOF {
			response, err := guest.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(&hostv1.WriteWorkspaceResponse{Entry: workspaceEntryFromGuest(response.GetEntry()), Sha256: response.GetSha256()})
		}
		if recvErr != nil {
			return recvErr
		}
		if err := guest.Send(writeWorkspaceToGuest(message, identity.NamespaceID)); err != nil {
			return err
		}
	}
}

func (server *agentServer) ArchiveWorkspace(request *hostv1.ArchiveWorkspaceRequest, stream hostv1.AgentRuntimeService_ArchiveWorkspaceServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.ArchiveWorkspace(stream.Context(), &vmmdv1.ArchiveWorkspaceRequest{ActorNamespaceId: identity.NamespaceID, Path: request.GetPath()})
	if err != nil {
		return err
	}
	for {
		message, err := guest.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&hostv1.ArchiveWorkspaceResponse{Data: message.GetData(), Sha256: message.GetSha256(), Final: message.GetFinal()}); err != nil {
			return err
		}
	}
}

func (server *agentServer) ExtractWorkspace(stream hostv1.AgentRuntimeService_ExtractWorkspaceServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.ExtractWorkspace(stream.Context())
	if err != nil {
		return err
	}
	for {
		message, recvErr := stream.Recv()
		if recvErr == io.EOF {
			response, err := guest.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(&hostv1.WriteWorkspaceResponse{Entry: workspaceEntryFromGuest(response.GetEntry()), Sha256: response.GetSha256()})
		}
		if recvErr != nil {
			return recvErr
		}
		if err := guest.Send(extractWorkspaceToGuest(message, identity.NamespaceID)); err != nil {
			return err
		}
	}
}

func workspaceEntryFromGuest(entry *vmmdv1.WorkspaceEntry) *hostv1.WorkspaceEntry {
	if entry == nil {
		return nil
	}
	return &hostv1.WorkspaceEntry{Path: entry.GetPath(), Kind: hostv1.WorkspaceEntryKind(entry.GetKind()), Size: entry.GetSize(),
		Mode: entry.GetMode(), ModifiedUnixNanos: entry.GetModifiedUnixNanos(), Sha256: entry.GetSha256()}
}

func writeWorkspaceToGuest(message *hostv1.WriteWorkspaceRequest, namespaceID string) *vmmdv1.WriteWorkspaceRequest {
	result := &vmmdv1.WriteWorkspaceRequest{}
	switch payload := message.GetPayload().(type) {
	case *hostv1.WriteWorkspaceRequest_Header:
		header := payload.Header
		result.Payload = &vmmdv1.WriteWorkspaceRequest_Header{Header: &vmmdv1.WriteWorkspaceHeader{RequestId: header.GetRequestId(),
			ActorNamespaceId: namespaceID, Path: header.GetPath(), Offset: header.GetOffset(),
			ExpectedPrefixSha256: header.GetExpectedPrefixSha256(), Mode: header.GetMode(), VolumeId: header.GetVolumeId()}}
	case *hostv1.WriteWorkspaceRequest_Data:
		result.Payload = &vmmdv1.WriteWorkspaceRequest_Data{Data: payload.Data}
	case *hostv1.WriteWorkspaceRequest_Finish:
		result.Payload = &vmmdv1.WriteWorkspaceRequest_Finish{Finish: payload.Finish}
	}
	return result
}

func extractWorkspaceToGuest(message *hostv1.ExtractWorkspaceRequest, namespaceID string) *vmmdv1.ExtractWorkspaceRequest {
	result := &vmmdv1.ExtractWorkspaceRequest{}
	switch payload := message.GetPayload().(type) {
	case *hostv1.ExtractWorkspaceRequest_Header:
		header := payload.Header
		result.Payload = &vmmdv1.ExtractWorkspaceRequest_Header{Header: &vmmdv1.ExtractWorkspaceHeader{
			RequestId: header.GetRequestId(), ActorNamespaceId: namespaceID, Path: header.GetPath()}}
	case *hostv1.ExtractWorkspaceRequest_Data:
		result.Payload = &vmmdv1.ExtractWorkspaceRequest_Data{Data: payload.Data}
	case *hostv1.ExtractWorkspaceRequest_Finish:
		result.Payload = &vmmdv1.ExtractWorkspaceRequest_Finish{Finish: payload.Finish}
	}
	return result
}

func workspaceGuestReadError(err error, phase string) error {
	original := status.Convert(err)
	if len(original.Proto().GetDetails()) != 0 {
		return err
	}
	annotated, detailErr := status.New(original.Code(), "workspace guest relay failed").WithDetails(&errdetails.ErrorInfo{
		Reason: "WORKSPACE_GUEST_RELAY_FAILURE", Domain: "agentvmm.afk.surf",
		Metadata: map[string]string{"hop": "guest", "phase": phase},
	})
	if detailErr != nil {
		return err
	}
	return annotated.Err()
}
