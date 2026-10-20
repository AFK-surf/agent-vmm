//go:build darwin

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func hostShareFromStore(value store.HostShare) *hostv1.HostShare {
	return &hostv1.HostShare{Id: value.ID, EnvironmentId: value.EnvironmentID, SourcePath: value.Source.Path, ReadOnly: value.ReadOnly, Revision: value.Revision, State: value.State, UnavailableReason: value.UnavailableReason}
}

func (server *operatorServer) ListHostShares(ctx context.Context, request *hostv1.ListHostSharesRequest) (*hostv1.ListHostSharesResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	values, err := server.controller.store.ListHostShares(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	response := new(hostv1.ListHostSharesResponse)
	for _, value := range values {
		response.Shares = append(response.Shares, hostShareFromStore(value))
	}
	return response, nil
}

func (server *operatorServer) CreateHostShare(ctx context.Context, request *hostv1.CreateHostShareRequest) (*hostv1.HostShareResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetEnvironmentId() == "" {
		return nil, invalidField("environment_id", "service environment is required")
	}
	return server.controller.mutateHostShare(ctx, "host-share-create", request.GetRequestId(), request, store.HostShare{ID: stableID("share", request.GetRequestId()), EnvironmentID: request.GetEnvironmentId(), Source: store.HostShareSource{Path: request.GetSourcePath()}, ReadOnly: request.GetReadOnly(), Revision: 1}, 0)
}

func (server *operatorServer) RebindHostShare(ctx context.Context, request *hostv1.RebindHostShareRequest) (*hostv1.HostShareResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetExpectedRevision() == 0 {
		return nil, invalidField("expected_revision", "current share revision is required")
	}
	value, err := server.controller.store.GetHostShare(ctx, request.GetShareId())
	if err != nil {
		return nil, err
	}
	value.Source = store.HostShareSource{Path: request.GetSourcePath()}
	value.ReadOnly = request.GetReadOnly()
	value.Revision = request.GetExpectedRevision() + 1
	return server.controller.mutateHostShare(ctx, "host-share-rebind", request.GetRequestId(), request, value, request.GetExpectedRevision())
}

func (server *operatorServer) RevokeHostShare(ctx context.Context, request *hostv1.RevokeHostShareRequest) (*hostv1.HostShareResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetExpectedRevision() == 0 {
		return nil, invalidField("expected_revision", "current share revision is required")
	}
	value, err := server.controller.store.GetHostShare(ctx, request.GetShareId())
	if err != nil {
		return nil, err
	}
	value.Revision = request.GetExpectedRevision() + 1
	value.State = hostv1.HostShareState_HOST_SHARE_STATE_REVOKED
	return server.controller.mutateHostShare(ctx, "host-share-revoke", request.GetRequestId(), request, value, request.GetExpectedRevision())
}

func (controller *Controller) mutateHostShare(ctx context.Context, kind, requestID string, request proto.Message, candidate store.HostShare, expected uint64) (*hostv1.HostShareResponse, error) {
	if requestID == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	release, err := controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	unlock := controller.lockEnvironment(candidate.EnvironmentID)
	defer unlock()
	controller.hostShareMu.Lock()
	defer controller.hostShareMu.Unlock()
	hash := runtimeRequestHash(request)
	operation, err := controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if operation.Kind != kind || operation.EnvironmentID != candidate.EnvironmentID || !bytes.Equal(operation.RequestSHA256, hash) {
			return nil, resourceError(codes.AlreadyExists, "REQUEST_ID_CONFLICT", "host_share", candidate.ID, "request ID belongs to a different owner or payload")
		}
		if operation.State == int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
			response := new(hostv1.HostShareResponse)
			err := proto.Unmarshal(operation.Receipt, response)
			return response, err
		}
		if err := json.Unmarshal(operation.Receipt, &candidate); err != nil {
			return nil, err
		}
	} else {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if controller.manager != nil && controller.vmLifecycle.snapshot().GetState() != hostv1.VMState_VM_STATE_STOPPED {
			return nil, resourceError(codes.FailedPrecondition, "HOST_SHARE_REQUIRES_VM_STOPPED", "vm", "local", "stop the VM before creating, rebinding, or revoking host shares")
		}
		if err := controller.requireHostShareOffline(); err != nil {
			return nil, err
		}
		approved, err := controller.store.HostShareEnvironmentApproved(ctx, candidate.EnvironmentID)
		if err != nil {
			return nil, err
		}
		if !approved && kind != "host-share-revoke" {
			return nil, resourceError(codes.PermissionDenied, "HOST_SHARE_ENVIRONMENT_NOT_APPROVED", "environment", candidate.EnvironmentID, "host shares require an operator-approved service environment")
		}
		if kind != "host-share-revoke" {
			source, identity, err := openHostShareSource(candidate.Source.Path)
			if err != nil {
				return nil, resourceError(codes.FailedPrecondition, "HOST_SHARE_SOURCE_UNAVAILABLE", "host_share", candidate.ID, err.Error())
			}
			source.Close()
			candidate.Source = identity
			candidate.State = hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE
		}
		receipt, err := json.Marshal(candidate)
		if err != nil {
			return nil, err
		}
		id := sha256.Sum256([]byte("host-share\x00" + requestID))
		operation, err = controller.store.BeginHostShareMutation(ctx, store.Operation{ID: "share-" + hex.EncodeToString(id[:]), Kind: kind, EnvironmentID: candidate.EnvironmentID, RequestID: requestID, RequestSHA256: hash, Receipt: receipt, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "accepted"}, candidate, expected)
		if err != nil {
			return nil, resourceError(codes.Aborted, "HOST_SHARE_REVISION_CONFLICT", "host_share", candidate.ID, err.Error())
		}
	}
	return controller.finishHostShareOperationLocked(ctx, operation, candidate)
}

func (controller *Controller) finishHostShareOperationLocked(ctx context.Context, operation store.Operation, candidate store.HostShare) (*hostv1.HostShareResponse, error) {
	if err := controller.requireHostShareOffline(); err != nil {
		return nil, err
	}
	current, err := controller.store.GetHostShare(ctx, candidate.ID)
	if err != nil {
		return nil, err
	}
	if current.OperationID != operation.ID || current.Revision != candidate.Revision {
		return nil, resourceError(codes.Aborted, "HOST_SHARE_REVISION_CONFLICT", "host_share", candidate.ID, "share authority changed")
	}
	candidate.OperationID = operation.ID
	candidate.UnavailableReason = ""
	if candidate.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		approved, err := controller.store.HostShareEnvironmentApproved(ctx, candidate.EnvironmentID)
		if err != nil {
			return nil, err
		}
		if !approved {
			return nil, resourceError(codes.PermissionDenied, "HOST_SHARE_ENVIRONMENT_NOT_APPROVED", "environment", candidate.EnvironmentID, "service environment is no longer approved")
		}
		candidate.State = hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE
		candidate.UnavailableReason = "VM is stopped"
	}
	response := &hostv1.HostShareResponse{Share: hostShareFromStore(candidate)}
	receipt, err := proto.Marshal(response)
	if err != nil {
		return nil, err
	}
	operation.Receipt = receipt
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED)
	operation.Stage = "settled"
	if err := controller.store.FinishHostShareMutation(ctx, operation, candidate); err != nil {
		return nil, fmt.Errorf("persist share result: %w", err)
	}
	return response, nil
}
