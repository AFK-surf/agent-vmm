//go:build darwin || linux

package hostshare

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
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func hostShareFromStore(value store.HostShare) *hostv1.HostShare {
	return &hostv1.HostShare{Id: value.ID, EnvironmentId: value.EnvironmentID, SourcePath: value.Source.Path, ReadOnly: value.ReadOnly, Revision: value.Revision, State: value.State, UnavailableReason: value.UnavailableReason}
}

func (controller *Owner) ListHostShares(ctx context.Context, request *hostv1.ListHostSharesRequest) (*hostv1.ListHostSharesResponse, error) {
	values, err := controller.store.ListHostShares(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	response := new(hostv1.ListHostSharesResponse)
	for _, value := range values {
		response.Shares = append(response.Shares, hostShareFromStore(value))
	}
	return response, nil
}

func (controller *Owner) CreateHostShare(ctx context.Context, request *hostv1.CreateHostShareRequest) (*hostv1.CreateHostShareResponse, error) {
	if request.GetEnvironmentId() == "" {
		return nil, hostv1.InvalidField("environment_id", "service environment is required")
	}
	return controller.mutateHostShare(ctx, "host-share-create", request.GetRequestId(), request, store.HostShare{ID: shareID(request.GetRequestId()), EnvironmentID: request.GetEnvironmentId(), Source: store.HostShareSource{Path: request.GetSourcePath()}, ReadOnly: request.GetReadOnly(), Revision: 1}, 0)
}

func (controller *Owner) RebindHostShare(ctx context.Context, request *hostv1.RebindHostShareRequest) (*hostv1.RebindHostShareResponse, error) {
	if request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("expected_revision", "current share revision is required")
	}
	value, err := controller.store.GetHostShare(ctx, request.GetShareId())
	if err != nil {
		return nil, err
	}
	value.Source = store.HostShareSource{Path: request.GetSourcePath()}
	value.ReadOnly = request.GetReadOnly()
	value.Revision = request.GetExpectedRevision() + 1
	response, err := controller.mutateHostShare(ctx, "host-share-rebind", request.GetRequestId(), request, value, request.GetExpectedRevision())
	return &hostv1.RebindHostShareResponse{Share: response.GetShare()}, err
}

func (controller *Owner) RevokeHostShare(ctx context.Context, request *hostv1.RevokeHostShareRequest) (*hostv1.RevokeHostShareResponse, error) {
	if request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("expected_revision", "current share revision is required")
	}
	value, err := controller.store.GetHostShare(ctx, request.GetShareId())
	if err != nil {
		return nil, err
	}
	value.Revision = request.GetExpectedRevision() + 1
	value.State = hostv1.HostShareState_HOST_SHARE_STATE_REVOKED
	response, err := controller.mutateHostShare(ctx, "host-share-revoke", request.GetRequestId(), request, value, request.GetExpectedRevision())
	return &hostv1.RevokeHostShareResponse{Share: response.GetShare()}, err
}

func (controller *Owner) mutateHostShare(ctx context.Context, kind, requestID string, request proto.Message, candidate store.HostShare, expected uint64) (*hostv1.CreateHostShareResponse, error) {
	if err := hostSharesSupported(); err != nil {
		return nil, err
	}
	if requestID == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	ctx, release, err := controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	unlock, lockErr := controller.workloads.LockEnvironment(ctx, candidate.EnvironmentID)
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	body, _ := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	digest := sha256.Sum256(body)
	hash := digest[:]
	operation, err := controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if operation.Kind != kind || operation.EnvironmentID != candidate.EnvironmentID || !bytes.Equal(operation.RequestSHA256, hash) {
			return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID belongs to a different owner or payload", hostv1.WithResource("host_share", candidate.ID))
		}
		if operation.State == int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
			response := new(hostv1.CreateHostShareResponse)
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
		if controller.manager != nil && controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_STOPPED {
			return nil, status.Error(codes.FailedPrecondition, "stop the VM before creating, rebinding, or revoking host shares")
		}
		if err := controller.requireHostShareOffline(); err != nil {
			return nil, err
		}
		approved, err := controller.store.HostShareEnvironmentApproved(ctx, candidate.EnvironmentID)
		if err != nil {
			return nil, err
		}
		if !approved && kind != "host-share-revoke" {
			return nil, status.Error(codes.PermissionDenied, "host shares require an operator-approved service environment")
		}
		if kind != "host-share-revoke" {
			source, identity, err := openHostShareSource(candidate.Source.Path)
			if err != nil {
				if status.Code(err) == codes.FailedPrecondition {
					return nil, err
				}
				return nil, status.Error(codes.FailedPrecondition, err.Error())
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
			return nil, status.Error(codes.Aborted, err.Error())
		}
	}
	return controller.finishHostShareOperationLocked(ctx, operation, candidate)
}

func (controller *Owner) finishHostShareOperationLocked(ctx context.Context, operation store.Operation, candidate store.HostShare) (*hostv1.CreateHostShareResponse, error) {
	if err := controller.requireHostShareOffline(); err != nil {
		return nil, err
	}
	current, err := controller.store.GetHostShare(ctx, candidate.ID)
	if err != nil {
		return nil, err
	}
	if current.OperationID != operation.ID || current.Revision != candidate.Revision {
		return nil, status.Error(codes.Aborted, "share authority changed")
	}
	candidate.OperationID = operation.ID
	candidate.UnavailableReason = ""
	if candidate.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		approved, err := controller.store.HostShareEnvironmentApproved(ctx, candidate.EnvironmentID)
		if err != nil {
			return nil, err
		}
		if !approved {
			return nil, status.Error(codes.PermissionDenied, "service environment is no longer approved")
		}
		candidate.State = hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE
		candidate.UnavailableReason = "VM is stopped"
	}
	response := &hostv1.CreateHostShareResponse{Share: hostShareFromStore(candidate)}
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

func shareID(requestID string) string {
	digest := sha256.Sum256([]byte("share\x00" + requestID))
	return hex.EncodeToString(digest[:16])
}
