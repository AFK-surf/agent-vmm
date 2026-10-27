//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/host/workload"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type operatorServer struct {
	hostv1.UnimplementedOperatorServiceServer
	controller *Controller
}

func (server *operatorServer) StartVM(ctx context.Context, request *hostv1.StartVMRequest) (*hostv1.StartVMResponse, error) {
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	if request.GetFence() == nil {
		return nil, hostv1.InvalidField("fence", "VM mutation fence is required")
	}
	return server.controller.vmLifecycle.StartVM(ctx, request.GetRequestId(), request.GetFence())
}

// StartVM serializes an authenticated operator start with the bounded
// unexpected-stop recovery owner.
func (controller *Controller) StartVM(ctx context.Context, requestID string) (*hostv1.StartVMResponse, error) {
	return controller.vmLifecycle.StartVM(ctx, requestID, nil)
}

func (server *operatorServer) GetVM(ctx context.Context, _ *hostv1.GetVMRequest) (*hostv1.GetVMResponse, error) {
	return &hostv1.GetVMResponse{Vm: server.controller.manager.Snapshot()}, nil
}

func (server *operatorServer) StopVM(ctx context.Context, request *hostv1.StopVMRequest) (*hostv1.StopVMResponse, error) {
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	if request.GetFence() == nil {
		return nil, hostv1.InvalidField("fence", "VM mutation fence is required")
	}
	return server.controller.vmLifecycle.StopVM(ctx, request)
}

func (server *operatorServer) CreateAgentSession(ctx context.Context, request *hostv1.CreateAgentSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "request ID is required")
	}
	if request.GetIntegrationId() == "" {
		return nil, hostv1.InvalidField("integration_id", "integration ID is required")
	}
	unlock, lockErr := server.controller.sessions.LockIntegration(ctx, request.GetIntegrationId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	integration, err := server.controller.store.GetAgentIntegration(ctx, request.GetIntegrationId())
	if err != nil || integration.DesiredState != int32(hostv1.IntegrationDesiredState_INTEGRATION_DESIRED_STATE_ALLOWED) ||
		(integration.ConfigState != int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_READY) &&
			integration.ConfigState != int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_NEEDS_RESTART)) {
		return nil, status.Error(codes.FailedPrecondition, "agent integration is not ready")
	}
	if request.ExpectedIntegrationRevision != nil && request.GetExpectedIntegrationRevision() != integration.Revision {
		return nil, integrationConflict(store.ErrOperationReceiptConflict)
	}
	ctx, leave, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer leave()
	if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING || !server.controller.vmLifecycle.AdmitsRemoteWork() {
		return nil, status.Error(codes.FailedPrecondition, "VM is not running")
	}
	unlockEnvironment, err := server.controller.workloads.LockEnvironment(ctx, integration.EnvironmentID)
	if err != nil {
		return nil, err
	}
	defer unlockEnvironment()
	session, environment, err := server.controller.sessions.Create(ctx, integration.EnvironmentID, integration.ID, integration.Revision, store.OwnerLocal, auth.Identity{})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "integration environment does not exist")
		}
		return nil, err
	}
	return &hostv1.CreateAgentSessionResponse{
		SessionId: session.ID, SocketPath: session.SocketPath, Environment: workload.EnvironmentFromStore(environment), ExpiresAt: timestamppb.New(session.ExpiresAt),
	}, nil
}

func (server *operatorServer) RevokeAgentSession(ctx context.Context, request *hostv1.RevokeAgentSessionRequest) (*hostv1.RevokeAgentSessionResponse, error) {
	if request.GetRequestId() == "" || request.GetSessionId() == "" {
		return nil, hostv1.InvalidField("request_id/session_id", "request and session IDs are required")
	}
	if err := server.controller.sessions.Revoke(ctx, request.GetSessionId()); err != nil {
		return nil, status.Error(codes.NotFound, "agent session does not exist or is already revoked")
	}
	return &hostv1.RevokeAgentSessionResponse{}, nil
}

func (server *operatorServer) ListEnvironments(ctx context.Context, _ *hostv1.ListEnvironmentsRequest) (*hostv1.ListEnvironmentsResponse, error) {
	environments, err := server.controller.store.ListEnvironments(ctx)
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListEnvironmentsResponse{}
	for _, environment := range environments {
		response.Environments = append(response.Environments, server.controller.services.EnvironmentSnapshot(ctx, environment))
	}
	return response, nil
}

func (server *operatorServer) GetEnvironment(ctx context.Context, request *hostv1.OperatorServiceGetEnvironmentRequest) (*hostv1.OperatorServiceGetEnvironmentResponse, error) {
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "agent environment does not exist")
	}
	if err != nil {
		return nil, err
	}
	return &hostv1.OperatorServiceGetEnvironmentResponse{Environment: server.controller.services.EnvironmentSnapshot(ctx, environment)}, nil
}

func (server *operatorServer) UpdateEnvironmentPolicy(ctx context.Context, request *hostv1.UpdateEnvironmentPolicyRequest) (*hostv1.UpdateEnvironmentPolicyResponse, error) {
	if request.GetRequestId() == "" || request.GetExpectedRevision() == 0 || request.GetLimits() == nil || request.GetEgressMode() == hostv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
		return nil, hostv1.InvalidField("request_id/expected_revision/limits/egress_mode", "complete policy update is required")
	}
	limits := request.GetLimits()
	if err := workload.ValidatePolicy(limits, request.GetEgressMode()); err != nil {
		return nil, err
	}
	if environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId()); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "agent environment does not exist")
		}
		return nil, err
	} else if err := requireLocalOperatorEnvironment(environment); err != nil {
		return nil, err
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

	if err := server.controller.services.ProjectionLock().Lock(ctx); err != nil {
		return nil, err
	}

	defer server.controller.services.ProjectionLock().Unlock()
	environment, err := server.controller.store.UpdatePolicy(ctx, request.GetEnvironmentId(), request.GetExpectedRevision(), store.Environment{
		PIDs: limits.GetPids(), DiskBytes: limits.GetDiskBytes(), EgressMode: int32(request.GetEgressMode()),
	})
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	if server.controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING && server.controller.vmLifecycle.AdmitsRemoteWork() {
		if err := server.controller.services.ReconcileEnvironmentLocked(ctx, environment); err != nil {
			return &hostv1.UpdateEnvironmentPolicyResponse{Environment: workload.EnvironmentFromStore(environment)}, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_POLICY_PROJECTION_FAILED, "host policy was saved but guest projection failed", hostv1.WithRetry(time.Second))
		}
	}
	return &hostv1.UpdateEnvironmentPolicyResponse{Environment: workload.EnvironmentFromStore(environment)}, nil
}

func requireLocalOperatorEnvironment(environment store.Environment) error {
	if environment.OwnerKind != store.OwnerLocal {
		return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_REMOTE_ALLOCATION_CONTROLLER_OWNED, "active controller-owned allocation must be changed through its controller", hostv1.WithResource("environment", environment.ID))
	}
	return nil
}
