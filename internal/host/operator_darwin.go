//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type operatorServer struct {
	hostv1.UnimplementedOperatorServiceServer
	controller *Controller
}

func (server *operatorServer) StartVM(ctx context.Context, request *hostv1.StartVMRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	operation, existing, err := server.controller.beginOperation(ctx, "vm-start", "system", request.GetRequestId())
	if err != nil {
		return nil, err
	}
	if existing {
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	if err := server.controller.recoverInterruptedApplianceAdminOperations(ctx); err != nil {
		server.controller.failOperation(ctx, &operation, "APPLIANCE_OPERATION_RECOVERY_FAILED", err)
		return nil, resourceError(codes.FailedPrecondition, "APPLIANCE_OPERATION_RECOVERY_FAILED", "vm", "local", err.Error())
	}
	if err := server.controller.recoverPendingApplianceRemovals(ctx); err != nil {
		server.controller.failOperation(ctx, &operation, "APPLIANCE_REMOVE_RECOVERY_FAILED", err)
		return nil, resourceError(codes.FailedPrecondition, "APPLIANCE_REMOVE_RECOVERY_FAILED", "vm", "local", err.Error())
	}
	if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		err = server.controller.configureApplianceBlocks(ctx)
	}
	if err == nil {
		_, err = server.controller.manager.Start(ctx)
	}
	if err != nil {
		server.controller.failOperation(ctx, &operation, "VM_START_FAILED", err)
		return nil, resourceError(codes.FailedPrecondition, "VM_START_FAILED", "vm", "local", err.Error())
	}
	if err := server.controller.startEgress(); err != nil {
		server.controller.rollbackStartedVM()
		server.controller.failOperation(ctx, &operation, "EGRESS_START_FAILED", err)
		return nil, resourceError(codes.FailedPrecondition, "EGRESS_START_FAILED", "vm", "local", err.Error())
	}
	if err := server.controller.reconcileAppliances(ctx); err != nil {
		_ = server.controller.store.Audit(ctx, "system", "system", "appliance.reconcile", "", "", int32(codes.FailedPrecondition), "APPLIANCE_RECONCILE_PARTIAL")
	}
	if err := server.controller.reconcilePolicies(ctx); err != nil {
		server.controller.rollbackStartedVM()
		server.controller.failOperation(ctx, &operation, "POLICY_RECONCILE_FAILED", err)
		return nil, retryable("POLICY_RECONCILE_FAILED", "VM started but policy projection failed", time.Second)
	}
	server.controller.completeOperation(ctx, &operation, "running")
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (controller *Controller) rollbackStartedVM() {
	controller.stopEgress()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = controller.manager.Stop(ctx, true)
}

func (server *operatorServer) GetVM(ctx context.Context, _ *hostv1.GetVMRequest) (*hostv1.VMResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	return &hostv1.VMResponse{Vm: server.controller.manager.Snapshot()}, nil
}

func (server *operatorServer) StopVM(ctx context.Context, request *hostv1.StopVMRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	operation, existing, err := server.controller.beginOperation(ctx, "vm-stop", "system", request.GetRequestId())
	if err != nil {
		return nil, err
	}
	if existing {
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	server.controller.stopEgress()
	server.controller.invalidateRuntimeCapabilities(ctx)
	_, err = server.controller.manager.Stop(ctx, request.GetForce())
	if err != nil {
		if server.controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
			_ = server.controller.startEgress()
			_ = server.controller.reconcilePolicies(context.Background())
		}
		server.controller.failOperation(ctx, &operation, "VM_STOP_FAILED", err)
		return nil, resourceError(codes.FailedPrecondition, "VM_STOP_FAILED", "vm", "local", err.Error())
	}
	server.controller.completeOperation(ctx, &operation, "stopped")
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) CreateAgentSession(ctx context.Context, request *hostv1.CreateAgentSessionRequest) (*hostv1.CreateAgentSessionResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	if request.GetIntegrationId() == "" {
		return nil, invalidField("integration_id", "integration ID is required")
	}
	unlock := server.controller.lockIntegration(request.GetIntegrationId())
	defer unlock()
	integration, err := server.controller.store.GetAgentIntegration(ctx, request.GetIntegrationId())
	if err != nil || integration.DesiredState != int32(hostv1.IntegrationDesiredState_INTEGRATION_DESIRED_STATE_ALLOWED) ||
		(integration.ConfigState != int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_READY) &&
			integration.ConfigState != int32(hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_NEEDS_RESTART)) {
		return nil, resourceError(codes.FailedPrecondition, "INTEGRATION_NOT_READY", "integration", request.GetIntegrationId(), "agent integration is not ready")
	}
	session, environment, err := server.controller.createSession(ctx, integration.EnvironmentID, integration.ID, store.OwnerLocal, auth.Identity{})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", integration.EnvironmentID, "integration environment does not exist")
		}
		return nil, resourceError(codes.FailedPrecondition, "SESSION_CREATE_FAILED", "environment", integration.EnvironmentID, err.Error())
	}
	return &hostv1.CreateAgentSessionResponse{
		SessionId: session.ID, SocketPath: session.SocketPath, Environment: environmentFromStore(environment), ExpiresAt: timestamppb.New(session.ExpiresAt),
	}, nil
}

func (server *operatorServer) RevokeAgentSession(ctx context.Context, request *hostv1.RevokeAgentSessionRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetSessionId() == "" {
		return nil, invalidField("request_id/session_id", "request and session IDs are required")
	}
	if err := server.controller.revokeSession(ctx, request.GetSessionId()); err != nil {
		return nil, resourceError(codes.NotFound, "SESSION_NOT_FOUND", "session", request.GetSessionId(), "agent session does not exist or is already revoked")
	}
	return &emptypb.Empty{}, nil
}

func (server *operatorServer) ListEnvironments(ctx context.Context, _ *hostv1.ListEnvironmentsRequest) (*hostv1.ListEnvironmentsResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	environments, err := server.controller.store.ListEnvironments(ctx)
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListEnvironmentsResponse{}
	for _, environment := range environments {
		response.Environments = append(response.Environments, server.controller.environmentSnapshot(ctx, environment))
	}
	return response, nil
}

func (server *operatorServer) GetEnvironment(ctx context.Context, request *hostv1.GetEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", request.GetEnvironmentId(), "agent environment does not exist")
	}
	if err != nil {
		return nil, err
	}
	return &hostv1.GetEnvironmentResponse{Environment: server.controller.environmentSnapshot(ctx, environment)}, nil
}

func (server *operatorServer) UpdateEnvironmentPolicy(ctx context.Context, request *hostv1.UpdateEnvironmentPolicyRequest) (*hostv1.GetEnvironmentResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetExpectedRevision() == 0 || request.GetLimits() == nil || request.GetEgressMode() == hostv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
		return nil, invalidField("request_id/expected_revision/limits/egress_mode", "complete policy update is required")
	}
	limits := request.GetLimits()
	if err := validatePolicy(limits, request.GetEgressMode()); err != nil {
		return nil, err
	}
	if environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId()); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", request.GetEnvironmentId(), "agent environment does not exist")
		}
		return nil, err
	} else if err := requireLocalOperatorEnvironment(environment); err != nil {
		return nil, err
	}
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	environment, err := server.controller.store.UpdatePolicy(ctx, request.GetEnvironmentId(), request.GetExpectedRevision(), store.Environment{
		CPUMillis: limits.GetCpuMillis(), MemoryBytes: limits.GetMemoryBytes(), PIDs: limits.GetPids(), DiskBytes: limits.GetDiskBytes(), EgressMode: int32(request.GetEgressMode()),
	})
	if err != nil {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "environment", request.GetEnvironmentId(), err.Error())
	}
	if err := server.controller.reconcileEnvironmentLocked(ctx, environment); err != nil {
		return &hostv1.GetEnvironmentResponse{Environment: environmentFromStore(environment)}, retryable("POLICY_PROJECTION_FAILED", "host policy was saved but guest projection failed", time.Second)
	}
	return &hostv1.GetEnvironmentResponse{Environment: environmentFromStore(environment)}, nil
}

func requireLocalOperatorEnvironment(environment store.Environment) error {
	if environment.OwnerKind != store.OwnerLocal {
		return resourceError(codes.FailedPrecondition, "REMOTE_ALLOCATION_CONTROLLER_OWNED", "environment", environment.ID, "active controller-owned allocation must be changed through its controller")
	}
	return nil
}

func validatePolicy(limits *hostv1.ResourceLimits, egress hostv1.EgressMode) error {
	if limits == nil || limits.CpuMillis == nil || limits.MemoryBytes == nil || limits.Pids == nil || limits.DiskBytes == nil {
		return invalidField("limits", "cpu, memory, pids, and disk limits must all be present")
	}
	if limits.GetCpuMillis() == 0 || limits.GetCpuMillis() > 1_000_000 || limits.GetMemoryBytes() < 16<<20 || limits.GetMemoryBytes() > 1<<40 || limits.GetPids() == 0 || limits.GetPids() > 1<<20 || limits.GetDiskBytes() == 0 || limits.GetDiskBytes() > 1<<50 {
		return invalidField("limits", "policy limits are outside supported bounds")
	}
	if egress != hostv1.EgressMode_EGRESS_MODE_DENY_ALL && egress != hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET {
		return invalidField("egress_mode", "egress mode must be deny-all or public-internet")
	}
	return nil
}
