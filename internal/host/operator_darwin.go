//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type operatorServer struct {
	hostv1.UnimplementedOperatorServiceServer
	controller *Controller
}

var errVMMutationFence = errors.New("VM mutation fence conflict")

const (
	vmOperatorExecutionTimeout = 2 * time.Minute
	vmOperatorTerminalTimeout  = 5 * time.Second
)

func (server *operatorServer) StartVM(ctx context.Context, request *hostv1.StartVMRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	if request.GetFence() == nil {
		return nil, invalidField("fence", "VM mutation fence is required")
	}
	return server.controller.startVM(ctx, request.GetRequestId(), request.GetFence())
}

// StartVM serializes an authenticated operator start with the bounded
// unexpected-stop recovery owner.
func (controller *Controller) StartVM(ctx context.Context, requestID string) (*hostv1.OperationResponse, error) {
	return controller.startVM(ctx, requestID, nil)
}

func (controller *Controller) startVM(ctx context.Context, requestID string, fence *hostv1.VMMutationFence) (*hostv1.OperationResponse, error) {
	if requestID == "" {
		return nil, invalidField("request_id", "request ID is required")
	}
	return controller.executeOperatorVMOperation(ctx, vmLifecycleStart, "vm-start", requestID, "running", "VM_START_FAILED", func(executionCtx context.Context, operation store.Operation) error {
		if err := controller.recoverInterruptedApplianceAdminOperations(executionCtx); err != nil {
			return fmt.Errorf("recover interrupted appliance operations: %w", err)
		}
		if err := controller.recoverPendingApplianceRemovals(executionCtx); err != nil {
			return fmt.Errorf("recover pending appliance removals: %w", err)
		}
		if fence != nil {
			if err := controller.validateVMMutationFenceLocked(executionCtx, fence); err != nil {
				return err
			}
		}
		startContext := vmm.WithStartObservation(executionCtx, operation.ID, requestID)
		return controller.vmLifecycle.startOwned(startContext)
	})
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
	if request.GetFence() == nil {
		return nil, invalidField("fence", "VM mutation fence is required")
	}
	return server.controller.executeOperatorVMOperation(ctx, vmLifecycleStop, "vm-stop", request.GetRequestId(), "stopped", "VM_STOP_FAILED", func(executionCtx context.Context, _ store.Operation) error {
		if err := server.controller.validateVMMutationFenceLocked(executionCtx, request.GetFence()); err != nil {
			return err
		}
		return server.controller.vmLifecycle.stopOwned(executionCtx, request.GetForce())
	})
}

func (controller *Controller) executeOperatorVMOperation(ctx context.Context, lifecycleOperation vmLifecycleOperation, kind, requestID, successStage, failureReason string, run func(context.Context, store.Operation) error) (*hostv1.OperationResponse, error) {
	release, err := controller.vmLifecycle.acquire(ctx, lifecycleOperation)
	if err != nil {
		return controller.reconcileOperatorVMAdmission(err, kind, requestID)
	}
	defer release()
	if err := ctx.Err(); err != nil {
		return controller.reconcileOperatorVMAdmission(err, kind, requestID)
	}
	acceptCtx, cancelAccept := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	operation, existing, err := controller.beginOperation(acceptCtx, kind, "system", requestID)
	cancelAccept()
	if err != nil {
		return nil, err
	}
	if existing {
		if operation.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			operation.State = int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED)
			operation.Stage = "interrupted"
			operation.ErrorReason = "VM_TERMINAL_RESULT_UNKNOWN"
			operation.ErrorMessage = "the accepted VM mutation did not save a terminal result; inspect the VM and use a new request ID"
			operation.UpdatedAt = time.Now()
			if err := controller.persistOperatorVMTerminal(&operation); err != nil {
				return nil, operatorVMTerminalError(operation)
			}
		}
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), vmOperatorExecutionTimeout)
	defer cancelExecution()
	if err := run(executionCtx, operation); err != nil {
		reason := failureReason
		code := codes.FailedPrecondition
		if errors.Is(err, errVMMutationFence) {
			reason = "VM_FENCE_CONFLICT"
			code = codes.Aborted
		}
		if persistErr := controller.persistOperatorVMFailure(&operation, reason, err); persistErr != nil {
			return nil, operatorVMTerminalError(operation)
		}
		return nil, resourceError(code, reason, "vm", "local", err.Error())
	}
	if err := controller.persistOperatorVMSuccess(&operation, successStage); err != nil {
		return nil, operatorVMTerminalError(operation)
	}
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (controller *Controller) reconcileOperatorVMAdmission(admissionErr error, kind, requestID string) (*hostv1.OperationResponse, error) {
	readCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	operation, err := controller.store.OperationByRequestID(readCtx, requestID)
	if err == nil {
		if operation.Kind != kind || operation.EnvironmentID != "system" {
			return nil, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", operation.ID, "request ID belongs to a different operation")
		}
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		if errors.Is(admissionErr, context.DeadlineExceeded) {
			return nil, resourceError(codes.DeadlineExceeded, "VM_ADMISSION_TIMEOUT", "vm", "local", "VM mutation timed out before an operation was accepted")
		}
		if errors.Is(admissionErr, context.Canceled) {
			return nil, resourceError(codes.Canceled, "VM_ADMISSION_CANCELED", "vm", "local", "VM mutation was canceled before an operation was accepted")
		}
		return nil, admissionErr
	}
	return nil, resourceErrorWithMetadata(codes.Unavailable, "VM_ADMISSION_OUTCOME_UNKNOWN", "vm", "local", "VM mutation admission failed and the request result could not be read", map[string]string{"request_id": requestID})
}

func (controller *Controller) persistOperatorVMFailure(operation *store.Operation, reason string, failure error) error {
	terminalCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	return controller.failOperation(terminalCtx, operation, reason, failure)
}

func (controller *Controller) persistOperatorVMSuccess(operation *store.Operation, stage string) error {
	terminalCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	return controller.completeOperation(terminalCtx, operation, stage)
}

func (controller *Controller) persistOperatorVMTerminal(operation *store.Operation) error {
	terminalCtx, cancel := context.WithTimeout(context.Background(), vmOperatorTerminalTimeout)
	defer cancel()
	return controller.store.UpdateOperation(terminalCtx, *operation)
}

func operatorVMTerminalError(operation store.Operation) error {
	return resourceErrorWithMetadata(codes.Unavailable, "VM_TERMINAL_PERSIST_FAILED", "operation", operation.ID, "VM mutation was accepted but its terminal result could not be saved", map[string]string{"operation_id": operation.ID, "request_id": operation.RequestID})
}

func (controller *Controller) validateVMMutationFence(ctx context.Context, expected *hostv1.VMMutationFence) error {
	controller.inventoryTransition.Lock()
	defer controller.inventoryTransition.Unlock()
	return controller.validateVMMutationFenceLocked(ctx, expected)
}

func (controller *Controller) validateVMMutationFenceLocked(ctx context.Context, expected *hostv1.VMMutationFence) error {
	if expected == nil || !expected.GetWholeVmImpactAcknowledged() {
		return fmt.Errorf("%w: whole-VM impact was not acknowledged", errVMMutationFence)
	}
	current := controller.vmImpactSummaryLocked(ctx)
	if current.GetVm().GetState() != expected.GetState() || current.GetVm().GetBootId() != expected.GetBootId() {
		return fmt.Errorf("%w: state or boot identity changed", errVMMutationFence)
	}
	currentByIdentity := make(map[string]*hostv1.ResourceReference, len(current.GetKnownWorkloads()))
	for _, reference := range current.GetKnownWorkloads() {
		currentByIdentity[vmImpactIdentity(reference)] = reference
	}
	for _, observed := range expected.GetObservedWorkloads() {
		if latest := currentByIdentity[vmImpactIdentity(observed)]; latest != nil && !proto.Equal(latest, observed) {
			return fmt.Errorf("%w: observed workload owner revision changed", errVMMutationFence)
		}
	}
	return nil
}

func vmImpactIdentity(reference *hostv1.ResourceReference) string {
	return reference.GetKind() + "\x00" + reference.GetEnvironmentId() + "\x00" + reference.GetResourceId()
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
	if _, err := server.controller.vmLifecycle.Start(ctx); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "VM_START_FAILED", "vm", "local", err.Error())
	}
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return nil, resourceError(codes.FailedPrecondition, "VM_NOT_RUNNING", "vm", "local", "VM stopped before the policy mutation could begin; retry after it is running")
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
