//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (server *applianceAdminServer) Activate(ctx context.Context, request *appliancev1.ActivateRequest) (*appliancev1.ApplianceOperationResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	response, err := server.setDesiredState(ctx, mutation, request.GetExpectedRevision(), true,
		appliancev1.ApplianceState_APPLIANCE_STATE_STARTING,
		appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE, "activation-pending")
	if err != nil || response.GetOperation().GetPhase() != appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED {
		return response, err
	}
	operation, err := server.finishActivation(ctx, response.GetOperation().GetOperationId(), request.GetInstallationId(), "activation-failed", "APPLIANCE_START_FAILED")
	if err != nil {
		return nil, err
	}
	return applianceOperationResponse(operation), nil
}

// finishActivation starts the appliance for an admitted activation and settles
// that operation. A Host restart resumes the same operation through it.
func (server *applianceAdminServer) finishActivation(ctx context.Context, operationID, installationID, failedStage, reason string) (store.ApplianceOperation, error) {
	if err := server.controller.activateAppliance(ctx, installationID); err != nil {
		_, _ = server.failApplianceOperation(ctx, operationID,
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, failedStage, reason, err)
		return store.ApplianceOperation{}, err
	}
	return server.completeApplianceOperation(ctx, operationID,
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "running", nil)
}

func (server *applianceAdminServer) Stop(ctx context.Context, request *appliancev1.StopApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	response, err := server.setDesiredState(ctx, mutation, request.GetExpectedRevision(), false,
		appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED,
		appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP, "stop-pending")
	if err != nil || response.GetOperation().GetPhase() != appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED {
		return response, err
	}
	if err := server.controller.stopAppliance(ctx, request.GetInstallationId()); err != nil {
		_, _ = server.failApplianceOperation(ctx, response.GetOperation().GetOperationId(),
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED,
			"stop-failed", "APPLIANCE_STOP_FAILED", err)
		return nil, err
	}
	operation, err := server.completeApplianceOperation(ctx, response.GetOperation().GetOperationId(),
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "stopped", nil)
	return applianceOperationResponse(operation), err
}

func (server *applianceAdminServer) Restart(ctx context.Context, request *appliancev1.RestartApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	response, err := server.setDesiredState(ctx, mutation, request.GetExpectedRevision(), true,
		appliancev1.ApplianceState_APPLIANCE_STATE_STARTING,
		appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART, "restart-pending")
	if err != nil || response.GetOperation().GetPhase() != appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED {
		return response, err
	}
	if err = server.controller.stopAppliance(ctx, request.GetInstallationId()); err == nil {
		err = server.controller.activateAppliance(ctx, request.GetInstallationId())
	}
	if err != nil {
		_, _ = server.failApplianceOperation(ctx, response.GetOperation().GetOperationId(),
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED,
			"restart-failed", "APPLIANCE_RESTART_FAILED", err)
		return nil, err
	}
	operation, err := server.completeApplianceOperation(ctx, response.GetOperation().GetOperationId(),
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "running", nil)
	return applianceOperationResponse(operation), err
}

func (server *applianceAdminServer) Revoke(ctx context.Context, request *appliancev1.RevokeApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	response, err := server.setDesiredState(ctx, mutation, request.GetExpectedRevision(), false,
		appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED,
		appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE, "revoke-pending")
	if err != nil || response.GetOperation().GetPhase() != appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED {
		return response, err
	}
	// Revoke the private domain even when trust cleanup fails. The installation
	// is already fenced disabled, so leaving the route enabled would be a
	// fail-open result; reconciliation retries whichever external cleanup did
	// not complete.
	domainErr := server.controller.revokePrivateControllerDomain(ctx, request.GetInstallationId())
	trustErr := server.controller.revokeApplianceTrustAuthority(ctx, request.GetInstallationId())
	if errors.Is(trustErr, sql.ErrNoRows) {
		trustErr = nil
	}
	stopErr := server.controller.stopAppliance(ctx, request.GetInstallationId())
	if err := errors.Join(trustErr, domainErr, stopErr); err != nil {
		_, _ = server.failApplianceOperation(ctx, response.GetOperation().GetOperationId(),
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED,
			"revoke-failed", "APPLIANCE_REVOKE_FAILED", err)
		return nil, err
	}
	operation, err := server.completeApplianceOperation(ctx, response.GetOperation().GetOperationId(),
		appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "revoked", nil)
	return applianceOperationResponse(operation), err
}
