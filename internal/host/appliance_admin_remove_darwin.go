//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

func (server *applianceAdminServer) PlanSealedRepair(ctx context.Context, request *appliancev1.PlanSealedRepairRequest) (*appliancev1.AppliancePlanResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	if installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		return nil, resourceError(codes.FailedPrecondition, "INSTALLATION_NOT_SEALED", "appliance_installation", installation.ID, "repair requires a sealed installation")
	}
	return &appliancev1.AppliancePlanResponse{Plan: &appliancev1.AppliancePlan{InstallationId: installation.ID,
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_SEALED_REPAIR, Revision: installation.Revision,
		ArtifactId: installation.ArtifactID, Impacts: []*appliancev1.ApplianceImpact{{ResourceType: "platform_key",
			ResourceId: installation.ID, Effect: "replace", Irreversible: true}}}}, nil
}

func (server *applianceAdminServer) ExecuteSealedRepair(ctx context.Context, request *appliancev1.ExecuteSealedRepairRequest) (*appliancev1.ApplianceOperationResponse, error) {
	defer clearSecret(request.RecoveryMaterial)
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	if server.controller.applianceSecrets == nil || len(request.GetRecoveryMaterial()) != 32 {
		return nil, resourceError(codes.FailedPrecondition, "RECOVERY_MATERIAL_INVALID", "appliance_installation", request.GetInstallationId(), "recovery material is invalid")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_SEALED_REPAIR); err != nil {
		return nil, err
	} else if found {
		if existing.Kind == int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_SEALED_REPAIR) &&
			existing.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED) {
			installation, _, err := server.exactInstallation(ctx, mutation)
			if err != nil {
				return nil, err
			}
			if installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
				return applianceOperationResponse(existing), nil
			}
			if err := server.controller.applianceSecrets.ReplaceVolumeKey(ctx, installation.ID, request.GetRecoveryMaterial()); err != nil {
				return nil, err
			}
			committed, err := server.controller.store.CommitSealedRepair(ctx, installation.ID, installation.Revision, existing.ID, nil)
			if err != nil {
				return nil, err
			}
			return applianceOperationResponse(committed), nil
		}
		return applianceOperationResponse(existing), nil
	}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	if installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		return nil, resourceError(codes.Aborted, "PLAN_REVISION_CONFLICT", "appliance_installation", installation.ID, "sealed repair plan is stale")
	}
	operation, err := server.newRunningApplianceOperation(ctx, mutation,
		appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_SEALED_REPAIR, "repairing-sealed")
	if err != nil {
		return nil, err
	}
	if err := server.controller.applianceSecrets.ReplaceVolumeKey(ctx, installation.ID, request.GetRecoveryMaterial()); err != nil {
		_, _ = server.failApplianceOperation(ctx, operation.ID,
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED,
			"sealed-repair-failed", "PLATFORM_KEY_REPAIR_FAILED", err)
		return nil, err
	}
	operation, err = server.controller.store.CommitSealedRepair(ctx, installation.ID, installation.Revision, operation.ID, nil)
	return applianceOperationResponse(operation), err
}

func (server *applianceAdminServer) PlanRemove(ctx context.Context, request *appliancev1.PlanRemoveRequest) (*appliancev1.AppliancePlanResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	plan := &appliancev1.AppliancePlan{InstallationId: installation.ID,
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_REMOVE, Revision: installation.Revision,
		ArtifactId: installation.ArtifactID, Impacts: []*appliancev1.ApplianceImpact{{ResourceType: "encrypted_volume",
			ResourceId: installation.EncryptedVolumeID, Effect: "delete", Irreversible: true}}}
	if installation.RegistrationID != "" {
		environments, err := server.controller.store.ListRemoteEnvironments(ctx, installation.RegistrationID)
		if err != nil {
			return nil, err
		}
		for _, environment := range environments {
			plan.Impacts = append(plan.Impacts, &appliancev1.ApplianceImpact{ResourceType: "allocation",
				ResourceId: environment.AllocationID, Effect: "must-discard", Blocker: true})
		}
	}
	return &appliancev1.AppliancePlanResponse{Plan: plan}, nil
}

func (server *applianceAdminServer) ExecuteRemove(ctx context.Context, request *appliancev1.ExecuteRemoveRequest) (*appliancev1.ApplianceOperationResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	server.controller.remoteAdmissionMu.Lock()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE); err != nil {
		server.controller.remoteAdmissionMu.Unlock()
		return nil, err
	} else if found {
		server.controller.remoteAdmissionMu.Unlock()
		if existing.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
			return server.resumeApplianceRemove(ctx, existing)
		}
		return applianceOperationResponse(existing), nil
	}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		server.controller.remoteAdmissionMu.Unlock()
		return nil, err
	}
	if !containsApplianceResource(request.GetConfirmedResourceIds(), installation.EncryptedVolumeID) {
		server.controller.remoteAdmissionMu.Unlock()
		return nil, resourceError(codes.FailedPrecondition, "REMOVE_CONFIRMATION_REQUIRED", "encrypted_volume", installation.EncryptedVolumeID, "exact remove plan and encrypted volume confirmation are required")
	}
	if installation.RegistrationID != "" {
		environments, listErr := server.controller.store.ListRemoteEnvironments(ctx, installation.RegistrationID)
		if listErr != nil || len(environments) != 0 {
			server.controller.remoteAdmissionMu.Unlock()
			if listErr != nil {
				return nil, listErr
			}
			return nil, resourceError(codes.FailedPrecondition, "APPLIANCE_DOMAIN_NOT_EMPTY", "appliance_installation", installation.ID, "discard every appliance allocation before removal")
		}
	}
	operationID, err := randomID()
	if err == nil {
		operation := store.ApplianceOperation{ID: operationID, InstallationID: installation.ID, RequestID: mutation.requestID,
			Kind:             int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE),
			State:            int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
			ExpectedRevision: mutation.expectedRevision, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: "remove-planned"}
		_, err = server.controller.store.BeginApplianceRemove(ctx, store.ApplianceRemove{InstallationID: installation.ID,
			OperationID: operationID, Operation: operation, ExpectedRevision: installation.Revision, ExpectedState: installation.State,
			PendingState:  int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPING),
			ExpectedPhase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
			Receipt:       []byte("pending-destroy:" + operationID)})
	}
	server.controller.remoteAdmissionMu.Unlock()
	if err != nil {
		return nil, err
	}
	operation, err := server.controller.store.GetApplianceOperation(ctx, operationID)
	if err != nil {
		return nil, err
	}
	operation.Phase = int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY)
	return server.resumeApplianceRemove(ctx, operation)
}

func containsApplianceResource(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (controller *Controller) recoverPendingApplianceRemovals(ctx context.Context) error {
	operations, err := controller.store.ListPendingApplianceRemovals(ctx)
	if err != nil {
		return err
	}
	server := &applianceAdminServer{controller: controller}
	var result error
	for _, operation := range operations {
		_, resumeErr := server.resumeApplianceRemove(ctx, operation)
		result = errors.Join(result, resumeErr)
	}
	return result
}

func (controller *Controller) recoverInterruptedApplianceAdminOperations(ctx context.Context) error {
	operations, err := controller.store.ListRunningApplianceOperations(ctx)
	if err != nil {
		return err
	}
	server := &applianceAdminServer{controller: controller}
	var result error
	for _, operation := range operations {
		if operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
			continue
		}
		_, recoverErr := server.failApplianceOperation(ctx, operation.ID,
			appliancev1.ApplianceOperationPhase(operation.Phase), "operation-interrupted",
			"HOST_RESTART_DURING_OPERATION", errors.New("host restarted during appliance operation"))
		result = errors.Join(result, recoverErr)
	}
	return result
}

func (server *applianceAdminServer) resumeApplianceRemove(ctx context.Context, operation store.ApplianceOperation) (*appliancev1.ApplianceOperationResponse, error) {
	installation, err := server.controller.store.GetApplianceInstallation(ctx, operation.InstallationID)
	if err != nil {
		return nil, err
	}
	if err := server.controller.revokeApplianceTrustAuthority(ctx, installation.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if installation.RegistrationID != "" {
		registration, err := server.controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
		if err != nil {
			return nil, err
		}
		if registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL) {
			if err := server.controller.revokePrivateControllerDomain(ctx, installation.ID); err != nil {
				return nil, err
			}
		}
	}
	if err := server.controller.stopAppliance(ctx, installation.ID); err != nil {
		return nil, err
	}
	var committed store.ApplianceOperation
	err = server.controller.withApplianceDataOfflineExcept(ctx, installation.ID, func() error {
		if err := server.controller.removeApplianceAssets(ctx, installation.ID, installation.EncryptedVolumeID); err != nil {
			return err
		}
		var commitErr error
		committed, commitErr = server.controller.store.CommitApplianceRemove(ctx, store.ApplianceRemove{
			InstallationID: installation.ID, RegistrationID: installation.RegistrationID,
			ReservationID: "system-" + installation.ID, OperationID: operation.ID,
			CommitRevision: installation.Revision, Receipt: []byte("removed:" + operation.ID)})
		return commitErr
	})
	return applianceOperationResponse(committed), err
}
