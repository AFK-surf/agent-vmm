//go:build darwin || linux

package appliance

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) PlanRemove(ctx context.Context, request *appliancev1.PlanRemoveRequest) (*appliancev1.PlanRemoveResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	installation, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	plan := &appliancev1.AppliancePlan{InstallationId: installation.ID,
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_REMOVE, Revision: installation.Revision,
		ArtifactId: installation.ArtifactID, Impacts: []*appliancev1.ApplianceImpact{
			{ResourceType: "installation", ResourceId: installation.ID, Effect: "delete", Irreversible: true},
			{ResourceType: "data_volume", ResourceId: installation.DataVolumeID, Effect: "delete", Irreversible: true}}}
	if installation.RegistrationID != "" {
		environments, err := server.store.ListRemoteEnvironments(ctx, installation.RegistrationID)
		if err != nil {
			return nil, err
		}
		plan.Impacts = append(plan.Impacts, &appliancev1.ApplianceImpact{ResourceType: "private_domain", ResourceId: installation.RegistrationID, Effect: "delete all domain data at execution", Irreversible: true})
		for _, environment := range environments {
			plan.Impacts = append(plan.Impacts, &appliancev1.ApplianceImpact{ResourceType: "allocation",
				ResourceId: environment.AllocationID, Effect: "delete", Irreversible: true})
		}
	}
	return &appliancev1.PlanRemoveResponse{Plan: plan}, nil
}

func (server *Owner) ExecuteRemove(ctx context.Context, request *appliancev1.ExecuteRemoveRequest) (*appliancev1.ExecuteRemoveResponse, error) {
	operation, err := server.acceptApplianceRemove(ctx, request)
	if err != nil {
		return nil, err
	}
	if operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
		{
			value, err := server.resumeApplianceRemove(ctx, operation, false)
			return &appliancev1.ExecuteRemoveResponse{Operation: value}, err
		}
	}
	return &appliancev1.ExecuteRemoveResponse{Operation: applianceOperationFromStore(operation)}, nil
}

func (server *Owner) acceptApplianceRemove(ctx context.Context, request *appliancev1.ExecuteRemoveRequest) (store.ApplianceOperation, error) {
	unlock, err := server.applianceLocks.Lock(ctx, request.GetInstallationId())
	if err != nil {
		return store.ApplianceOperation{}, err
	}
	defer unlock()
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	releaseRemoteAdmission := server.remote.LockAdmission()

	defer releaseRemoteAdmission()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE); err != nil {
		return store.ApplianceOperation{}, err
	} else if found {
		return existing, nil
	}
	installation, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return store.ApplianceOperation{}, err
	}
	if !containsApplianceResource(request.GetConfirmedResourceIds(), installation.DataVolumeID) {
		return store.ApplianceOperation{}, status.Error(codes.FailedPrecondition, "exact remove plan and data volume confirmation are required")
	}
	operationID, err := opreceipt.NewID()
	if err == nil {
		operation := store.ApplianceOperation{ID: operationID, InstallationID: installation.ID, RequestID: mutation.requestID,
			Kind:             int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE),
			State:            int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
			ExpectedRevision: mutation.expectedRevision, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: "remove-planned"}
		_, err = server.store.BeginApplianceRemove(ctx, store.ApplianceRemove{InstallationID: installation.ID,
			OperationID: operationID, Operation: operation, ExpectedRevision: installation.Revision, ExpectedState: installation.State,
			PendingState:  int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPING),
			ExpectedPhase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
			Receipt:       []byte("pending-destroy:" + operationID)})
	}
	if err != nil {
		return store.ApplianceOperation{}, err
	}
	return server.store.GetApplianceOperation(ctx, operationID)
}

func containsApplianceResource(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (controller *Owner) recoverPendingApplianceRemovals(ctx context.Context) error {
	operations, err := controller.store.ListPendingApplianceRemovals(ctx)
	if err != nil {
		return err
	}
	server := controller
	var result error
	for _, operation := range operations {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		_, resumeErr := server.resumeApplianceRemove(ctx, operation, true)
		if resumeErr != nil {
			controller.logger.Warn("pending appliance removal", "installation_id", operation.InstallationID, "operation_id", operation.ID, "request_id", operation.RequestID, "error", resumeErr)
		}
		result = errors.Join(result, resumeErr)
	}
	return result
}

func (controller *Owner) RecoverInterruptedApplianceAdminOperations(ctx context.Context) error {
	operations, err := controller.store.ListRunningApplianceOperations(ctx)
	if err != nil {
		return err
	}
	server := controller
	var result error
	for _, operation := range operations {
		if operation.Kind == int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE) {
			if operation.DeadlineUnixMillis > time.Now().UnixMilli() {
				go server.runApplianceUpdate(operation.ID)
				continue
			}
			_, recoverErr := controller.store.TransitionApplianceOperation(ctx, operation.ID, store.ApplianceOperationTransition{ExpectedState: operation.State, ExpectedPhase: operation.Phase, State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED), Phase: operation.Phase, Stage: operation.Stage, ErrorReason: "UPDATE_DEADLINE_EXPIRED", ErrorMessage: "recover the same update with an extended deadline", Receipt: operation.Receipt})
			result = errors.Join(result, recoverErr)
			continue
		}

		if operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
			continue
		}
		// The restart stopped the old appliance instance. An admitted activation
		// or restart continues under its original operation; the Guest launch
		// identity is derived again from the installation and the new boot.
		if (operation.Kind == int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE) ||
			operation.Kind == int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART)) &&
			operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED) {
			failedStage, reason := "activation-failed", "APPLIANCE_START_FAILED"
			if operation.Kind == int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_RESTART) {
				failedStage, reason = "restart-failed", "APPLIANCE_RESTART_FAILED"
			}
			go func(operation store.ApplianceOperation) {
				// The activation waits behind the Host's resume of the VM.
				ctx, cancel := context.WithTimeout(context.Background(), hostlifecycle.RecoveryTimeout)
				defer cancel()
				if _, err := server.finishActivation(ctx, operation.ID, operation.InstallationID, failedStage, reason); err != nil {
					controller.logger.Warn("interrupted appliance activation", "installation_id", operation.InstallationID, "operation_id", operation.ID, "error", err)
				}
			}(operation)
			continue
		}
		_, recoverErr := server.failApplianceOperation(ctx, operation.ID,
			appliancev1.ApplianceOperationPhase(operation.Phase), "operation-interrupted",
			"HOST_RESTART_DURING_OPERATION", errors.New("host restarted during appliance operation"))
		result = errors.Join(result, recoverErr)
	}
	return result
}

func (server *Owner) resumeApplianceRemove(ctx context.Context, operation store.ApplianceOperation, background bool) (*appliancev1.ApplianceOperation, error) {
	deadline := time.Now().Add(removeExecutionTimeout)
	if acceptedDeadline := time.UnixMilli(operation.DeadlineUnixMillis); acceptedDeadline.After(time.Now()) && acceptedDeadline.Before(deadline) {
		deadline = acceptedDeadline
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var response *appliancev1.ApplianceOperation
	result := server.vmLifecycle.Run(ctx, hostlifecycle.Appliance, nil, func(context.Context) error {
		if background && server.vmLifecycle.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
			current, err := server.store.GetApplianceOperation(ctx, operation.ID)
			response = applianceOperationFromStore(current)
			return err
		}
		var err error
		response, err = server.resumeApplianceRemoveOwned(ctx, operation, background)
		return err
	})
	if result != nil {
		observation, cancel := context.WithTimeout(context.WithoutCancel(ctx), removeTerminalTimeout)
		defer cancel()
		pending, err := server.store.TransitionApplianceOperation(observation, operation.ID, store.ApplianceOperationTransition{
			ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			ExpectedPhase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY),
			State:         int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			Phase:         int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY), Stage: "pending-destroy",
			ErrorReason: "APPLIANCE_REMOVE_PENDING", ErrorMessage: result.Error(), Receipt: operation.Receipt,
		})
		if errors.Is(err, store.ErrOperationReceiptConflict) {
			pending, err = server.store.GetApplianceOperation(observation, operation.ID)
		}
		if err != nil {
			return nil, errors.Join(result, err)
		}
		response = applianceOperationFromStore(pending)
		if !background {
			return response, nil
		}
	}
	return response, result
}

func (server *Owner) resumeApplianceRemoveOwned(ctx context.Context, operation store.ApplianceOperation, background bool) (*appliancev1.ApplianceOperation, error) {
	unlock, err := server.applianceLocks.Lock(ctx, operation.InstallationID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	operation, err = server.store.GetApplianceOperation(ctx, operation.ID)
	if err != nil {
		return nil, err
	}
	if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) || operation.Phase != int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
		return applianceOperationFromStore(operation), nil
	}
	installation, err := server.store.GetApplianceInstallation(ctx, operation.InstallationID)
	if err != nil {
		return nil, err
	}
	if !background {
		if err := server.vmLifecycle.StartOwned(ctx); err != nil {
			return nil, err
		}
	}
	if err := server.revokeApplianceTrustAuthority(ctx, installation.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if installation.RegistrationID != "" {
		registration, err := server.store.GetRemoteRegistration(ctx, installation.RegistrationID)
		if err != nil {
			return nil, err
		}
		if registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL) {
			if err := server.RevokePrivateControllerDomain(ctx, installation.ID); err != nil {
				return nil, err
			}
		}
	}
	if err := server.discardApplianceDomainOwned(ctx, operation, installation); err != nil {
		return nil, err
	}
	if err := server.Stop(ctx, installation.ID); err != nil {
		return nil, err
	}
	client, err := server.guestConnection.Client()
	if err != nil {
		return nil, err
	}
	if err := server.removeApplianceData(ctx, client, installation); err != nil {
		return nil, err
	}
	committed, err := server.store.CommitApplianceRemove(ctx, store.ApplianceRemove{
		InstallationID: installation.ID, RegistrationID: installation.RegistrationID,
		OperationID:    operation.ID,
		CommitRevision: installation.Revision, Receipt: []byte("removed:" + operation.ID)})
	if err == nil {
	}
	return applianceOperationFromStore(committed), err
}

func (server *Owner) discardApplianceDomainOwned(ctx context.Context, parent store.ApplianceOperation, installation store.ApplianceInstallation) error {
	if installation.RegistrationID == "" {
		return nil
	}
	client, err := server.guestConnection.Client()
	if err != nil {
		return err
	}

	return server.discardApplianceDomainWithClient(ctx, client, parent, installation)
}

func (server *Owner) discardApplianceDomainWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, parent store.ApplianceOperation, installation store.ApplianceInstallation) error {
	return server.cleanup.DiscardRegistrationEnvironments(ctx, client, parent.ID, installation.RegistrationID)
}

const (
	removeExecutionTimeout = 2 * time.Minute
	removeTerminalTimeout  = 5 * time.Second
)
