//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

func (server *applianceAdminServer) PlanRemove(ctx context.Context, request *appliancev1.PlanRemoveRequest) (*appliancev1.AppliancePlanResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	plan := &appliancev1.AppliancePlan{InstallationId: installation.ID,
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_REMOVE, Revision: installation.Revision,
		ArtifactId: installation.ArtifactID, Impacts: []*appliancev1.ApplianceImpact{
			{ResourceType: "installation", ResourceId: installation.ID, Effect: "delete", Irreversible: true},
			{ResourceType: "data_volume", ResourceId: installation.DataVolumeID, Effect: "delete", Irreversible: true}}}
	if installation.RegistrationID != "" {
		environments, err := server.controller.store.ListRemoteEnvironments(ctx, installation.RegistrationID)
		if err != nil {
			return nil, err
		}
		plan.Impacts = append(plan.Impacts, &appliancev1.ApplianceImpact{ResourceType: "private_domain", ResourceId: installation.RegistrationID, Effect: "delete all domain data at execution", Irreversible: true})
		for _, environment := range environments {
			plan.Impacts = append(plan.Impacts, &appliancev1.ApplianceImpact{ResourceType: "allocation",
				ResourceId: environment.AllocationID, Effect: "delete", Irreversible: true})
		}
	}
	return &appliancev1.AppliancePlanResponse{Plan: plan}, nil
}

func (server *applianceAdminServer) ExecuteRemove(ctx context.Context, request *appliancev1.ExecuteRemoveRequest) (*appliancev1.ApplianceOperationResponse, error) {
	operation, err := server.acceptApplianceRemove(ctx, request)
	if err != nil {
		return nil, err
	}
	if operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
		return server.resumeApplianceRemove(ctx, operation, false)
	}
	return applianceOperationResponse(operation), nil
}

func (server *applianceAdminServer) acceptApplianceRemove(ctx context.Context, request *appliancev1.ExecuteRemoveRequest) (store.ApplianceOperation, error) {
	lock := server.controller.integrationLock("appliance-lifecycle:" + request.GetInstallationId())
	for !lock.TryLock() {
		if err := waitForControllerLock(ctx); err != nil {
			return store.ApplianceOperation{}, err
		}
	}
	defer lock.Unlock()
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE); err != nil {
		return store.ApplianceOperation{}, err
	} else if found {
		return existing, nil
	}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return store.ApplianceOperation{}, err
	}
	if !containsApplianceResource(request.GetConfirmedResourceIds(), installation.DataVolumeID) {
		return store.ApplianceOperation{}, resourceError(codes.FailedPrecondition, "REMOVE_CONFIRMATION_REQUIRED", "data_volume", installation.DataVolumeID, "exact remove plan and data volume confirmation are required")
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
	if err != nil {
		return store.ApplianceOperation{}, err
	}
	return server.controller.store.GetApplianceOperation(ctx, operationID)
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
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		_, resumeErr := server.resumeApplianceRemove(ctx, operation, true)
		if resumeErr != nil {
			log.Printf("pending appliance removal: installation=%s operation=%s request=%s error=%v", operation.InstallationID, operation.ID, operation.RequestID, resumeErr)
		}
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
				ctx, cancel := context.WithTimeout(context.Background(), unexpectedVMRecoveryTimeout)
				defer cancel()
				if _, err := server.finishActivation(ctx, operation.ID, operation.InstallationID, failedStage, reason); err != nil {
					log.Printf("interrupted appliance activation: installation=%s operation=%s error=%v", operation.InstallationID, operation.ID, err)
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

func (server *applianceAdminServer) resumeApplianceRemove(ctx context.Context, operation store.ApplianceOperation, background bool) (*appliancev1.ApplianceOperationResponse, error) {
	deadline := time.Now().Add(vmOperatorExecutionTimeout)
	if acceptedDeadline := time.UnixMilli(operation.DeadlineUnixMillis); acceptedDeadline.After(time.Now()) && acceptedDeadline.Before(deadline) {
		deadline = acceptedDeadline
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var response *appliancev1.ApplianceOperationResponse
	result := server.controller.vmLifecycle.execute(ctx, vmLifecycleAppliance, func() vmLifecycleResult {
		if background && server.controller.vmLifecycle.snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
			current, err := server.controller.store.GetApplianceOperation(ctx, operation.ID)
			response = applianceOperationResponse(current)
			return vmLifecycleResult{err: err}
		}
		var err error
		response, err = server.resumeApplianceRemoveOwned(ctx, operation, background)
		return vmLifecycleResult{err: err}
	})
	if result.err != nil {
		observation, cancel := context.WithTimeout(context.WithoutCancel(ctx), vmOperatorTerminalTimeout)
		defer cancel()
		pending, err := server.controller.store.TransitionApplianceOperation(observation, operation.ID, store.ApplianceOperationTransition{
			ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			ExpectedPhase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY),
			State:         int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
			Phase:         int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY), Stage: "pending-destroy",
			ErrorReason: "APPLIANCE_REMOVE_PENDING", ErrorMessage: result.err.Error(), Receipt: operation.Receipt,
		})
		if errors.Is(err, store.ErrOperationReceiptConflict) {
			pending, err = server.controller.store.GetApplianceOperation(observation, operation.ID)
		}
		if err != nil {
			return nil, errors.Join(result.err, err)
		}
		response = applianceOperationResponse(pending)
		if !background {
			return response, nil
		}
	}
	return response, result.err
}

func (server *applianceAdminServer) resumeApplianceRemoveOwned(ctx context.Context, operation store.ApplianceOperation, background bool) (*appliancev1.ApplianceOperationResponse, error) {
	lock := server.controller.integrationLock("appliance-lifecycle:" + operation.InstallationID)
	for !lock.TryLock() {
		if err := waitForControllerLock(ctx); err != nil {
			return nil, err
		}
	}
	defer lock.Unlock()
	operation, err := server.controller.store.GetApplianceOperation(ctx, operation.ID)
	if err != nil {
		return nil, err
	}
	if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) || operation.Phase != int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
		return applianceOperationResponse(operation), nil
	}
	installation, err := server.controller.store.GetApplianceInstallation(ctx, operation.InstallationID)
	if err != nil {
		return nil, err
	}
	if !background {
		if err := server.controller.vmLifecycle.startOwned(ctx); err != nil {
			return nil, err
		}
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
	if err := server.discardApplianceDomainOwned(ctx, operation, installation); err != nil {
		return nil, err
	}
	if err := server.controller.stopAppliance(ctx, installation.ID); err != nil {
		return nil, err
	}
	_, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	if err := server.controller.removeApplianceData(ctx, connection, installation); err != nil {
		return nil, err
	}
	committed, err := server.controller.store.CommitApplianceRemove(ctx, store.ApplianceRemove{
		InstallationID: installation.ID, RegistrationID: installation.RegistrationID,
		OperationID:    operation.ID,
		CommitRevision: installation.Revision, Receipt: []byte("removed:" + operation.ID)})
	return applianceOperationResponse(committed), err
}

func (server *applianceAdminServer) discardApplianceDomainOwned(ctx context.Context, parent store.ApplianceOperation, installation store.ApplianceInstallation) error {
	if installation.RegistrationID == "" {
		return nil
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	return server.discardApplianceDomainWithClient(ctx, client, parent, installation)
}

func (server *applianceAdminServer) discardApplianceDomainWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, parent store.ApplianceOperation, installation store.ApplianceInstallation) error {
	release, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return err
	}
	defer release()
	environments, err := server.controller.store.ListRemoteEnvironments(ctx, installation.RegistrationID)
	if err != nil {
		return err
	}
	operator := &operatorServer{controller: server.controller}
	for _, environment := range environments {
		plan, err := operator.planCleanupWithClient(ctx, client, environment.ID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD, true)
		if err != nil {
			return err
		}
		suffix, err := randomID()
		if err != nil {
			return err
		}
		requestID := "remove-" + parent.ID + "-" + environment.ID + "-" + suffix
		child, err := operator.executeCleanupAdmissionLockedWithClient(ctx, client, requestID, plan, nil, "", true, true, nil)
		if err != nil {
			return err
		}
		if child.State != int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) {
			return resourceErrorWithMetadata(codes.Unavailable, "APPLIANCE_DOMAIN_CLEANUP_PENDING", "environment", environment.ID,
				child.ErrorMessage, map[string]string{"operation_id": child.ID, "request_id": child.RequestID})
		}
	}
	return nil
}
