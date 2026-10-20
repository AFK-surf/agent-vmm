//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"slices"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (server *applianceAdminServer) validateUpdate(ctx context.Context, request *appliancev1.UpdateApplianceRequest) (store.ApplianceInstallation, store.ApplianceArtifact, error) {
	installation, _, err := server.exactInstallation(ctx, applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()})
	if err != nil {
		return installation, store.ApplianceArtifact{}, err
	}
	artifact, err := server.controller.store.GetApplianceArtifact(ctx, request.GetArtifactId())
	if err != nil {
		return installation, artifact, err
	}
	if artifact.ProductID != installation.ProductID {
		return installation, artifact, status.Error(codes.FailedPrecondition, "update target must belong to the installed product")
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return installation, artifact, err
	}
	if err := validateApprovedGrant(manifest, request.GetApprovedGrant()); err != nil {
		return installation, artifact, err
	}
	if err := validateApplianceIsolationLimits(request.GetApprovedGrant(), controllerSystemIsolationCapacity(server.controller), controllerWorkloadIsolationCapacity(server.controller)); err != nil {
		return installation, artifact, err
	}
	if artifact.ID == installation.ArtifactID {
		stored, err := server.controller.store.GetApplianceGrant(ctx, installation.ID)
		if err != nil {
			return installation, artifact, err
		}
		previous := grantToProto(stored)
		frontends, err := server.controller.store.ListApplianceFrontends(ctx, installation.ID)
		if err != nil {
			return installation, artifact, err
		}
		previous.Frontends = frontendsToProto(frontends)
		if effectiveApplianceGrantsEqual(previous, request.GetApprovedGrant()) {
			return installation, artifact, status.Error(codes.FailedPrecondition, "update has no artifact or effective grant change")
		}
	}
	revoked, err := server.controller.store.HasApplianceRevocation(ctx, installation.ID)
	if err != nil {
		return installation, artifact, err
	}
	if revoked || installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		return installation, artifact, status.Error(codes.FailedPrecondition, "sealed or revoked installation cannot update")
	}
	pending, err := server.controller.store.HasPendingApplianceUpdate(ctx, installation.ID)
	if err != nil {
		return installation, artifact, err
	}
	running, err := server.controller.store.HasRunningApplianceOperation(ctx, installation.ID)
	if err != nil {
		return installation, artifact, err
	}
	if pending || running {
		return installation, artifact, status.Error(codes.FailedPrecondition, "installation has an unfinished lifecycle operation")
	}
	return installation, artifact, nil
}

func (server *applianceAdminServer) PlanUpdate(ctx context.Context, request *appliancev1.PlanUpdateRequest) (*appliancev1.AppliancePlanResponse, error) {
	installation, artifact, err := server.validateUpdate(ctx, &appliancev1.UpdateApplianceRequest{InstallationId: request.GetInstallationId(), RequestId: request.GetRequestId(), ExpectedRevision: request.GetExpectedRevision(), ArtifactId: request.GetArtifactId(), DeadlineUnixMillis: request.GetDeadlineUnixMillis(), ApprovedGrant: request.GetApprovedGrant()})
	if err != nil {
		return nil, err
	}
	return &appliancev1.AppliancePlanResponse{Plan: &appliancev1.AppliancePlan{InstallationId: installation.ID, ArtifactId: artifact.ID, Revision: installation.Revision, Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_UPDATE}}, nil
}

func updateProgress(operation store.ApplianceOperation) (*appliancev1.ApplianceUpdateProgress, error) {
	progress := new(appliancev1.ApplianceUpdateProgress)
	if err := proto.Unmarshal(operation.Receipt, progress); err != nil {
		return nil, err
	}
	if progress.GetRequest() == nil {
		return nil, errors.New("update receipt has no original request")
	}
	return progress, nil
}

func (server *applianceAdminServer) Update(ctx context.Context, request *appliancev1.UpdateApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	unlock := server.controller.lockIntegration("appliance-lifecycle:" + request.GetInstallationId())
	defer unlock()
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	existing, err := server.controller.store.ApplianceOperationByRequest(ctx, request.GetRequestId())
	if err == nil {
		progress, decodeErr := updateProgress(existing)
		if existing.Kind != int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE) || decodeErr != nil || !proto.Equal(progress.GetRequest(), request) {
			return nil, status.Error(codes.AlreadyExists, "request ID has a different update payload")
		}
		return applianceOperationResponse(existing), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err := applianceAdmissionDeadline(applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}); err != nil {
		return nil, err
	}
	installation, artifact, err := server.validateUpdate(ctx, request)
	if err != nil {
		return nil, err
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return nil, err
	}
	if err := server.controller.requireApplianceContent(ctx, manifest); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "CONTENT_NOT_STAGED", "appliance_artifact", artifact.ID, err.Error())
	}
	grant, err := server.controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	sourceGrant := grantToProto(grant)
	frontends, err := server.controller.store.ListApplianceFrontends(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	sourceGrant.Frontends = frontendsToProto(frontends)
	progress := &appliancev1.ApplianceUpdateProgress{Request: proto.Clone(request).(*appliancev1.UpdateApplianceRequest), SourceArtifactId: installation.ArtifactID, InstallationRevision: installation.Revision + 1, SourceGrant: sourceGrant}
	receipt, err := proto.Marshal(progress)
	if err != nil {
		return nil, err
	}
	operationID, err := randomID()
	if err != nil {
		return nil, err
	}
	operation, err := server.controller.store.BeginApplianceStateMutation(ctx, installation, request.GetExpectedRevision(), store.ApplianceOperation{ID: operationID, InstallationID: installation.ID, RequestID: request.GetRequestId(), Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING), Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING), ExpectedRevision: request.GetExpectedRevision(), DeadlineUnixMillis: request.GetDeadlineUnixMillis(), Stage: "preparing", Receipt: receipt})
	if err != nil {
		return nil, err
	}
	go server.runApplianceUpdate(operation.ID)
	return applianceOperationResponse(operation), nil
}

func (server *applianceAdminServer) RecoverUpdate(ctx context.Context, request *appliancev1.RecoverApplianceUpdateRequest) (*appliancev1.ApplianceOperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	unlock := server.controller.lockIntegration("appliance-lifecycle:" + request.GetInstallationId())
	defer unlock()
	operation, err := server.controller.store.GetApplianceOperation(ctx, request.GetOperationId())
	if err != nil {
		return nil, err
	}
	if operation.InstallationID != request.GetInstallationId() || operation.Kind != int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE) {
		return nil, status.Error(codes.InvalidArgument, "operation is not this installation's update")
	}
	progress, err := updateProgress(operation)
	if err != nil {
		return nil, err
	}
	if proto.Equal(progress.GetRecovery(), request) {
		return applianceOperationResponse(operation), nil
	}
	if progress.GetRecovery() != nil && progress.GetRecovery().GetExpectedUpdatedUnixNanos() == request.GetExpectedUpdatedUnixNanos() {
		return nil, status.Error(codes.AlreadyExists, "recovery observation has a different deadline")
	}
	if request.GetDeadlineUnixMillis() <= time.Now().UnixMilli() || request.GetDeadlineUnixMillis() <= operation.DeadlineUnixMillis {
		return nil, status.Error(codes.InvalidArgument, "recovery requires an extended finite deadline")
	}
	if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED) {
		return nil, status.Error(codes.FailedPrecondition, "only a failed unfinished update can recover")
	}
	installation, loadErr := server.controller.store.GetApplianceInstallation(ctx, operation.InstallationID)
	if loadErr != nil {
		return nil, loadErr
	}
	revoked, loadErr := server.controller.store.HasApplianceRevocation(ctx, installation.ID)
	if loadErr != nil {
		return nil, loadErr
	}
	if revoked || installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		return nil, status.Error(codes.FailedPrecondition, "sealed or revoked update cannot reactivate; explicitly stop, revoke, or remove the installation")
	}
	progress.Recovery = proto.Clone(request).(*appliancev1.RecoverApplianceUpdateRequest)

	operation.Receipt, err = proto.Marshal(progress)
	if err != nil {
		return nil, err
	}
	operation.DeadlineUnixMillis = request.GetDeadlineUnixMillis()
	operation, err = server.controller.store.ResumeApplianceUpdate(ctx, operation, time.Unix(0, request.GetExpectedUpdatedUnixNanos()))
	if err != nil {
		return nil, err
	}
	go server.runApplianceUpdate(operation.ID)
	return applianceOperationResponse(operation), nil
}

func (server *applianceAdminServer) runApplianceUpdate(id string) {
	unlock := server.controller.lockIntegration("appliance-update:" + id)
	defer unlock()
	operation, err := server.controller.store.GetApplianceOperation(context.Background(), id)
	if err != nil || operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
		return
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.UnixMilli(operation.DeadlineUnixMillis))
	defer cancel()
	progress, err := updateProgress(operation)
	if err == nil && !server.controller.vmLifecycle.admitsRemoteWork() {
		_, err = server.controller.vmLifecycle.Start(ctx)
	}
	for err == nil {
		if err = ctx.Err(); err != nil {
			break
		}
		unlockPhase := func() {}
		if operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_STOPPING) || operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_SWITCHING) {
			unlockPhase = server.controller.lockIntegration("appliance-lifecycle:" + operation.InstallationID)
		}
		current, loadErr := server.controller.store.GetApplianceOperation(ctx, id)
		if loadErr != nil || current.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
			unlockPhase()
			return
		}
		operation = current
		installation, loadErr := server.controller.store.GetApplianceInstallation(ctx, operation.InstallationID)
		if loadErr != nil {
			err = loadErr
			unlockPhase()
			break
		}
		switch appliancev1.ApplianceOperationPhase(operation.Phase) {
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING:
			target, loadErr := server.controller.store.GetApplianceArtifact(ctx, progress.GetRequest().GetArtifactId())
			if loadErr != nil {
				err = loadErr
				break
			}
			client, connection, loadErr := server.controller.guestClient(ctx)
			if loadErr != nil {
				err = loadErr
				break
			}
			operation, err = server.prepareApplianceUpdate(ctx, client, operation, installation, target, progress)
			connection.Close()
			if err == nil {
				continue
			}
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_DRAINING:
			err = server.drainApplianceUpdate(ctx, installation)
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_STOPPING:
			err = server.controller.stopAppliance(ctx, installation.ID)
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_SWITCHING:
			previousReceipt := operation.Receipt
			progress.InstallationRevision++
			operation.Receipt, err = proto.Marshal(progress)
			if err != nil {
				break
			}
			oldGrant, loadErr := server.controller.store.GetApplianceGrant(ctx, installation.ID)
			if loadErr != nil {
				err = loadErr
				break
			}
			grant := grantFromProto(installation.ID, oldGrant.Revision+1, progress.GetRequest().GetApprovedGrant())
			switched, switchErr := server.controller.store.CommitApplianceUpdateSwitch(ctx, operation, progress.GetSourceArtifactId(), progress.GetInstallationRevision()-1, progress.GetRequest().GetArtifactId(), grant, frontendsFromProto(progress.GetRequest().GetApprovedGrant().GetFrontends()))
			err = switchErr
			if err == nil {
				operation = switched
				unlockPhase()
				continue
			}
			progress.InstallationRevision--
			operation.Receipt = previousReceipt
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_ACTIVATING:
			err = server.controller.activateAppliance(ctx, installation.ID)
			if err == nil {
				_, err = server.completeApplianceOperation(ctx, id, appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_ACTIVATING, "updated", operation.Receipt)
				unlockPhase()
				return
			}
		default:
			err = errors.New("unknown durable update phase")
		}
		if err != nil {
			unlockPhase()
			break
		}
		next := operation.Phase + 1
		advanced, transitionErr := server.controller.store.TransitionApplianceOperation(ctx, id, store.ApplianceOperationTransition{ExpectedState: operation.State, ExpectedPhase: operation.Phase, State: operation.State, Phase: next, Stage: appliancev1.ApplianceOperationPhase(next).String(), Receipt: operation.Receipt})
		err = transitionErr
		if err == nil {
			operation = advanced
		}
		unlockPhase()
	}
	if err != nil {
		_, _ = server.controller.store.TransitionApplianceOperation(context.Background(), id, store.ApplianceOperationTransition{ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING), ExpectedPhase: operation.Phase, State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED), Phase: operation.Phase, Stage: operation.Stage, ErrorReason: "UPDATE_PHASE_FAILED", ErrorMessage: err.Error(), Receipt: operation.Receipt})
	}
}

func (server *applianceAdminServer) drainApplianceUpdate(ctx context.Context, installation store.ApplianceInstallation) error {
	unlock := server.controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	server.controller.remoteAdmissionMu.Lock()
	registration, err := server.controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err == nil && registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) {
		_, err = server.controller.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING))
	}
	server.controller.remoteAdmissionMu.Unlock()
	if err != nil {
		unlock()
		return err
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) && registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		unlock()
		return errors.New("update registration is not drainable")
	}
	if err := server.controller.invalidateRemoteController(ctx, registration.ID); err != nil {
		unlock()
		return err
	}
	unlock()
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	for {
		server.controller.mu.Lock()
		settled := server.controller.workloadRunsRecovered
		for _, run := range server.controller.workloadRuns {
			if run.registrationID == registration.ID && (run.starting || run.stopping || run.inFlight || run.quiescing || !run.uncertainStartUntil.IsZero() || len(run.executions) > 0) {
				settled = false
				break
			}
		}
		server.controller.mu.Unlock()
		if settled {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func effectiveApplianceGrantsEqual(left, right *appliancev1.ApprovedGrant) bool {
	normalize := func(value *appliancev1.ApprovedGrant) *appliancev1.ApprovedGrant {
		if value == nil {
			return nil
		}
		clone := proto.Clone(value).(*appliancev1.ApprovedGrant)
		clone.Revision = 0
		slices.SortFunc(clone.HostTcpEndpoints, func(a, b *appliancev1.HostTcpEndpoint) int {
			if a.GetIp() < b.GetIp() {
				return -1
			}
			if a.GetIp() > b.GetIp() {
				return 1
			}
			if a.GetPort() < b.GetPort() {
				return -1
			}
			if a.GetPort() > b.GetPort() {
				return 1
			}
			return 0
		})
		slices.SortFunc(clone.Frontends, func(a, b *appliancev1.ControllerFrontend) int {
			if a.GetName() < b.GetName() {
				return -1
			}
			if a.GetName() > b.GetName() {
				return 1
			}
			return 0
		})
		return clone
	}
	return proto.Equal(normalize(left), normalize(right))
}

func appliancePreparationCapacityWait(err error) bool {
	if status.Code(err) != codes.ResourceExhausted {
		return false
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() == "RESOURCE_CAPACITY_EXHAUSTED" {
			dimension := info.GetMetadata()["resource_dimension"]
			stage := info.GetMetadata()["stage"]
			return (dimension == "import_slot" && stage == "import_slot") || (dimension == "storage_headroom" && stage == "import_admission")
		}
	}
	return false
}

func (server *applianceAdminServer) prepareApplianceUpdate(ctx context.Context, client vmmdv1.VMMDServiceClient, operation store.ApplianceOperation, installation store.ApplianceInstallation, target store.ApplianceArtifact, progress *appliancev1.ApplianceUpdateProgress) (store.ApplianceOperation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return operation, err
		}
		current, err := server.controller.store.GetApplianceOperation(ctx, operation.ID)
		if err != nil {
			return operation, err
		}
		if current.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) || current.Phase != int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING) {
			return operation, store.ErrOperationReceiptConflict
		}
		operation = current
		err = server.controller.importApplianceArchive(ctx, client, installation, target, grantFromProto(installation.ID, 1, progress.GetRequest().GetApprovedGrant()))
		if err == nil {
			progress.InstallationRevision++
			operation.Receipt, err = proto.Marshal(progress)
			if err != nil {
				return operation, err
			}
			prepared, commitErr := server.controller.store.CommitApplianceUpdatePreparation(ctx, operation, progress.GetSourceArtifactId(), progress.GetInstallationRevision()-1)
			if commitErr != nil {
				progress.InstallationRevision--
				operation = current
				return operation, commitErr
			}
			return prepared, nil
		}
		if !appliancePreparationCapacityWait(err) {
			return operation, err
		}
		log.Printf("appliance update waiting: operation=%s installation=%s phase=PREPARING deadline_ms=%d error=%v details=%v", operation.ID, installation.ID, operation.DeadlineUnixMillis, err, status.Convert(err).Details())
		waiting, transitionErr := server.controller.store.TransitionApplianceOperation(ctx, operation.ID, store.ApplianceOperationTransition{ExpectedState: operation.State, ExpectedPhase: operation.Phase, State: operation.State, Phase: operation.Phase, Stage: "waiting_for_resources", ErrorReason: "RESOURCE_CAPACITY_EXHAUSTED", ErrorMessage: err.Error(), Receipt: operation.Receipt})
		if transitionErr != nil {
			return operation, transitionErr
		}
		operation = waiting
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return operation, ctx.Err()
		case <-timer.C:
		}
	}
}
