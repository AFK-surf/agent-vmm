//go:build darwin || linux

package appliance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"slices"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (server *Owner) validateUpdate(ctx context.Context, request *appliancev1.UpdateApplianceRequest) (store.ApplianceInstallation, store.ApplianceArtifact, error) {
	installation, err := server.exactInstallation(ctx, applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()})
	if err != nil {
		return installation, store.ApplianceArtifact{}, err
	}
	artifact, err := server.store.GetApplianceArtifact(ctx, request.GetArtifactId())
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
	if err := validateApplianceIsolationLimits(request.GetApprovedGrant(), controllerSystemIsolationCapacity(server), controllerWorkloadIsolationCapacity(server)); err != nil {
		return installation, artifact, err
	}
	if artifact.ID == installation.ArtifactID && request.OpaqueBootstrapSeed == nil {
		stored, err := server.store.GetApplianceGrant(ctx, installation.ID)
		if err != nil {
			return installation, artifact, err
		}
		previous := grantToProto(stored)
		frontends, err := server.store.ListApplianceFrontends(ctx, installation.ID)
		if err != nil {
			return installation, artifact, err
		}
		previous.Frontends = frontendsToProto(frontends)
		if effectiveApplianceGrantsEqual(previous, request.GetApprovedGrant()) {
			return installation, artifact, status.Error(codes.FailedPrecondition, "update has no artifact or effective grant change")
		}
	}
	revoked, err := server.store.HasApplianceRevocation(ctx, installation.ID)
	if err != nil {
		return installation, artifact, err
	}
	if revoked || installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		return installation, artifact, status.Error(codes.FailedPrecondition, "sealed or revoked installation cannot update")
	}
	pending, err := server.store.HasPendingApplianceUpdate(ctx, installation.ID)
	if err != nil {
		return installation, artifact, err
	}
	running, err := server.store.HasRunningApplianceOperation(ctx, installation.ID)
	if err != nil {
		return installation, artifact, err
	}
	if pending || running {
		return installation, artifact, status.Error(codes.FailedPrecondition, "installation has an unfinished lifecycle operation")
	}
	return installation, artifact, nil
}

func (server *Owner) PlanUpdate(ctx context.Context, request *appliancev1.PlanUpdateRequest) (*appliancev1.PlanUpdateResponse, error) {
	installation, artifact, err := server.validateUpdate(ctx, &appliancev1.UpdateApplianceRequest{InstallationId: request.GetInstallationId(), RequestId: request.GetRequestId(), ExpectedRevision: request.GetExpectedRevision(), ArtifactId: request.GetArtifactId(), DeadlineUnixMillis: request.GetDeadlineUnixMillis(), ApprovedGrant: request.GetApprovedGrant()})
	if err != nil {
		return nil, err
	}
	return &appliancev1.PlanUpdateResponse{Plan: &appliancev1.AppliancePlan{InstallationId: installation.ID, ArtifactId: artifact.ID, Revision: installation.Revision, Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_UPDATE}}, nil
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

func (server *Owner) UpdateAppliance(ctx context.Context, request *appliancev1.UpdateApplianceRequest) (*appliancev1.UpdateApplianceResponse, error) {
	defer clear(request.OpaqueBootstrapSeed)
	recordedRequest := proto.Clone(request).(*appliancev1.UpdateApplianceRequest)
	recordedRequest.OpaqueBootstrapSeed = nil
	var seedDigest []byte
	if request.OpaqueBootstrapSeed != nil {
		digest := sha256.Sum256(request.OpaqueBootstrapSeed)
		seedDigest = digest[:]
	}
	unlock, lockErr := server.applianceLocks.Lock(ctx, request.GetInstallationId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := server.applianceContentStage.RLock(ctx); err != nil {
		return nil, err
	}
	defer server.applianceContentStage.RUnlock()
	releaseRemoteAdmission := server.remote.LockAdmission()

	defer releaseRemoteAdmission()
	existing, err := server.store.ApplianceOperationByRequest(ctx, request.GetRequestId())
	if err == nil {
		progress, decodeErr := updateProgress(existing)
		if existing.Kind != int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE) || decodeErr != nil || !proto.Equal(progress.GetRequest(), recordedRequest) || !bytes.Equal(progress.GetBootstrapSeedSha256(), seedDigest) {
			return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID has a different update payload", hostv1.WithResource("appliance_installation", request.GetInstallationId()))
		}
		return &appliancev1.UpdateApplianceResponse{Operation: applianceOperationFromStore(existing)}, nil
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
	if err := server.requireApplianceContent(ctx, manifest); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	grant, err := server.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	sourceGrant := grantToProto(grant)
	frontends, err := server.store.ListApplianceFrontends(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	sourceGrant.Frontends = frontendsToProto(frontends)
	operationID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	progress := &appliancev1.ApplianceUpdateProgress{Request: recordedRequest, SourceArtifactId: installation.ArtifactID, InstallationRevision: installation.Revision + 1, SourceGrant: sourceGrant, BootstrapSeedSha256: seedDigest}
	if request.OpaqueBootstrapSeed != nil {
		progress.BootstrapId = operationID
		if err := server.applianceSecrets.StoreBootstrap(ctx, installation.ID, progress.BootstrapId, request.OpaqueBootstrapSeed); err != nil {
			return nil, err
		}
	}
	keepBootstrap := false
	defer func() {
		if !keepBootstrap && progress.BootstrapId != "" {
			_ = server.applianceSecrets.DeleteBootstrap(context.Background(), installation.ID, progress.BootstrapId)
		}
	}()
	receipt, err := proto.Marshal(progress)
	if err != nil {
		return nil, err
	}
	operation, err := server.store.BeginApplianceStateMutation(ctx, installation, request.GetExpectedRevision(), store.ApplianceOperation{ID: operationID, InstallationID: installation.ID, RequestID: request.GetRequestId(), Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING), Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING), ExpectedRevision: request.GetExpectedRevision(), DeadlineUnixMillis: request.GetDeadlineUnixMillis(), Stage: "preparing", Receipt: receipt})
	if err != nil {
		return nil, err
	}
	keepBootstrap = true
	go server.runApplianceUpdate(operation.ID)
	return &appliancev1.UpdateApplianceResponse{Operation: applianceOperationFromStore(operation)}, nil
}

func (server *Owner) RecoverApplianceUpdate(ctx context.Context, request *appliancev1.RecoverApplianceUpdateRequest) (*appliancev1.RecoverApplianceUpdateResponse, error) {
	unlock, lockErr := server.applianceLocks.Lock(ctx, request.GetInstallationId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	operation, err := server.store.GetApplianceOperation(ctx, request.GetOperationId())
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
		return &appliancev1.RecoverApplianceUpdateResponse{Operation: applianceOperationFromStore(operation)}, nil
	}
	if progress.GetRecovery() != nil && progress.GetRecovery().GetExpectedUpdatedUnixNanos() == request.GetExpectedUpdatedUnixNanos() {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "recovery observation has a different deadline", hostv1.WithResource("appliance_installation", request.GetInstallationId()))
	}
	if request.GetDeadlineUnixMillis() <= time.Now().UnixMilli() || request.GetDeadlineUnixMillis() <= operation.DeadlineUnixMillis {
		return nil, status.Error(codes.InvalidArgument, "recovery requires an extended finite deadline")
	}
	if operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED) {
		return nil, status.Error(codes.FailedPrecondition, "only a failed unfinished update can recover")
	}
	installation, loadErr := server.store.GetApplianceInstallation(ctx, operation.InstallationID)
	if loadErr != nil {
		return nil, loadErr
	}
	revoked, loadErr := server.store.HasApplianceRevocation(ctx, installation.ID)
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
	operation, err = server.store.ResumeApplianceUpdate(ctx, operation, time.Unix(0, request.GetExpectedUpdatedUnixNanos()))
	if err != nil {
		return nil, err
	}
	go server.runApplianceUpdate(operation.ID)
	return &appliancev1.RecoverApplianceUpdateResponse{Operation: applianceOperationFromStore(operation)}, nil
}

var updatePhaseNext = map[appliancev1.ApplianceOperationPhase]appliancev1.ApplianceOperationPhase{
	appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING: appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_DRAINING,
	appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_DRAINING:  appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_STOPPING,
	appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_STOPPING:  appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_SWITCHING,
	appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_SWITCHING: appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_ACTIVATING,
}

func (server *Owner) runApplianceUpdate(id string) {
	unlock, lockErr := server.applianceUpdates.Lock(context.Background(), id)
	if lockErr != nil {
		return
	}

	defer unlock()
	operation, err := server.store.GetApplianceOperation(context.Background(), id)
	if err != nil || operation.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
		return
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.UnixMilli(operation.DeadlineUnixMillis))
	defer cancel()
	progress, err := updateProgress(operation)
	if err == nil && !server.vmLifecycle.AdmitsRemoteWork() {
		err = server.vmLifecycle.Run(ctx, hostlifecycle.Start, nil, server.vmLifecycle.StartOwned)
	}
	for err == nil {
		if err = ctx.Err(); err != nil {
			break
		}
		unlockPhase := func() {}
		if operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_STOPPING) || operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_SWITCHING) {
			unlockPhase, err = server.applianceLocks.Lock(ctx, operation.InstallationID)
			if err != nil {
				break
			}
		}
		current, loadErr := server.store.GetApplianceOperation(ctx, id)
		if loadErr != nil || current.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
			unlockPhase()
			return
		}
		operation = current
		installation, loadErr := server.store.GetApplianceInstallation(ctx, operation.InstallationID)
		if loadErr != nil {
			err = loadErr
			unlockPhase()
			break
		}
		switch appliancev1.ApplianceOperationPhase(operation.Phase) {
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING:
			target, loadErr := server.store.GetApplianceArtifact(ctx, progress.GetRequest().GetArtifactId())
			if loadErr != nil {
				err = loadErr
				break
			}
			client, loadErr := server.guestConnection.Client()
			if loadErr != nil {
				err = loadErr
				break
			}
			operation, err = server.prepareApplianceUpdate(ctx, client, operation, installation, target, progress)
			if err == nil {
				continue
			}
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_DRAINING:
			err = server.drainApplianceUpdate(ctx, installation)
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_STOPPING:
			err = server.Stop(ctx, installation.ID)
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_SWITCHING:
			previousReceipt := operation.Receipt
			progress.InstallationRevision++
			operation.Receipt, err = proto.Marshal(progress)
			if err != nil {
				break
			}
			oldGrant, loadErr := server.store.GetApplianceGrant(ctx, installation.ID)
			if loadErr != nil {
				err = loadErr
				break
			}
			grant := grantFromProto(installation.ID, oldGrant.Revision+1, progress.GetRequest().GetApprovedGrant())
			switched, switchErr := server.store.CommitApplianceUpdateSwitch(ctx, operation, progress.GetSourceArtifactId(), progress.GetInstallationRevision()-1, progress.GetRequest().GetArtifactId(), progress.GetBootstrapId(), grant, frontendsFromProto(progress.GetRequest().GetApprovedGrant().GetFrontends()))
			err = switchErr
			if err == nil {
				operation = switched
				unlockPhase()
				continue
			}
			progress.InstallationRevision--
			operation.Receipt = previousReceipt
		case appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_ACTIVATING:
			err = server.activateAppliance(ctx, installation.ID)

		default:
			err = errors.New("unknown durable update phase")
		}
		if err != nil {
			unlockPhase()
			break
		}
		if appliancev1.ApplianceOperationPhase(operation.Phase) == appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_ACTIVATING {
			_, err = server.completeApplianceOperation(ctx, id, appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_ACTIVATING, "updated", operation.Receipt)
			unlockPhase()
			if err == nil {
				return
			}
			break
		}
		next, ok := updatePhaseNext[appliancev1.ApplianceOperationPhase(operation.Phase)]
		if !ok {
			err = errors.New("unknown durable update phase")
			unlockPhase()
			break
		}
		advanced, transitionErr := server.store.TransitionApplianceOperation(ctx, id, store.ApplianceOperationTransition{ExpectedState: operation.State, ExpectedPhase: operation.Phase, State: operation.State, Phase: int32(next), Stage: next.String(), Receipt: operation.Receipt})
		err = transitionErr
		if err == nil {
			operation = advanced
		}
		unlockPhase()
	}
	if err != nil {
		_, _ = server.store.TransitionApplianceOperation(context.Background(), id, store.ApplianceOperationTransition{ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING), ExpectedPhase: operation.Phase, State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED), Phase: operation.Phase, Stage: operation.Stage, ErrorReason: "UPDATE_PHASE_FAILED", ErrorMessage: err.Error(), Receipt: operation.Receipt})
	}
}

func (server *Owner) drainApplianceUpdate(ctx context.Context, installation store.ApplianceInstallation) error {
	unlock, lockErr := server.applianceLocks.Lock(ctx, installation.ID)
	if lockErr != nil {
		return lockErr
	}
	releaseRemoteAdmission := server.remote.LockAdmission()

	registration, err := server.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err == nil && registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) {
		_, err = server.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING))
	}
	releaseRemoteAdmission()
	if err != nil {
		unlock()
		return err
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) && registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING) {
		unlock()
		return errors.New("update registration is not drainable")
	}
	if err := server.sessions.RevokeRegistration(ctx, registration.ID); err != nil {
		unlock()
		return err
	}
	unlock()
	return server.workloads.WaitRegistrationQuiesced(ctx, registration.ID)
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
	reason, ok := hostv1.ErrorReasonOf(err)
	if status.Code(err) != codes.ResourceExhausted || !ok || reason != hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED {
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

func (server *Owner) prepareApplianceUpdate(ctx context.Context, client vmmdv1.VMMDServiceClient, operation store.ApplianceOperation, installation store.ApplianceInstallation, target store.ApplianceArtifact, progress *appliancev1.ApplianceUpdateProgress) (store.ApplianceOperation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return operation, err
		}
		current, err := server.store.GetApplianceOperation(ctx, operation.ID)
		if err != nil {
			return operation, err
		}
		if current.State != int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) || current.Phase != int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PREPARING) {
			return operation, store.ErrOperationReceiptConflict
		}
		operation = current
		err = server.importApplianceArchive(ctx, client, installation, target, grantFromProto(installation.ID, 1, progress.GetRequest().GetApprovedGrant()))
		if err == nil {
			progress.InstallationRevision++
			operation.Receipt, err = proto.Marshal(progress)
			if err != nil {
				return operation, err
			}
			prepared, commitErr := server.store.CommitApplianceUpdatePreparation(ctx, operation, progress.GetSourceArtifactId(), progress.GetInstallationRevision()-1)
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
		server.logger.Info("appliance update waiting", "operation_id", operation.ID, "installation_id", installation.ID, "deadline_unix_millis", operation.DeadlineUnixMillis, "error", err)
		waiting, transitionErr := server.store.TransitionApplianceOperation(ctx, operation.ID, store.ApplianceOperationTransition{ExpectedState: operation.State, ExpectedPhase: operation.Phase, State: operation.State, Phase: operation.Phase, Stage: "waiting_for_resources", ErrorReason: "RESOURCE_CAPACITY_EXHAUSTED", ErrorMessage: err.Error(), Receipt: operation.Receipt})
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
