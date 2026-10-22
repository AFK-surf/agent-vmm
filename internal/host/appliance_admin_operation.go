//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type applianceAdminServer struct {
	appliancev1.UnimplementedApplianceAdminServiceServer
	controller *Controller
}

type applianceMutation struct {
	installationID     string
	requestID          string
	expectedRevision   uint64
	deadlineUnixMillis int64
}

func validateApplianceMutation(value applianceMutation) error {
	if value.installationID == "" || value.requestID == "" {
		return invalidField("installation_id/request_id", "appliance mutation identity is required")
	}
	if value.deadlineUnixMillis <= time.Now().UnixMilli() {
		return status.Error(codes.DeadlineExceeded, "appliance mutation deadline expired")
	}
	return nil
}

func (server *applianceAdminServer) exactInstallation(ctx context.Context, mutation applianceMutation) (store.ApplianceInstallation, store.ApplianceArtifact, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return store.ApplianceInstallation{}, store.ApplianceArtifact{}, err
	}
	if err := validateApplianceMutation(mutation); err != nil {
		return store.ApplianceInstallation{}, store.ApplianceArtifact{}, err
	}
	installation, err := server.controller.store.GetApplianceInstallation(ctx, mutation.installationID)
	if err != nil {
		return store.ApplianceInstallation{}, store.ApplianceArtifact{}, err
	}
	if installation.Revision != mutation.expectedRevision {
		return store.ApplianceInstallation{}, store.ApplianceArtifact{}, resourceError(codes.Aborted,
			"INSTALLATION_REVISION_CONFLICT", "appliance_installation", installation.ID, "installation revision changed")
	}
	artifact, err := server.controller.store.GetApplianceArtifact(ctx, installation.ArtifactID)
	if err != nil {
		return store.ApplianceInstallation{}, store.ApplianceArtifact{}, resourceError(codes.FailedPrecondition,
			"ARTIFACT_FENCE_FAILED", "appliance_artifact", installation.ArtifactID, "fixed artifact is unavailable")
	}
	return installation, artifact, nil
}

func applianceAdmissionDeadline(mutation applianceMutation) error {
	if mutation.deadlineUnixMillis <= time.Now().UnixMilli() {
		return resourceErrorWithMetadata(codes.DeadlineExceeded, "APPLIANCE_OPERATION_NOT_ADMITTED", "appliance_installation", mutation.installationID, "appliance intent expired before admission; review and approve a new plan", map[string]string{"admission": "not_accepted"})
	}
	return nil
}

func (server *applianceAdminServer) existingApplianceOperation(ctx context.Context, mutation applianceMutation, kind appliancev1.ApplianceOperationKind) (store.ApplianceOperation, bool, error) {
	existing, err := server.controller.store.ApplianceOperationByRequest(ctx, mutation.requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ApplianceOperation{}, false, applianceAdmissionDeadline(mutation)
	}
	if err != nil {
		return store.ApplianceOperation{}, false, err
	}
	if existing.InstallationID != mutation.installationID || existing.ExpectedRevision != mutation.expectedRevision ||
		existing.DeadlineUnixMillis != mutation.deadlineUnixMillis {
		return store.ApplianceOperation{}, false, status.Error(codes.AlreadyExists,
			"request ID was reused with different appliance fencing")
	}
	if kind != appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UNSPECIFIED && existing.Kind != int32(kind) {
		return store.ApplianceOperation{}, false, status.Error(codes.AlreadyExists,
			"request ID was reused for a different appliance operation")
	}
	return existing, true, nil
}

func (server *applianceAdminServer) newRunningApplianceOperation(ctx context.Context, mutation applianceMutation,
	kind appliancev1.ApplianceOperationKind, stage string) (store.ApplianceOperation, error) {
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, kind); err != nil || found {
		return existing, err
	}
	id, err := randomID()
	if err != nil {
		return store.ApplianceOperation{}, err
	}
	return server.controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: id,
		InstallationID: mutation.installationID, RequestID: mutation.requestID, Kind: int32(kind),
		State:            int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
		ExpectedRevision: mutation.expectedRevision, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: stage})
}

func (server *applianceAdminServer) completeApplianceOperation(ctx context.Context, id string,
	expectedPhase appliancev1.ApplianceOperationPhase, stage string, receipt []byte) (store.ApplianceOperation, error) {
	return server.controller.store.TransitionApplianceOperation(ctx, id, store.ApplianceOperationTransition{
		ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		ExpectedPhase: int32(expectedPhase), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase:          int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason: int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED),
		Stage:          stage, Receipt: append([]byte(nil), receipt...)})
}

func (server *applianceAdminServer) failApplianceOperation(ctx context.Context, id string,
	expectedPhase appliancev1.ApplianceOperationPhase, stage, reason string, cause error) (store.ApplianceOperation, error) {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return server.controller.store.TransitionApplianceOperation(ctx, id, store.ApplianceOperationTransition{
		ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		ExpectedPhase: int32(expectedPhase), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED),
		Phase:          int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason: int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_OPERATION_FAILED),
		Stage:          stage, ErrorReason: reason, ErrorMessage: message})
}

func (server *applianceAdminServer) setDesiredState(ctx context.Context, mutation applianceMutation,
	planRevision uint64, enabled bool, state appliancev1.ApplianceState, kind appliancev1.ApplianceOperationKind,
	stage string) (*appliancev1.ApplianceOperationResponse, error) {
	unlock := server.controller.lockIntegration("appliance-lifecycle:" + mutation.installationID)
	defer unlock()
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, kind); err != nil {
		return nil, err
	} else if found {
		if enabled && existing.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED) {
			current, err := server.controller.store.GetApplianceInstallation(ctx, mutation.installationID)
			if err != nil {
				return nil, err
			}
			revoked, err := server.controller.store.HasApplianceRevocation(ctx, current.ID)
			if err != nil {
				return nil, err
			}
			if revoked {
				return nil, resourceError(codes.FailedPrecondition, "APPLIANCE_REVOKED", "appliance_installation", current.ID, "revoked appliance cannot start")
			}
			if current.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
				return nil, resourceError(codes.FailedPrecondition, "SEALED_INTEGRITY", "appliance_installation",
					current.ID, "sealed appliance cannot start")
			}
		}
		return applianceOperationResponse(existing), nil
	}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	if enabled {
		revoked, err := server.controller.store.HasApplianceRevocation(ctx, installation.ID)
		if err != nil {
			return nil, err
		}
		if revoked {
			return nil, resourceError(codes.FailedPrecondition, "APPLIANCE_REVOKED", "appliance_installation", installation.ID, "revoked appliance cannot start")
		}
	}
	if planRevision != installation.Revision {
		return nil, resourceError(codes.Aborted, "PLAN_REVISION_CONFLICT", "appliance_installation",
			installation.ID, "operation plan is stale")
	}
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		if enabled {
			return nil, resourceError(codes.FailedPrecondition, "SEALED_INTEGRITY", "appliance_installation",
				installation.ID, "sealed appliance cannot start")
		}
		state = appliancev1.ApplianceState_APPLIANCE_STATE_SEALED
	}
	installation.DesiredEnabled, installation.State = enabled, int32(state)
	operationID, err := randomID()
	if err != nil {
		return nil, err
	}
	operation, err := server.controller.store.BeginApplianceStateMutation(ctx, installation, mutation.expectedRevision, store.ApplianceOperation{
		ID: operationID, InstallationID: mutation.installationID, RequestID: mutation.requestID,
		Kind: int32(kind), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
		ExpectedRevision: mutation.expectedRevision, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: stage,
	})
	if err != nil || enabled || installation.RegistrationID == "" {
		return applianceOperationResponse(operation), err
	}
	registration, registrationErr := server.controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if registrationErr == nil && registration.OwnerKind == store.RemoteRegistrationPrivate &&
		registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) &&
		registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		_, registrationErr = server.controller.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision,
			int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING))
	}
	if registrationErr != nil {
		_, _ = server.failApplianceOperation(ctx, operation.ID,
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED,
			stage, "REGISTRATION_DRAIN_FAILED", registrationErr)
		return nil, registrationErr
	}
	return applianceOperationResponse(operation), err
}
