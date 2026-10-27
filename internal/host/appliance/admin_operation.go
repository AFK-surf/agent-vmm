//go:build darwin || linux

package appliance

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type applianceMutation struct {
	installationID     string
	requestID          string
	expectedRevision   uint64
	deadlineUnixMillis int64
}

func validateApplianceMutation(value applianceMutation) error {
	if value.installationID == "" || value.requestID == "" {
		return hostv1.InvalidField("installation_id/request_id", "appliance mutation identity is required")
	}
	if value.deadlineUnixMillis <= time.Now().UnixMilli() {
		return status.Error(codes.DeadlineExceeded, "appliance mutation deadline expired")
	}
	return nil
}

func (server *Owner) exactInstallation(ctx context.Context, mutation applianceMutation) (store.ApplianceInstallation, error) {
	if err := validateApplianceMutation(mutation); err != nil {
		return store.ApplianceInstallation{}, err
	}
	installation, err := server.store.GetApplianceInstallation(ctx, mutation.installationID)
	if err != nil {
		return store.ApplianceInstallation{}, err
	}
	if installation.Revision != mutation.expectedRevision {
		return store.ApplianceInstallation{}, status.Error(codes.Aborted, "installation revision changed")
	}
	_, err = server.store.GetApplianceArtifact(ctx, installation.ArtifactID)
	if err != nil {
		return store.ApplianceInstallation{}, status.Error(codes.FailedPrecondition, "fixed artifact is unavailable")
	}
	return installation, nil
}

func applianceAdmissionDeadline(mutation applianceMutation) error {
	if mutation.deadlineUnixMillis <= time.Now().UnixMilli() {
		return hostv1.NewError(codes.DeadlineExceeded, hostv1.ErrorReason_ERROR_REASON_APPLIANCE_OPERATION_NOT_ADMITTED, "appliance intent expired before admission; review and approve a new plan", hostv1.WithResource("appliance_installation", mutation.installationID), hostv1.WithMetadata(map[string]string{"admission": "not_accepted"}))
	}
	return nil
}

func (server *Owner) existingApplianceOperation(ctx context.Context, mutation applianceMutation, kind appliancev1.ApplianceOperationKind) (store.ApplianceOperation, bool, error) {
	existing, err := server.store.ApplianceOperationByRequest(ctx, mutation.requestID)
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

func (server *Owner) completeApplianceOperation(ctx context.Context, id string,
	expectedPhase appliancev1.ApplianceOperationPhase, stage string, receipt []byte) (store.ApplianceOperation, error) {
	return server.store.TransitionApplianceOperation(ctx, id, store.ApplianceOperationTransition{
		ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		ExpectedPhase: int32(expectedPhase), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase:          int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason: int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED),
		Stage:          stage, Receipt: append([]byte(nil), receipt...)})
}

func (server *Owner) failApplianceOperation(ctx context.Context, id string,
	expectedPhase appliancev1.ApplianceOperationPhase, stage, reason string, cause error) (store.ApplianceOperation, error) {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return server.store.TransitionApplianceOperation(ctx, id, store.ApplianceOperationTransition{
		ExpectedState: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		ExpectedPhase: int32(expectedPhase), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED),
		Phase:          int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason: int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_OPERATION_FAILED),
		Stage:          stage, ErrorReason: reason, ErrorMessage: message})
}

func (server *Owner) setDesiredState(ctx context.Context, mutation applianceMutation,
	planRevision uint64, enabled bool, state appliancev1.ApplianceState, kind appliancev1.ApplianceOperationKind,
	stage string) (*appliancev1.ApplianceOperation, error) {
	releaseRemoteAdmission := server.remote.LockAdmission()

	defer releaseRemoteAdmission()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, kind); err != nil {
		return nil, err
	} else if found {
		if enabled && existing.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED) {
			current, err := server.store.GetApplianceInstallation(ctx, mutation.installationID)
			if err != nil {
				return nil, err
			}
			revoked, err := server.store.HasApplianceRevocation(ctx, current.ID)
			if err != nil {
				return nil, err
			}
			if revoked {
				return nil, status.Error(codes.FailedPrecondition, "revoked appliance cannot start")
			}
			if current.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
				return nil, status.Error(codes.FailedPrecondition, "sealed appliance cannot start")
			}
		}
		return applianceOperationFromStore(existing), nil
	}
	installation, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	if enabled {
		revoked, err := server.store.HasApplianceRevocation(ctx, installation.ID)
		if err != nil {
			return nil, err
		}
		if revoked {
			return nil, status.Error(codes.FailedPrecondition, "revoked appliance cannot start")
		}
	}
	if planRevision != installation.Revision {
		return nil, status.Error(codes.Aborted, "operation plan is stale")
	}
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		if enabled {
			return nil, status.Error(codes.FailedPrecondition, "sealed appliance cannot start")
		}
		state = appliancev1.ApplianceState_APPLIANCE_STATE_SEALED
	}
	installation.DesiredEnabled, installation.State = enabled, int32(state)
	operationID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	operation, err := server.store.BeginApplianceStateMutation(ctx, installation, mutation.expectedRevision, store.ApplianceOperation{
		ID: operationID, InstallationID: mutation.installationID, RequestID: mutation.requestID,
		Kind: int32(kind), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING),
		Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
		ExpectedRevision: mutation.expectedRevision, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: stage,
	})
	if err != nil || enabled || installation.RegistrationID == "" {
		return applianceOperationFromStore(operation), err
	}
	registration, registrationErr := server.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if registrationErr == nil && registration.OwnerKind == store.RemoteRegistrationPrivate &&
		registration.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) &&
		registration.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		_, registrationErr = server.store.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision,
			int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING))
	}
	if registrationErr != nil {
		_, _ = server.failApplianceOperation(ctx, operation.ID,
			appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED,
			stage, "REGISTRATION_DRAIN_FAILED", registrationErr)
		return nil, registrationErr
	}
	return applianceOperationFromStore(operation), err
}
