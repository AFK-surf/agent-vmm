//go:build darwin || linux

package remote

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *Owner) beginRemoteRegistration(ctx context.Context, request *hostv1.RemoteRegistrationConnection) (*hostv1.ApplyRemoteRegistrationResponse, error) {
	if request.GetRequestId() == "" || request.GetEndpoint() == "" {
		return nil, hostv1.InvalidField("request_id/endpoint", "request ID and endpoint are required")
	}
	if _, err := remoteconnector.ParseEndpoint(request.GetEndpoint()); err != nil {
		return nil, hostv1.InvalidField("endpoint", err.Error())
	}
	if err := server.cleanup.RequireUndisposedRegistration(ctx, request.GetRegistrationId(), request.GetRequestId()); err != nil {
		return nil, err
	}
	if existing, err := server.store.RemoteRegistrationByEnrollmentRequest(ctx, request.GetRequestId()); err == nil {
		return &hostv1.ApplyRemoteRegistrationResponse{Registration: server.RegistrationCurrent(ctx, existing)}, nil
	}
	registration, err := registrationFromPool(request.GetPool(), server.Capacity())
	if err != nil {
		return nil, err
	}
	registration.ID = request.GetRegistrationId()
	if registration.ID == "" {
		registration.ID, err = opreceipt.NewID()
		if err != nil {
			return nil, err
		}
	}
	registration.Endpoint = request.GetEndpoint()
	registration.DisplayName = request.GetDisplayName()
	registration.State = int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED)
	registration.ConnectionState = int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED)
	registration.EnrollmentState = int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL)
	registration.ConnectorPhase = int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_ENROLLING)
	registration.EnrollmentRequestID = request.GetRequestId()
	registration.TrustBundle = append([]byte(nil), request.GetTrustBundle()...)
	registration.SourcePolicyRevision = request.GetSourcePolicyRevision()
	registration, err = server.store.CreateRemoteRegistration(ctx, registration)
	if err != nil {
		if existing, lookupErr := server.store.RemoteRegistrationByEnrollmentRequest(ctx, request.GetRequestId()); lookupErr == nil {
			return &hostv1.ApplyRemoteRegistrationResponse{Registration: server.RegistrationCurrent(ctx, existing)}, nil
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &hostv1.ApplyRemoteRegistrationResponse{Registration: server.RegistrationCurrent(ctx, registration)}, nil
}

func (server *Owner) ApplyRemoteRegistration(ctx context.Context, request *hostv1.ApplyRemoteRegistrationRequest) (*hostv1.ApplyRemoteRegistrationResponse, error) {
	ctx, releaseInventory, err := server.inventory.Enter(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	managed := request.GetRegistration()
	if managed == nil || managed.GetRegistrationId() == "" {
		return nil, hostv1.InvalidField("registration", "registration identity and policy are required")
	}
	if len(request.GetCredential()) == 0 || len(request.GetCredential()) > remoteconnector.MaxCredentialBytes {
		return nil, hostv1.InvalidField("credential", "a bounded credential is required")
	}
	unlock, err := server.remoteEnrollmentLocks.Lock(ctx, managed.GetRegistrationId())
	if err != nil {
		return nil, err
	}
	defer unlock()
	server.remoteAdmissionMu.Lock()
	defer server.remoteAdmissionMu.Unlock()
	if err := server.cleanup.RequireUndisposedRegistration(ctx, managed.GetRegistrationId(), managed.GetRequestId()); err != nil {
		return nil, err
	}
	stored, err := server.store.GetRemoteRegistration(ctx, managed.GetRegistrationId())
	if errors.Is(err, sql.ErrNoRows) {
		registered, beginErr := server.beginRemoteRegistration(ctx, managed)
		if beginErr != nil {
			return nil, beginErr
		}
		if registered.GetRegistration().GetId() != managed.GetRegistrationId() {
			return nil, status.Error(codes.FailedPrecondition, "request is bound to another registration")
		}
		stored, err = server.store.GetRemoteRegistration(ctx, managed.GetRegistrationId())
	}
	if err != nil {
		return nil, err
	}
	if stored.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		return nil, status.Error(codes.PermissionDenied, "registration was explicitly revoked")
	}
	if !registrationConnectionMatches(managed, stored) {
		return nil, status.Error(codes.FailedPrecondition, "registration identity does not match durable state")
	}
	stored, err = server.applyRegistrationPolicy(ctx, managed, stored)
	if err != nil {
		return nil, err
	}
	if err := server.remoteCredentials.Replace(ctx, stored.ID, request.GetCredential()); err != nil {
		return nil, err
	}
	if stored.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) {
		stored, err = server.store.SetRemoteEnrollment(ctx, stored.ID, stored.Revision, stored.EnrollmentState, int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", "")
		if err != nil {
			return nil, err
		}
	}
	server.ensureExternalConnector(stored.ID)
	return &hostv1.ApplyRemoteRegistrationResponse{Registration: server.RegistrationCurrent(ctx, stored)}, nil
}

// registrationConnectionMatches compares the fields that name the
// controller and its authority. Policy fields are updated in place.
func registrationConnectionMatches(managed *hostv1.RemoteRegistrationConnection, stored store.RemoteRegistration) bool {
	return stored.ID == managed.GetRegistrationId() && stored.Endpoint == managed.GetEndpoint() &&
		bytes.Equal(stored.TrustBundle, managed.GetTrustBundle())
}

// applyRegistrationPolicy stores the display name, source policy
// revision, and pool policy of a repeated managed enrollment. An older source
// policy revision, or the same revision with a different pool, is a conflict.
func (controller *Owner) applyRegistrationPolicy(ctx context.Context, managed *hostv1.RemoteRegistrationConnection, stored store.RemoteRegistration) (store.RemoteRegistration, error) {
	update, err := registrationFromPool(managed.GetPool(), controller.Capacity())
	if err != nil {
		return store.RemoteRegistration{}, err
	}
	poolChanged := stored.PerEnvPIDs != update.PerEnvPIDs || stored.PerEnvDiskBytes != update.PerEnvDiskBytes ||
		stored.MaxEgressMode != update.MaxEgressMode || stored.StopGraceSeconds != update.StopGraceSeconds
	if managed.GetSourcePolicyRevision() < stored.SourcePolicyRevision || (managed.GetSourcePolicyRevision() == stored.SourcePolicyRevision && poolChanged) {
		return store.RemoteRegistration{}, status.Error(codes.Aborted, "managed policy revision is older than durable state")
	}
	if !poolChanged && stored.DisplayName == managed.GetDisplayName() && stored.SourcePolicyRevision == managed.GetSourcePolicyRevision() {
		return stored, nil
	}
	update.ID, update.DisplayName, update.SourcePolicyRevision = stored.ID, managed.GetDisplayName(), managed.GetSourcePolicyRevision()
	updated, err := controller.store.UpdateManagedRemoteRegistration(ctx, update, stored.Revision)
	if err != nil {
		return store.RemoteRegistration{}, status.Error(codes.Aborted, err.Error())
	}
	if poolChanged {
		return controller.fencePoolPolicyChange(ctx, stored, updated)
	}
	return updated, nil
}

// fencePoolPolicyChange closes the current connection after a pool change so
// the next connection reports the new pool policy revision.
func (controller *Owner) fencePoolPolicyChange(ctx context.Context, previous, updated store.RemoteRegistration) (store.RemoteRegistration, error) {
	if previous.ConnectionState != int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) || previous.ConnectionInstance == "" {
		return updated, nil
	}
	controller.deleteRemoteConnectionEpoch(previous.ID, previous.ConnectionInstance)
	return controller.store.SetRemoteConnection(ctx, previous.ID, previous.ConnectionInstance, int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED))
}

func (server *Owner) BeginRemoteRevocation(ctx context.Context, request *hostv1.BeginRemoteRevocationRequest) (*hostv1.BeginRemoteRevocationResponse, error) {
	if request.GetRequestId() == "" || request.GetRegistrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	server.remoteAdmissionMu.Lock()

	updated, err := server.store.BeginRemoteRevocation(ctx, request.GetRegistrationId(), request.GetExpectedRevision())
	server.remoteAdmissionMu.Unlock()
	if err == nil {
		if updated.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
			return &hostv1.BeginRemoteRevocationResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
		}
		server.StopExternalConnector(request.GetRegistrationId())
		if revokeErr := server.RetireRegistration(ctx, request.GetRegistrationId()); revokeErr != nil {
			return nil, status.Error(codes.FailedPrecondition, revokeErr.Error())
		}
		return &hostv1.BeginRemoteRevocationResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
	}
	if errors.Is(err, store.ErrRemoteRegistrationRevisionConflict) {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_REVISION_CONFLICT, "registration revision changed", hostv1.WithResource("remote_registration", request.GetRegistrationId()))
	}
	if errors.Is(err, store.ErrRemoteRegistrationEnrollmentState) {
		return nil, status.Error(codes.FailedPrecondition, "registration enrollment state does not permit revocation")
	}
	return nil, status.Error(codes.Unavailable, "remote registration could not be updated")
}

func (server *Owner) CompleteRemoteRevocation(ctx context.Context, request *hostv1.CompleteRemoteRevocationRequest) (*hostv1.CompleteRemoteRevocationResponse, error) {
	if request.GetRequestId() == "" || request.GetRegistrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	current, readErr := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if readErr != nil {
		return nil, status.Error(codes.Unavailable, "remote registration could not be read")
	}
	if current.Revision != request.GetExpectedRevision() {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_REVISION_CONFLICT, "registration revision changed", hostv1.WithResource("remote_registration", current.ID))
	}
	if current.State == int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED) {
		return &hostv1.CompleteRemoteRevocationResponse{Registration: server.RegistrationCurrent(ctx, current)}, nil
	}
	if revokeErr := server.RetireRegistration(ctx, request.GetRegistrationId()); revokeErr != nil {
		return nil, status.Error(codes.FailedPrecondition, revokeErr.Error())
	}
	server.remoteAdmissionMu.Lock()

	updated, err := server.store.CompleteRemoteRevocation(ctx, request.GetRegistrationId(), request.GetExpectedRevision())
	server.remoteAdmissionMu.Unlock()
	if err != nil {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_REVISION_CONFLICT, err.Error(), hostv1.WithResource("remote_registration", request.GetRegistrationId()))
	}
	return &hostv1.CompleteRemoteRevocationResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
}

func (server *Owner) UpdateRemotePool(ctx context.Context, request *hostv1.UpdateRemotePoolRequest) (*hostv1.UpdateRemotePoolResponse, error) {
	if request.GetRequestId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/expected_revision", "request ID and revision are required")
	}
	server.remoteAdmissionMu.Lock()

	defer server.remoteAdmissionMu.Unlock()
	current, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "active remote registration does not exist")
	}
	if err != nil {
		return nil, err
	}
	update, err := registrationFromPool(request.GetPool(), server.Capacity())
	if err != nil {
		return nil, err
	}
	update.ID = current.ID
	updated, err := server.store.UpdateRemotePool(ctx, update, request.GetExpectedRevision())
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	updated, err = server.fencePoolPolicyChange(ctx, current, updated)
	if err != nil {
		return nil, err
	}
	return &hostv1.UpdateRemotePoolResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
}

func (server *Owner) SetRemoteRegistrationState(ctx context.Context, request *hostv1.SetRemoteRegistrationStateRequest) (*hostv1.SetRemoteRegistrationStateResponse, error) {
	if request.GetRequestId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("request_id/expected_revision", "request ID and revision are required")
	}
	if request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING {
		return nil, hostv1.InvalidField("state", "registration state must be enabled or draining")
	}
	server.remoteAdmissionMu.Lock()

	if err := server.cleanup.RequireUndisposedRegistration(ctx, request.GetRegistrationId(), ""); err != nil {
		server.remoteAdmissionMu.Unlock()
		return nil, err
	}
	current, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		server.remoteAdmissionMu.Unlock()
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "remote registration does not exist")
		}
		return nil, status.Error(codes.Unavailable, "remote registration could not be read")
	}
	updated, err := server.store.SetRemoteRegistrationState(ctx, current.ID, request.GetExpectedRevision(), int32(request.GetState()))
	server.remoteAdmissionMu.Unlock()
	if err != nil {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_REVISION_CONFLICT, err.Error(), hostv1.WithResource("remote_registration", current.ID))
	}
	if externalRegistrationDesired(updated) {
		server.ensureExternalConnector(current.ID)
	} else {
		server.StopExternalConnector(current.ID)
	}
	return &hostv1.SetRemoteRegistrationStateResponse{Registration: server.RegistrationCurrent(ctx, updated)}, nil
}

func (server *Owner) GetRemoteRegistration(ctx context.Context, request *hostv1.GetRemoteRegistrationRequest) (*hostv1.GetRemoteRegistrationResponse, error) {
	registration, err := server.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if errors.Is(err, sql.ErrNoRows) {
		disposed, readErr := server.store.RegistrationDisposed(ctx, request.GetRegistrationId(), "")
		if readErr != nil {
			return nil, status.Error(codes.Unavailable, "local revocation facts could not be read")
		}
		if disposed {
			return nil, hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_LOCAL_DISPOSED, "the local operator disposed this registration", hostv1.WithResource("remote_registration", request.GetRegistrationId()))
		}
		return nil, status.Error(codes.NotFound, "remote registration does not exist")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "remote registration could not be read")
	}
	return &hostv1.GetRemoteRegistrationResponse{Registration: server.RegistrationCurrent(ctx, registration)}, nil
}

func registrationFromPool(pool *hostv1.RemotePoolPolicy, capacity store.Environment) (store.RemoteRegistration, error) {
	if pool == nil || pool.GetPerEnvironmentLimits() == nil || pool.GetStopGrace() == nil {
		return store.RemoteRegistration{}, hostv1.InvalidField("pool", "complete remote pool policy is required")
	}
	perEnvironment := pool.GetPerEnvironmentLimits()
	if perEnvironment.Pids == nil || perEnvironment.DiskBytes == nil || perEnvironment.GetPids() == 0 || perEnvironment.GetPids() > 1<<20 || perEnvironment.GetDiskBytes() == 0 || perEnvironment.GetDiskBytes() > 1<<50 || (pool.GetMaxEgressMode() != hostv1.EgressMode_EGRESS_MODE_DENY_ALL && pool.GetMaxEgressMode() != hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET) {
		return store.RemoteRegistration{}, hostv1.InvalidField("pool", "remote pool limits are invalid")
	}
	if perEnvironment.GetPids() > capacity.PIDs || perEnvironment.GetDiskBytes() > capacity.DiskBytes {
		return store.RemoteRegistration{}, hostv1.InvalidField("per_environment_limits", "per-environment limit exceeds configured VM capacity")
	}
	stop := pool.GetStopGrace().AsDuration()
	if stop < time.Second || stop > 5*time.Minute {
		return store.RemoteRegistration{}, hostv1.InvalidField("pool duration", "stop grace duration is outside supported bounds")
	}
	return store.RemoteRegistration{
		PerEnvPIDs: perEnvironment.GetPids(), PerEnvDiskBytes: perEnvironment.GetDiskBytes(),
		MaxEgressMode: int32(pool.GetMaxEgressMode()), StopGraceSeconds: uint32(stop / time.Second),
	}, nil
}

func registrationToProto(registration store.RemoteRegistration) *hostv1.RemoteRegistration {
	connectionState := hostv1.RemoteConnectionState(registration.ConnectionState)
	connectorPhase := hostv1.ConnectorPhase(registration.ConnectorPhase)
	failureCode := registration.ConnectorFailureCode
	failureMessage := registration.ConnectorFailureMessage
	if connectionState == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED &&
		(time.Since(registration.ConnectorObservedAt) > 20*time.Second || registration.ConnectorObservedAt.IsZero()) {
		connectionState = hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED
		connectorPhase = hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT
		failureCode = "HEARTBEAT_STALE"
		failureMessage = "The connector stopped reporting status."
	}
	pids, disk := registration.PerEnvPIDs, registration.PerEnvDiskBytes
	return &hostv1.RemoteRegistration{
		Id: registration.ID, Endpoint: registration.Endpoint, DisplayName: registration.DisplayName,
		Revision: registration.Revision, PoolPolicyRevision: registration.PoolPolicyRevision, State: hostv1.RemoteRegistrationState(registration.State), ConnectionState: connectionState,
		EnrollmentState: hostv1.RemoteEnrollmentState(registration.EnrollmentState), EnrollmentErrorCode: registration.EnrollmentErrorCode,
		EnrollmentErrorMessage: registration.EnrollmentErrorMessage, CreatedAt: timestamppb.New(registration.CreatedAt), UpdatedAt: timestamppb.New(registration.UpdatedAt),
		OwnerKind: hostv1.RemoteRegistrationOwnerKind(registration.OwnerKind), InstallationId: registration.InstallationID,
		InventoryRevision: registration.InventoryRevision,
		SourceRequestId:   strings.TrimSuffix(strings.TrimSuffix(registration.EnrollmentRequestID, ":managed-enrollment"), ":registration"),
		ConnectorStatus: &hostv1.ConnectorStatus{Phase: connectorPhase, ConnectorRelease: registration.ConnectorRelease,
			ObservedAt: timestamppb.New(registration.ConnectorObservedAt), LastConnectedAt: timestamppb.New(registration.LastConnectedAt),
			DisconnectedAt: timestamppb.New(registration.DisconnectedAt), NextRetryAt: timestamppb.New(registration.NextRetryAt),
			ReconnectAttempt: registration.ReconnectAttempt, FailureCode: failureCode, FailureMessage: failureMessage},
		Pool: &hostv1.RemotePoolPolicy{
			PerEnvironmentLimits: &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk},
			MaxEgressMode:        hostv1.EgressMode(registration.MaxEgressMode), StopGrace: durationpb.New(time.Duration(registration.StopGraceSeconds) * time.Second),
		},
	}
}
