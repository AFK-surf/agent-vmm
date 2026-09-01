//go:build darwin

package host

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *operatorServer) BeginRemoteEnrollment(ctx context.Context, request *hostv1.BeginRemoteEnrollmentRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetEndpoint() == "" || request.GetProtocolVersion() == "" {
		return nil, invalidField("request_id/endpoint/protocol_version", "request ID, endpoint, and protocol version are required")
	}
	if existing, err := server.controller.store.RemoteRegistrationByEnrollmentRequest(ctx, request.GetRequestId()); err == nil {
		return server.controller.remoteRegistrationResponse(ctx, existing), nil
	}
	registration, err := registrationFromPool(request.GetPool(), server.controller.remoteCapacity)
	if err != nil {
		return nil, err
	}
	registration.ID = request.GetRegistrationId()
	if registration.ID == "" {
		registration.ID, err = randomID()
		if err != nil {
			return nil, err
		}
	}
	registration.Endpoint = request.GetEndpoint()
	registration.DisplayName = request.GetDisplayName()
	registration.ProtocolVersion = request.GetProtocolVersion()
	registration.State = int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED)
	registration.ConnectionState = int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED)
	registration.EnrollmentState = int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL)
	registration.ConnectorPhase = int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_ENROLLING)
	registration.EnrollmentRequestID = request.GetRequestId()
	registration.TrustBundle = append([]byte(nil), request.GetTrustBundle()...)
	registration.ScopeDigest = request.GetScopeDigest()
	registration.SourcePolicyRevision = request.GetSourcePolicyRevision()
	registration, err = server.controller.store.CreateRemoteRegistration(ctx, registration)
	if err != nil {
		if existing, lookupErr := server.controller.store.RemoteRegistrationByEnrollmentRequest(ctx, request.GetRequestId()); lookupErr == nil {
			return server.controller.remoteRegistrationResponse(ctx, existing), nil
		}
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_CREATE_FAILED", "remote_registration", registration.ID, err.Error())
	}
	return server.controller.remoteRegistrationResponse(ctx, registration), nil
}

func (server *operatorServer) ApplyManagedRemoteEnrollment(ctx context.Context, request *hostv1.ApplyManagedRemoteEnrollmentRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	managed := request.GetRegistration()
	if managed == nil || managed.GetRegistrationId() == "" || managed.GetSourcePolicyRevision() == 0 || len(managed.GetTrustBundle()) == 0 || !validScopeDigest(managed.GetScopeDigest()) {
		return nil, invalidField("managed_registration", "complete managed registration identity, trust, scope, and policy are required")
	}
	if len(request.GetEnrollmentToken()) == 0 || len(request.GetEnrollmentToken()) > remoteconnector.MaxCredentialBytes {
		return nil, invalidField("enrollment_token", "a bounded enrollment token is required")
	}
	registered, err := server.BeginRemoteEnrollment(ctx, managed)
	if err != nil {
		return nil, err
	}
	registration := registered.GetRegistration()
	if registration.GetId() != managed.GetRegistrationId() {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_IDENTITY_MISMATCH", "remote_registration", managed.GetRegistrationId(), "enrollment request is already bound to another registration")
	}
	stored, err := server.controller.store.GetRemoteRegistration(ctx, registration.GetId())
	if err != nil {
		return nil, err
	}
	if matches, matchErr := managedRegistrationMatches(managed, stored, server.controller.remoteCapacity); matchErr != nil || !matches {
		return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_IDENTITY_MISMATCH", "remote_registration", managed.GetRegistrationId(), "managed registration identity or policy does not match durable state")
	}
	if registration.GetEnrollmentState() == hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY {
		server.controller.ensureExternalConnector(registration.GetId())
		return registered, nil
	}
	if stored.EnrollmentState == int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE) {
		stored, err = server.controller.store.RetryRemoteEnrollment(ctx, stored.ID, stored.Revision)
		if err != nil {
			return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", stored.ID, err.Error())
		}
	}
	config, err := server.controller.externalConnectorConfig(stored)
	if err != nil {
		return nil, invalidField("managed_registration", err.Error())
	}
	token := append([]byte(nil), request.GetEnrollmentToken()...)
	if _, err := remoteconnector.Enroll(ctx, config, token); err != nil {
		_, _ = server.controller.store.SetRemoteEnrollment(ctx, stored.ID, stored.Revision,
			int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL),
			int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE),
			"MANAGED_ENROLLMENT_FAILED", "The managed controller enrollment did not complete.")
		return nil, resourceError(codes.Unavailable, "MANAGED_ENROLLMENT_FAILED", "remote_registration", stored.ID, "managed controller enrollment failed")
	}
	completed, err := server.controller.store.SetRemoteEnrollment(ctx, stored.ID, stored.Revision,
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL),
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY), "", "")
	if err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", stored.ID, err.Error())
	}
	server.controller.ensureExternalConnector(completed.ID)
	return server.controller.remoteRegistrationResponse(ctx, completed), nil
}

func managedRegistrationMatches(managed *hostv1.BeginRemoteEnrollmentRequest, stored store.RemoteRegistration, capacity store.Environment) (bool, error) {
	policy, err := registrationFromPool(managed.GetPool(), capacity)
	if err != nil {
		return false, err
	}
	return stored.ID == managed.GetRegistrationId() && stored.Endpoint == managed.GetEndpoint() &&
		stored.DisplayName == managed.GetDisplayName() && stored.ProtocolVersion == managed.GetProtocolVersion() &&
		bytes.Equal(stored.TrustBundle, managed.GetTrustBundle()) && stored.ScopeDigest == managed.GetScopeDigest() &&
		stored.SourcePolicyRevision == managed.GetSourcePolicyRevision() &&
		stored.MaxEnvironments == policy.MaxEnvironments && stored.AggregateCPUMillis == policy.AggregateCPUMillis &&
		stored.AggregateMemoryBytes == policy.AggregateMemoryBytes && stored.AggregatePIDs == policy.AggregatePIDs &&
		stored.AggregateDiskBytes == policy.AggregateDiskBytes && stored.PerEnvCPUMillis == policy.PerEnvCPUMillis &&
		stored.PerEnvMemoryBytes == policy.PerEnvMemoryBytes && stored.PerEnvPIDs == policy.PerEnvPIDs &&
		stored.PerEnvDiskBytes == policy.PerEnvDiskBytes && stored.MaxEgressMode == policy.MaxEgressMode &&
		stored.MaxLeaseSeconds == policy.MaxLeaseSeconds && stored.DisconnectGraceSeconds == policy.DisconnectGraceSeconds &&
		stored.StopGraceSeconds == policy.StopGraceSeconds, nil
}

func validScopeDigest(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func (server *operatorServer) CompleteRemoteEnrollment(ctx context.Context, request *hostv1.CompleteRemoteEnrollmentRequest) (*hostv1.RemoteRegistrationResponse, error) {
	return server.setEnrollment(ctx, request.GetRequestId(), request.GetRegistrationId(), request.GetExpectedRevision(),
		hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL,
		hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY, "", "")
}

func (server *operatorServer) FailRemoteEnrollment(ctx context.Context, request *hostv1.FailRemoteEnrollmentRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if !boundedError(request.GetErrorCode(), request.GetErrorMessage()) {
		return nil, invalidField("enrollment failure", "failure details are invalid")
	}
	return server.setEnrollment(ctx, request.GetRequestId(), request.GetRegistrationId(), request.GetExpectedRevision(),
		hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL,
		hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_SETUP_INCOMPLETE, request.GetErrorCode(), request.GetErrorMessage())
}

func (server *operatorServer) RetryRemoteEnrollment(ctx context.Context, request *hostv1.RetryRemoteEnrollmentRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetRegistrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	updated, err := server.controller.store.RetryRemoteEnrollment(ctx, request.GetRegistrationId(), request.GetExpectedRevision())
	if err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", request.GetRegistrationId(), err.Error())
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) setEnrollment(ctx context.Context, requestID, id string, revision uint64, from, state hostv1.RemoteEnrollmentState, code, message string) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if requestID == "" || id == "" || revision == 0 {
		return nil, invalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	updated, err := server.controller.store.SetRemoteEnrollment(ctx, id, revision, int32(from), int32(state), code, message)
	if err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", id, err.Error())
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) RemoveIncompleteRemoteRegistration(ctx context.Context, request *hostv1.RemoveIncompleteRemoteRegistrationRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetRegistrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	if err := server.controller.store.DeleteIncompleteRemoteRegistration(ctx, request.GetRegistrationId(), request.GetExpectedRevision()); err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", request.GetRegistrationId(), err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (server *operatorServer) BeginRemoteRevocation(ctx context.Context, request *hostv1.BeginRemoteRevocationRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetRegistrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	updated, err := server.controller.store.BeginRemoteRevocation(ctx, request.GetRegistrationId(), request.GetExpectedRevision())
	server.controller.remoteAdmissionMu.Unlock()
	if err == nil {
		server.controller.stopExternalConnector(request.GetRegistrationId())
		if revokeErr := server.controller.revokeRemoteLeases(request.GetRegistrationId()); revokeErr != nil {
			return nil, resourceError(codes.FailedPrecondition, "REMOTE_LEASE_REVOKE_FAILED", "remote_registration", request.GetRegistrationId(), revokeErr.Error())
		}
		return server.controller.remoteRegistrationResponse(ctx, updated), nil
	}
	return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", request.GetRegistrationId(), err.Error())
}

func (server *operatorServer) CompleteRemoteRevocation(ctx context.Context, request *hostv1.CompleteRemoteRevocationRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetRegistrationId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/registration_id/expected_revision", "request ID, registration ID, and revision are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	updated, err := server.controller.store.CompleteRemoteRevocation(ctx, request.GetRegistrationId(), request.GetExpectedRevision())
	server.controller.remoteAdmissionMu.Unlock()
	if err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", request.GetRegistrationId(), err.Error())
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) UpdateRemotePool(ctx context.Context, request *hostv1.UpdateRemotePoolRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/expected_revision", "request ID and revision are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	current, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	if err != nil {
		return nil, err
	}
	update, err := registrationFromPool(request.GetPool(), server.controller.remoteCapacity)
	if err != nil {
		return nil, err
	}
	update.ID = current.ID
	updated, err := server.controller.store.UpdateRemotePool(ctx, update, request.GetExpectedRevision())
	if err != nil {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "remote_registration", current.ID, err.Error())
	}
	if current.ConnectionState == int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) && current.ConnectionInstance != "" {
		server.controller.hostFrontends.closeRegistration(current.ID, current.ConnectionInstance, hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
		delete(server.controller.remoteConnectionEpochs, frontendConnectionKey(current.ID, current.ConnectionInstance))
		updated, err = server.controller.store.SetRemoteConnection(ctx, current.ID, current.ConnectionInstance, int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED))
		if err != nil {
			return nil, err
		}
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) SetRemoteRegistrationState(ctx context.Context, request *hostv1.SetRemoteRegistrationStateRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("request_id/expected_revision", "request ID and revision are required")
	}
	if request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED &&
		request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING {
		return nil, invalidField("state", "registration state must be enabled or draining")
	}
	server.controller.remoteAdmissionMu.Lock()
	current, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		server.controller.remoteAdmissionMu.Unlock()
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	updated, err := server.controller.store.SetRemoteRegistrationState(ctx, current.ID, request.GetExpectedRevision(), int32(request.GetState()))
	server.controller.remoteAdmissionMu.Unlock()
	if err != nil {
		return nil, resourceError(codes.Aborted, "REGISTRATION_REVISION_CONFLICT", "remote_registration", current.ID, err.Error())
	}
	if request.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING {
		server.controller.stopExternalConnector(current.ID)
	} else if externalRegistrationDesired(updated) {
		server.controller.ensureExternalConnector(current.ID)
	}
	return server.controller.remoteRegistrationResponse(ctx, updated), nil
}

func (server *operatorServer) GetRemoteRegistration(ctx context.Context, request *hostv1.GetRemoteRegistrationRequest) (*hostv1.RemoteRegistrationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetRegistrationId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "REGISTRATION_NOT_FOUND", "remote_registration", "active", "active remote registration does not exist")
	}
	return server.controller.remoteRegistrationResponse(ctx, registration), nil
}

func (server *operatorServer) ListRemoteRegistrations(ctx context.Context, _ *hostv1.ListRemoteRegistrationsRequest) (*hostv1.ListRemoteRegistrationsResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	registrations, err := server.controller.store.ListRemoteRegistrations(ctx)
	if err != nil {
		return nil, err
	}
	response := &hostv1.ListRemoteRegistrationsResponse{}
	for _, registration := range registrations {
		response.Registrations = append(response.Registrations, server.controller.remoteRegistrationResponse(ctx, registration).GetRegistration())
	}
	return response, nil
}

func registrationFromPool(pool *hostv1.RemotePoolPolicy, capacity store.Environment) (store.RemoteRegistration, error) {
	if pool == nil || pool.GetAggregateLimits() == nil || pool.GetPerEnvironmentLimits() == nil ||
		pool.GetMaxLeaseDuration() == nil || pool.GetDisconnectGrace() == nil || pool.GetStopGrace() == nil {
		return store.RemoteRegistration{}, invalidField("pool", "complete remote pool policy is required")
	}
	aggregate := pool.GetAggregateLimits()
	perEnvironment := pool.GetPerEnvironmentLimits()
	if pool.GetMaxEnvironments() == 0 || pool.GetMaxEnvironments() > maxRemoteEnvironments || validatePolicy(aggregate, pool.GetMaxEgressMode()) != nil || validatePolicy(perEnvironment, pool.GetMaxEgressMode()) != nil {
		return store.RemoteRegistration{}, invalidField("pool", "remote pool limits are invalid")
	}
	if aggregate.GetCpuMillis() > capacity.CPUMillis || aggregate.GetMemoryBytes() > capacity.MemoryBytes ||
		aggregate.GetPids() > capacity.PIDs || aggregate.GetDiskBytes() > capacity.DiskBytes {
		return store.RemoteRegistration{}, invalidField("aggregate_limits", "remote pool exceeds configured VM capacity")
	}
	if perEnvironment.GetCpuMillis() > aggregate.GetCpuMillis() || perEnvironment.GetMemoryBytes() > aggregate.GetMemoryBytes() ||
		perEnvironment.GetPids() > aggregate.GetPids() || perEnvironment.GetDiskBytes() > aggregate.GetDiskBytes() {
		return store.RemoteRegistration{}, invalidField("per_environment_limits", "per-environment limit exceeds aggregate pool")
	}
	lease := pool.GetMaxLeaseDuration().AsDuration()
	disconnect := pool.GetDisconnectGrace().AsDuration()
	stop := pool.GetStopGrace().AsDuration()
	if lease < time.Minute || lease > 24*time.Hour || disconnect < time.Second || disconnect > 10*time.Minute || stop < time.Second || stop > 5*time.Minute {
		return store.RemoteRegistration{}, invalidField("pool duration", "lease or grace duration is outside supported bounds")
	}
	return store.RemoteRegistration{
		MaxEnvironments: pool.GetMaxEnvironments(), AggregateCPUMillis: aggregate.GetCpuMillis(), AggregateMemoryBytes: aggregate.GetMemoryBytes(),
		AggregatePIDs: aggregate.GetPids(), AggregateDiskBytes: aggregate.GetDiskBytes(), PerEnvCPUMillis: perEnvironment.GetCpuMillis(),
		PerEnvMemoryBytes: perEnvironment.GetMemoryBytes(), PerEnvPIDs: perEnvironment.GetPids(), PerEnvDiskBytes: perEnvironment.GetDiskBytes(),
		MaxEgressMode: int32(pool.GetMaxEgressMode()), MaxLeaseSeconds: uint32(lease / time.Second),
		DisconnectGraceSeconds: uint32(disconnect / time.Second), StopGraceSeconds: uint32(stop / time.Second),
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
	return &hostv1.RemoteRegistration{
		Id: registration.ID, Endpoint: registration.Endpoint, DisplayName: registration.DisplayName, ProtocolVersion: registration.ProtocolVersion,
		Revision: registration.Revision, PoolPolicyRevision: registration.PoolPolicyRevision, State: hostv1.RemoteRegistrationState(registration.State), ConnectionState: connectionState,
		EnrollmentState: hostv1.RemoteEnrollmentState(registration.EnrollmentState), EnrollmentErrorCode: registration.EnrollmentErrorCode,
		EnrollmentErrorMessage: registration.EnrollmentErrorMessage, CreatedAt: timestamppb.New(registration.CreatedAt), UpdatedAt: timestamppb.New(registration.UpdatedAt),
		OwnerKind: hostv1.RemoteRegistrationOwnerKind(registration.OwnerKind), InstallationId: registration.InstallationID,
		InventoryRevision: registration.InventoryRevision,
		ConnectorStatus: &hostv1.ConnectorStatus{Phase: connectorPhase, ConnectorRelease: registration.ConnectorRelease,
			ObservedAt: timestamppb.New(registration.ConnectorObservedAt), LastConnectedAt: timestamppb.New(registration.LastConnectedAt),
			DisconnectedAt: timestamppb.New(registration.DisconnectedAt), NextRetryAt: timestamppb.New(registration.NextRetryAt),
			ReconnectAttempt: registration.ReconnectAttempt, FailureCode: failureCode, FailureMessage: failureMessage},
		Pool: &hostv1.RemotePoolPolicy{
			MaxEnvironments:      registration.MaxEnvironments,
			AggregateLimits:      &hostv1.ResourceLimits{CpuMillis: optional(registration.AggregateCPUMillis), MemoryBytes: optional(registration.AggregateMemoryBytes), Pids: optional(registration.AggregatePIDs), DiskBytes: optional(registration.AggregateDiskBytes)},
			PerEnvironmentLimits: &hostv1.ResourceLimits{CpuMillis: optional(registration.PerEnvCPUMillis), MemoryBytes: optional(registration.PerEnvMemoryBytes), Pids: optional(registration.PerEnvPIDs), DiskBytes: optional(registration.PerEnvDiskBytes)},
			MaxEgressMode:        hostv1.EgressMode(registration.MaxEgressMode), MaxLeaseDuration: durationpb.New(time.Duration(registration.MaxLeaseSeconds) * time.Second),
			DisconnectGrace: durationpb.New(time.Duration(registration.DisconnectGraceSeconds) * time.Second), StopGrace: durationpb.New(time.Duration(registration.StopGraceSeconds) * time.Second),
		},
	}
}
