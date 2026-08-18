//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func (server *applianceAdminServer) StageArtifact(ctx context.Context, request *appliancev1.StageArtifactRequest) (*appliancev1.ApplianceOperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetDeadlineUnixMillis() <= time.Now().UnixMilli() {
		return nil, invalidField("request_id/deadline_unix_millis", "artifact staging identity is invalid")
	}
	if existing, err := server.controller.store.ApplianceOperationByRequest(ctx, request.GetRequestId()); err == nil {
		if existing.InstallationID != "" || existing.DeadlineUnixMillis != request.GetDeadlineUnixMillis() {
			return nil, resourceError(codes.AlreadyExists, "REQUEST_ID_CONFLICT", "appliance_artifact", "", "request ID was reused")
		}
		response := applianceOperationResponse(existing)
		response.ArtifactId = string(existing.Receipt)
		return response, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	canonical, canonicalHash, err := (applianceArtifactVerifier{store: server.controller.store}).verify(ctx,
		request.GetManifest(), request.GetPublisherSignature(), nil)
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "ARTIFACT_VERIFICATION_FAILED", "appliance_artifact", "", err.Error())
	}
	artifact, err := server.controller.store.GetApplianceArtifactByCanonicalHash(ctx, canonicalHash)
	if errors.Is(err, sql.ErrNoRows) {
		artifactID, idErr := randomID()
		if idErr != nil {
			return nil, idErr
		}
		artifact = store.ApplianceArtifact{ID: artifactID, CanonicalHash: append([]byte(nil), canonicalHash...),
			ImageDigest: request.GetManifest().GetImageManifestDigest(), ImageReference: request.GetManifest().GetImageReference(),
			Platform: request.GetManifest().GetPlatform(), PublisherKeyID: request.GetManifest().GetPublisherKeyId(),
			ProductID: request.GetManifest().GetProductId(), Release: request.GetManifest().GetRelease(), Verified: true,
			Manifest: canonical, PublisherSignature: append([]byte(nil), request.GetPublisherSignature()...)}
	} else if err != nil {
		return nil, err
	}
	operationID, err := randomID()
	if err != nil {
		return nil, err
	}
	operation := store.ApplianceOperation{ID: operationID, RequestID: request.GetRequestId(),
		Kind:               int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE),
		State:              int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase:              int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason:     int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED),
		DeadlineUnixMillis: request.GetDeadlineUnixMillis(), Stage: "artifact-staged", Receipt: []byte(artifact.ID)}
	if artifact.CreatedAt.IsZero() {
		artifact, operation, err = server.controller.store.StageApplianceArtifact(ctx, artifact, operation)
	} else {
		operation, err = server.controller.store.CreateApplianceOperation(ctx, operation)
	}
	if err != nil {
		return nil, err
	}
	response := applianceOperationResponse(operation)
	response.ArtifactId = artifact.ID
	return response, nil
}

func (server *applianceAdminServer) PlanInstall(ctx context.Context, request *appliancev1.PlanInstallRequest) (*appliancev1.AppliancePlanResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	if err := validateApplianceMutation(mutation); err != nil {
		return nil, err
	}
	if request.GetExpectedRevision() != 0 || request.GetArtifactId() == "" {
		return nil, resourceError(codes.Aborted, "INSTALLATION_REVISION_CONFLICT", "appliance_installation", request.GetInstallationId(), "new installation must start at revision zero")
	}
	if _, err := server.controller.store.GetApplianceInstallation(ctx, request.GetInstallationId()); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return nil, err
		}
		return nil, resourceError(codes.AlreadyExists, "INSTALLATION_EXISTS", "appliance_installation", request.GetInstallationId(), "installation already exists")
	}
	artifact, err := server.controller.store.GetApplianceArtifact(ctx, request.GetArtifactId())
	if err != nil || !artifact.Verified {
		return nil, resourceError(codes.FailedPrecondition, "ARTIFACT_ID_INVALID", "appliance_artifact", request.GetArtifactId(), "verified artifact is unavailable")
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "GRANT_EXPANSION", "appliance_artifact", artifact.ID, "approved grant exceeds the signed manifest")
	}
	if err := validateApprovedGrant(manifest, request.GetApprovedGrant()); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "GRANT_EXPANSION", "appliance_artifact", artifact.ID, err.Error())
	}
	if err := validateApplianceCapacity(request.GetApprovedGrant(), controllerCapacity(server.controller)); err != nil {
		return nil, resourceError(codes.ResourceExhausted, "RESOURCE_LEDGER_EXHAUSTED", "appliance_installation", request.GetInstallationId(), err.Error())
	}
	return &appliancev1.AppliancePlanResponse{Plan: &appliancev1.AppliancePlan{InstallationId: request.GetInstallationId(),
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_INSTALL, ArtifactId: artifact.ID}}, nil
}

func (server *applianceAdminServer) PlanActivation(ctx context.Context, request *appliancev1.PlanActivationRequest) (*appliancev1.AppliancePlanResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	installation, _, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	return &appliancev1.AppliancePlanResponse{Plan: &appliancev1.AppliancePlan{InstallationId: installation.ID,
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_ACTIVATE, Revision: installation.Revision,
		ArtifactId: installation.ArtifactID}}, nil
}

func (server *applianceAdminServer) Install(ctx context.Context, request *appliancev1.InstallRequest) (*appliancev1.ApplianceOperationResponse, error) {
	defer clearSecret(request.OpaqueBootstrapSeed)
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if err := validateApplianceMutation(mutation); err != nil {
		return nil, err
	}
	if request.GetExpectedRevision() != 0 || request.GetArtifactId() == "" {
		return nil, resourceError(codes.Aborted, "PLAN_REVISION_CONFLICT", "appliance_installation", request.GetInstallationId(), "install plan is stale")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_INSTALL); err != nil {
		return nil, err
	} else if found {
		return applianceOperationResponse(existing), nil
	}
	artifact, err := server.controller.store.GetApplianceArtifact(ctx, request.GetArtifactId())
	if err != nil || !artifact.Verified {
		return nil, resourceError(codes.FailedPrecondition, "ARTIFACT_ID_INVALID", "appliance_artifact", request.GetArtifactId(), "verified artifact is unavailable")
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "GRANT_EXPANSION", "appliance_artifact", artifact.ID, "approved grant exceeds the signed manifest")
	}
	if err := validateApprovedGrant(manifest, request.GetApprovedGrant()); err != nil {
		return nil, resourceError(codes.FailedPrecondition, "GRANT_EXPANSION", "appliance_artifact", artifact.ID, err.Error())
	}
	if err := validateApplianceCapacity(request.GetApprovedGrant(), controllerCapacity(server.controller)); err != nil {
		return nil, resourceError(codes.ResourceExhausted, "RESOURCE_LEDGER_EXHAUSTED", "appliance_installation", request.GetInstallationId(), err.Error())
	}
	volumeID, err := randomID()
	if err != nil {
		return nil, err
	}
	volumeIdentity, err := newLUKSIdentity()
	if err != nil {
		return nil, err
	}
	bootstrapID, err := randomID()
	if err != nil {
		return nil, err
	}
	registrationID, err := randomID()
	if err != nil {
		return nil, err
	}
	operationID, err := randomID()
	if err != nil {
		return nil, err
	}
	installation := store.ApplianceInstallation{ID: request.GetInstallationId(), ProductID: artifact.ProductID,
		PublisherKeyID: artifact.PublisherKeyID, ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED),
		Revision: 1, EncryptedVolumeID: volumeID, EncryptedVolumeIdentity: volumeIdentity,
		DataVolumeBytes: manifest.GetDataVolumeBytes(), BootstrapID: bootstrapID}
	grant := grantFromProto(installation.ID, 1, request.GetApprovedGrant())
	if server.controller.applianceSecrets == nil {
		return nil, errors.New("appliance protected store is unavailable")
	}
	volumeKey, err := server.controller.applianceSecrets.EnsureVolumeKey(ctx, installation.ID)
	if err != nil {
		return nil, err
	}
	clearSecret(volumeKey)
	if err := server.controller.applianceSecrets.StoreBootstrap(ctx, installation.ID, bootstrapID, request.GetOpaqueBootstrapSeed()); err != nil {
		return nil, err
	}
	keepBootstrap := false
	defer func() {
		if !keepBootstrap {
			_ = server.controller.applianceSecrets.DeleteInstallation(context.Background(), installation.ID)
		}
	}()
	operation := store.ApplianceOperation{ID: operationID, InstallationID: installation.ID, RequestID: mutation.requestID,
		Kind:             int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_INSTALL),
		State:            int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason:   int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED),
		ExpectedRevision: 0, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: "installed"}
	_, _, operation, err = server.controller.store.InstallApplianceDomain(ctx, installation, grant,
		store.ResourceReservation{ID: "system-" + installation.ID, OwnerKind: store.ReservationOwnerAppliance,
			OwnerID: installation.ID, ResourceKind: store.ReservationResourceSystem, ResourceID: installation.ID, Amount: grant.System},
		controllerCapacity(server.controller), remoteRegistrationFromGrant(registrationID, installation, grant),
		frontendsFromProto(request.GetApprovedGrant().GetFrontends()), operation)
	if err != nil {
		if errors.Is(err, store.ErrResourceCapacityExceeded) || errors.Is(err, store.ErrDomainCapacityExceeded) {
			return nil, resourceError(codes.ResourceExhausted, "RESOURCE_LEDGER_EXHAUSTED", "appliance_installation", installation.ID, err.Error())
		}
		return nil, err
	}
	keepBootstrap = true
	return applianceOperationResponse(operation), nil
}
