//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/mcptoken"
	"os"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (server *Owner) StageArtifact(ctx context.Context, request *appliancev1.StageArtifactRequest) (*appliancev1.StageArtifactResponse, error) {
	if request.GetRequestId() == "" {
		return nil, hostv1.InvalidField("request_id", "artifact staging identity is invalid")
	}
	lookup := func(ctx context.Context) (*appliancev1.StageArtifactResponse, bool, error) {
		existing, err := server.store.ApplianceOperationByRequest(ctx, request.GetRequestId())
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		if existing.InstallationID != "" || existing.DeadlineUnixMillis != request.GetDeadlineUnixMillis() {
			return nil, true, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "request ID was reused", hostv1.WithResource("appliance_artifact", ""))
		}
		response := &appliancev1.StageArtifactResponse{Operation: applianceOperationFromStore(existing)}
		response.ArtifactId = string(existing.Receipt)
		return response, true, nil
	}
	if response, found, err := lookup(ctx); found || err != nil {
		return response, err
	}
	if err := applianceAdmissionDeadline(applianceMutation{requestID: request.GetRequestId(), deadlineUnixMillis: request.GetDeadlineUnixMillis()}); err != nil {
		return nil, err
	}
	canonical, canonicalHash, err := (applianceArtifactVerifier{}).verify(request.GetManifest(), nil)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	admission, cancel := context.WithDeadline(ctx, time.UnixMilli(request.GetDeadlineUnixMillis()))
	defer cancel()
	contentLock := server.applianceContentStageLock()
	if err := contentLock.Lock(admission); err != nil {
		return nil, err
	}
	defer contentLock.Unlock()
	if response, found, err := lookup(ctx); found || err != nil {
		return response, err
	}
	if err := applianceAdmissionDeadline(applianceMutation{requestID: request.GetRequestId(), deadlineUnixMillis: request.GetDeadlineUnixMillis()}); err != nil {
		return nil, err
	}
	ctx = admission
	artifact, err := server.store.GetApplianceArtifactByCanonicalHash(ctx, canonicalHash)
	if errors.Is(err, sql.ErrNoRows) {
		controllerSource := false
		for _, content := range request.GetManifest().GetContentRefs() {
			controllerSource = controllerSource || content.GetRole() == appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER
		}
		if !controllerSource {
			return nil, status.Error(codes.FailedPrecondition, "new appliance artifact requires a controller content reference")
		}
		artifactID, idErr := opreceipt.NewID()
		if idErr != nil {
			return nil, idErr
		}
		artifact = store.ApplianceArtifact{ID: artifactID, CanonicalHash: append([]byte(nil), canonicalHash...),
			ImageDigest: request.GetManifest().GetImageManifestDigest(), ImageReference: request.GetManifest().GetImageReference(),
			Platform:  request.GetManifest().GetPlatform(),
			ProductID: request.GetManifest().GetProductId(), Release: request.GetManifest().GetRelease(),
			Manifest: canonical}
	} else if err != nil {
		return nil, err
	}
	operationID, err := opreceipt.NewID()
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
		artifact, operation, err = server.store.StageApplianceArtifact(ctx, artifact, operation)
	} else {
		operation, err = server.store.CreateApplianceOperation(ctx, operation)
	}
	if err != nil {
		return nil, err
	}
	response := &appliancev1.StageArtifactResponse{Operation: applianceOperationFromStore(operation)}
	response.ArtifactId = artifact.ID
	return response, nil
}

func (server *Owner) PlanInstall(ctx context.Context, request *appliancev1.PlanInstallRequest) (*appliancev1.PlanInstallResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	if err := validateApplianceMutation(mutation); err != nil {
		return nil, err
	}
	if request.GetExpectedRevision() != 0 || request.GetArtifactId() == "" {
		return nil, status.Error(codes.Aborted, "new installation must start at revision zero")
	}
	artifact, _, err := server.validateInstall(ctx, request)
	if err != nil {
		return nil, err
	}
	return &appliancev1.PlanInstallResponse{Plan: &appliancev1.AppliancePlan{InstallationId: request.GetInstallationId(),
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_INSTALL, ArtifactId: artifact.ID}}, nil
}

func (server *Owner) PlanActivation(ctx context.Context, request *appliancev1.PlanActivationRequest) (*appliancev1.PlanActivationResponse, error) {
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	installation, err := server.exactInstallation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	return &appliancev1.PlanActivationResponse{Plan: &appliancev1.AppliancePlan{InstallationId: installation.ID,
		Kind: appliancev1.AppliancePlanKind_APPLIANCE_PLAN_KIND_ACTIVATE, Revision: installation.Revision,
		ArtifactId: installation.ArtifactID}}, nil
}

func (server *Owner) Install(ctx context.Context, request *appliancev1.InstallRequest) (*appliancev1.InstallResponse, error) {
	defer func() { clear(request.OpaqueBootstrapSeed) }()
	mutation := applianceMutation{request.GetInstallationId(), request.GetRequestId(), request.GetExpectedRevision(), request.GetDeadlineUnixMillis()}
	if request.GetExpectedRevision() != 0 || request.GetArtifactId() == "" {
		return nil, status.Error(codes.Aborted, "install plan is stale")
	}
	unlock, err := server.applianceLocks.Lock(ctx, request.GetInstallationId())
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := server.applianceContentStage.RLock(ctx); err != nil {
		return nil, err
	}
	defer server.applianceContentStage.RUnlock()
	if existing, found, err := server.existingApplianceOperation(ctx, mutation, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_INSTALL); err != nil {
		return nil, err
	} else if found {
		return &appliancev1.InstallResponse{Operation: applianceOperationFromStore(existing)}, nil
	}
	if err := validateApplianceMutation(mutation); err != nil {
		return nil, err
	}
	artifact, manifest, err := server.validateInstall(ctx, request)
	if err != nil {
		return nil, err
	}
	if installationID := request.GetInstallationId(); installationID == mcptoken.InstallationID {
		key, err := server.applianceSecrets.LoadMCPTokenKey(ctx, installationID)
		if errors.Is(err, os.ErrNotExist) {
			key = make([]byte, mcptoken.KeySize)
			if _, err = rand.Read(key); err == nil {
				err = server.applianceSecrets.StoreMCPTokenKey(ctx, installationID, key)
			}
		}
		if err != nil {
			return nil, err
		}
		defer clear(key)
		request.OpaqueBootstrapSeed = append([]byte(nil), key...)
	}
	volumeID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	bootstrapID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	registrationID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	operationID, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	installation := store.ApplianceInstallation{ID: request.GetInstallationId(), ProductID: artifact.ProductID,
		ArtifactID: artifact.ID, State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED),
		Revision: 1, DataVolumeID: volumeID, DataVolumeBytes: manifest.GetDataVolumeBytes(), BootstrapID: bootstrapID}
	grant := grantFromProto(installation.ID, 1, request.GetApprovedGrant())
	if server.applianceSecrets == nil {
		return nil, errors.New("appliance protected store is unavailable")
	}
	if err := server.applianceSecrets.StoreBootstrap(ctx, installation.ID, bootstrapID, request.GetOpaqueBootstrapSeed()); err != nil {
		return nil, err
	}
	keepBootstrap := false
	defer func() {
		if !keepBootstrap {
			_ = server.applianceSecrets.DeleteBootstrap(context.Background(), installation.ID, bootstrapID)
		}
	}()
	operation := store.ApplianceOperation{ID: operationID, InstallationID: installation.ID, RequestID: mutation.requestID,
		Kind:             int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_INSTALL),
		State:            int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase:            int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL),
		TerminalReason:   int32(appliancev1.ApplianceTerminalReason_APPLIANCE_TERMINAL_REASON_SUCCEEDED),
		ExpectedRevision: 0, DeadlineUnixMillis: mutation.deadlineUnixMillis, Stage: "installed"}
	_, _, operation, err = server.store.InstallApplianceDomain(ctx, installation, grant,
		remoteRegistrationFromGrant(registrationID, installation, grant),
		frontendsFromProto(request.GetApprovedGrant().GetFrontends()), operation)
	if err != nil {
		var constraint interface{ Code() int }
		if errors.Is(err, store.ErrInstallationRevisionConflict) || (errors.As(err, &constraint) && constraint.Code()&0xff == 19) {
			return nil, status.Error(codes.Aborted, "installation facts changed before install committed")
		}
		return nil, err
	}
	keepBootstrap = true
	return &appliancev1.InstallResponse{Operation: applianceOperationFromStore(operation)}, nil
}

func (server *Owner) validateInstall(ctx context.Context, request interface {
	GetInstallationId() string
	GetArtifactId() string
	GetApprovedGrant() *appliancev1.ApprovedGrant
}) (store.ApplianceArtifact, *appliancev1.ControllerApplianceManifest, error) {
	if _, err := server.store.GetApplianceInstallation(ctx, request.GetInstallationId()); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return store.ApplianceArtifact{}, nil, err
		}
		return store.ApplianceArtifact{}, nil, status.Error(codes.AlreadyExists, "installation already exists")
	}
	artifact, err := server.store.GetApplianceArtifact(ctx, request.GetArtifactId())
	if err != nil {
		return store.ApplianceArtifact{}, nil, status.Error(codes.FailedPrecondition, "artifact is unavailable")
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return store.ApplianceArtifact{}, nil, status.Error(codes.FailedPrecondition, "approved grant exceeds the canonical manifest")
	}
	if err := validateApprovedGrant(manifest, request.GetApprovedGrant()); err != nil {
		return store.ApplianceArtifact{}, nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := validateApplianceIsolationLimits(request.GetApprovedGrant(), controllerSystemIsolationCapacity(server), controllerWorkloadIsolationCapacity(server)); err != nil {
		return store.ApplianceArtifact{}, nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	if err := server.requireApplianceContent(ctx, manifest); err != nil {
		return store.ApplianceArtifact{}, nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return artifact, manifest, nil
}
