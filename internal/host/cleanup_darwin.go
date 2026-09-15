//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cleanupPlanMaxInventory = 100
	cleanupPlanReadTimeout  = 30 * time.Second
	cleanupExecutionTimeout = 2 * time.Minute
	cleanupTerminalTimeout  = 5 * time.Second
)

var errCleanupTerminalPersistence = errors.New("cleanup terminal persistence failed")

func (server *operatorServer) PlanResetEnvironment(ctx context.Context, request *hostv1.PlanEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	plan, err := server.planCleanup(ctx, request.GetEnvironmentId(), hostv1.CleanupKind_CLEANUP_KIND_RESET)
	if err != nil {
		return nil, err
	}
	return &hostv1.CleanupPlanResponse{Plan: plan}, nil
}

func (server *operatorServer) PlanDiscardEnvironment(ctx context.Context, request *hostv1.PlanEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	plan, err := server.planCleanup(ctx, request.GetEnvironmentId(), hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
	if err != nil {
		return nil, err
	}
	return &hostv1.CleanupPlanResponse{Plan: plan}, nil
}

func (server *operatorServer) planCleanup(ctx context.Context, environmentID string, kind hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
	return server.planCleanupWithRemoteOwner(ctx, environmentID, kind, false)
}

func (server *operatorServer) planCleanupWithRemoteOwner(ctx context.Context, environmentID string, kind hostv1.CleanupKind, remoteOwner bool) (*hostv1.CleanupPlan, error) {
	ctx, cancel := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancel()
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", environmentID, "agent environment does not exist")
	}
	if !remoteOwner && environment.OwnerKind == store.OwnerRemoteRegistration && kind != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return nil, resourceError(codes.FailedPrecondition, "REMOTE_ALLOCATION_CONTROLLER_OWNED", "environment", environmentID, "active controller-owned allocation must be reset through its controller")
	}
	if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && !remoteOwner {
		if err := server.validateEnvironmentDiscard(ctx, environmentID); err != nil {
			return nil, err
		}
	}
	if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		if err := server.controller.reconcileEnvironment(ctx, environment); err != nil {
			return nil, err
		}
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID, Limit: cleanupPlanMaxInventory + 1})
	if err != nil {
		if status.Code(err) == codes.ResourceExhausted {
			return nil, cleanupInventoryLimitError(environmentID)
		}
		if isMissingNamespaceDiscard(kind, err) {
			return server.missingNamespaceCleanupPlan(ctx, environment)
		}
		return nil, err
	}
	if len(containers.GetContainers()) > cleanupPlanMaxInventory {
		return nil, cleanupInventoryLimitError(environmentID)
	}
	remaining := cleanupPlanMaxInventory - len(containers.GetContainers())
	volumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID, Limit: uint32(remaining + 1), RelatedContainerLimit: cleanupPlanMaxInventory + 1})
	if err != nil {
		if status.Code(err) == codes.ResourceExhausted {
			return nil, cleanupInventoryLimitError(environmentID)
		}
		if isMissingNamespaceDiscard(kind, err) {
			return server.missingNamespaceCleanupPlan(ctx, environment)
		}
		return nil, err
	}
	if len(containers.GetContainers())+len(volumes.GetVolumes()) > cleanupPlanMaxInventory {
		return nil, cleanupInventoryLimitError(environmentID)
	}
	remaining -= len(volumes.GetVolumes())
	images, err := client.ListImages(ctx, &vmmdv1.ListImagesRequest{ActorNamespaceId: environment.NamespaceID, Limit: uint32(remaining + 1)})
	if err != nil {
		if status.Code(err) == codes.ResourceExhausted {
			return nil, cleanupInventoryLimitError(environmentID)
		}
		if isMissingNamespaceDiscard(kind, err) {
			return server.missingNamespaceCleanupPlan(ctx, environment)
		}
		return nil, err
	}
	used := len(containers.GetContainers()) + len(volumes.GetVolumes()) + len(images.GetImages())
	if used > cleanupPlanMaxInventory {
		return nil, cleanupInventoryLimitError(environmentID)
	}
	grants, err := server.controller.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory-used)
	if err != nil {
		return nil, err
	}
	if used+len(grants) > cleanupPlanMaxInventory || kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && used+len(grants)+1 > cleanupPlanMaxInventory {
		return nil, cleanupInventoryLimitError(environmentID)
	}
	plan := &hostv1.CleanupPlan{EnvironmentId: environmentID, Kind: kind}
	for _, grant := range grants {
		if grant.Revoked {
			continue
		}
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "grant", grant.ID, "access will be revoked", false))
	}
	for _, container := range containers.GetContainers() {
		impact := cleanupImpact(environmentID, "container", container.GetId(), "writable snapshot and logs will be deleted", true)
		impact.Resource.GenerationId = container.GetGenerationId()
		plan.Impacts = append(plan.Impacts, impact)
	}
	for _, volume := range volumes.GetVolumes() {
		effect := "named volume will be retained"
		irreversible := false
		if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
			effect = "named volume and all contents will be deleted"
			irreversible = true
		}
		impact := cleanupImpact(environmentID, "volume", volume.GetId(), effect, irreversible)
		impact.Resource.GenerationId = volume.GetGenerationId()
		plan.Impacts = append(plan.Impacts, impact)
	}
	if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		for _, image := range images.GetImages() {
			plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "image", image.GetReference(), "namespace-scoped image record will be deleted; shared content is retained while referenced", true))
		}
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "environment", environmentID, "namespace and host policy will be deleted", true))
	}
	plan.Revision = cleanupInventoryRevision(kind, environment.PolicyRevision, environment.NamespaceID, grants, containers.GetContainers(), volumes.GetVolumes(), images.GetImages())
	return plan, nil
}

func cleanupInventoryLimitError(environmentID string) error {
	return resourceError(codes.ResourceExhausted, "CLEANUP_INVENTORY_LIMIT", "environment", environmentID, "cleanup inventory exceeds the 100-object owner work bound")
}

func cleanupInventoryRevision(kind hostv1.CleanupKind, policyRevision uint64, namespaceID string, grants []store.Grant, containers []*vmmdv1.Container, volumes []*vmmdv1.Volume, images []*vmmdv1.Image) uint64 {
	parts := []string{kind.String(), strconv.FormatUint(policyRevision, 10)}
	for _, grant := range grants {
		if grant.SourceNamespaceID == namespaceID || grant.TargetNamespaceID == namespaceID {
			parts = append(parts, "grant:"+grant.ID+":"+strconv.FormatBool(grant.Revoked)+":"+grant.Payload)
		}
	}
	for _, container := range containers {
		parts = append(parts, "container:"+container.GetId()+":"+container.GetGenerationId())
	}
	for _, volume := range volumes {
		parts = append(parts, "volume:"+volume.GetId()+":"+volume.GetGenerationId())
	}
	for _, image := range images {
		parts = append(parts, "image:"+image.GetReference()+":"+image.GetGenerationId()+":"+image.GetDigest())
	}
	sort.Strings(parts[2:])
	hash := fnv.New64a()
	for _, part := range parts {
		writeRevisionPart(hash, part)
	}
	return cleanupRevision(hash.Sum64())
}

func (server *operatorServer) missingNamespaceCleanupPlan(ctx context.Context, environment store.Environment) (*hostv1.CleanupPlan, error) {
	grants, err := server.controller.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory-1)
	if err != nil {
		return nil, err
	}
	if len(grants)+1 > cleanupPlanMaxInventory {
		return nil, cleanupInventoryLimitError(environment.ID)
	}
	return missingNamespaceCleanupPlan(environment, grants), nil
}

func missingNamespaceCleanupPlan(environment store.Environment, grants []store.Grant) *hostv1.CleanupPlan {
	hash := fnv.New64a()
	writeRevisionPart(hash, hostv1.CleanupKind_CLEANUP_KIND_DISCARD.String())
	writeRevisionPart(hash, strconv.FormatUint(environment.PolicyRevision, 10))
	writeRevisionPart(hash, "namespace:missing")
	parts := make([]string, 0, len(grants))
	impacts := make([]*hostv1.CleanupImpact, 0, len(grants)+1)
	for _, grant := range grants {
		parts = append(parts, "grant:"+grant.ID+":"+strconv.FormatBool(grant.Revoked)+":"+grant.Payload)
		if !grant.Revoked {
			impacts = append(impacts, cleanupImpact(environment.ID, "grant", grant.ID, "access will be revoked", false))
		} else if grant.Kind == "network-link" {
			impacts = append(impacts, cleanupImpact(environment.ID, "grant", grant.ID, "revoked network access will be removed from Guest and Host", false))
		}
	}
	sort.Strings(parts)
	for _, part := range parts {
		writeRevisionPart(hash, part)
	}
	impact := cleanupImpact(environment.ID, "environment", environment.ID, "remaining host policy and retained metadata will be deleted", true)
	impact.Resource.GenerationId = "namespace-missing"
	impacts = append(impacts, impact)
	return &hostv1.CleanupPlan{
		EnvironmentId: environment.ID,
		Kind:          hostv1.CleanupKind_CLEANUP_KIND_DISCARD,
		Revision:      cleanupRevision(hash.Sum64()),
		Impacts:       impacts,
	}
}

func isMissingNamespaceDiscard(kind hostv1.CleanupKind, err error) bool {
	return kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound
}

func cleanupRevision(value uint64) uint64 {
	value &= uint64(^uint64(0) >> 1)
	if value == 0 {
		return 1
	}
	return value
}

func writeRevisionPart(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(value))
	_, _ = hash.Write([]byte{0})
}

func cleanupImpact(environmentID, kind, resourceID, effect string, irreversible bool) *hostv1.CleanupImpact {
	return &hostv1.CleanupImpact{Resource: &hostv1.ResourceReference{Kind: kind, EnvironmentId: environmentID, ResourceId: resourceID}, Effect: effect, Irreversible: irreversible}
}

func (server *operatorServer) ExecuteCleanup(ctx context.Context, request *hostv1.ExecuteCleanupRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetPlan() == nil || request.GetPlan().GetRevision() == 0 {
		return nil, invalidField("request_id/plan", "request ID and revision-bound plan are required")
	}
	if err := validateCleanupKind(request.GetPlan().GetKind()); err != nil {
		return nil, err
	}
	operation, err := server.executeCleanup(ctx, request.GetRequestId(), request.GetPlan(), request.GetConfirmedVolumeIds(), "")
	if err != nil {
		return nil, err
	}
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) executeCleanup(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string) (operation store.Operation, err error) {
	return server.executeCleanupWithVolumeAuthorization(ctx, requestID, requested, confirmedVolumes, currentSessionID, false)
}

func (server *operatorServer) executeAgentCleanup(ctx context.Context, requestID, environmentID string, kind hostv1.CleanupKind, revision uint64, currentSessionID string) (operation store.Operation, err error) {
	requested := &hostv1.CleanupPlan{EnvironmentId: environmentID, Kind: kind, Revision: revision}
	return server.executeCleanupWithVolumeAuthorization(ctx, requestID, requested, nil, currentSessionID, kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
}

func (server *operatorServer) executeCleanupWithVolumeAuthorization(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes bool) (operation store.Operation, err error) {
	ctx, cancel := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancel()
	releaseInventory, err := server.controller.lockInventoryTransition(ctx)
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer releaseAdmission()
	return server.executeCleanupAdmissionLocked(ctx, requestID, requested, confirmedVolumes, currentSessionID, authorizeCurrentVolumes)
}

func (server *operatorServer) executeCleanupAdmissionLocked(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes bool) (operation store.Operation, err error) {
	return server.executeCleanupAdmissionLockedWithFence(ctx, requestID, requested, confirmedVolumes, currentSessionID, authorizeCurrentVolumes, false, nil)
}

func (server *operatorServer) executeCleanupAdmissionLockedWithFence(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, authorizeCurrentVolumes, remoteOwner bool, beforeCleanup func(context.Context) error) (operation store.Operation, err error) {
	if err := validateCleanupKind(requested.GetKind()); err != nil {
		return operation, err
	}
	unlock, err := server.controller.lockEnvironmentContext(ctx, requested.GetEnvironmentId())
	if err != nil {
		return server.reconcileCleanupAdmissionFailure(err, requestID, requested)
	}
	defer unlock()
	prior, err := server.controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return operation, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", prior.ID, "request ID belongs to a different operation")
		}
		if prior.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			prior.State = int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED)
			prior.Stage = "interrupted"
			prior.ErrorReason = "CLEANUP_TERMINAL_RESULT_UNKNOWN"
			prior.ErrorMessage = "the accepted cleanup did not save a terminal result; request a new cleanup plan"
			repairCtx, cancelRepair := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
			err := server.controller.store.UpdateOperation(repairCtx, prior)
			cancelRepair()
			if err != nil {
				return operation, resourceErrorWithMetadata(codes.Unavailable, "CLEANUP_TERMINAL_PERSIST_FAILED", "operation", prior.ID,
					"cleanup replay could not mark the orphaned operation interrupted",
					map[string]string{"operation_id": prior.ID, "request_id": prior.RequestID})
			}
		}
		return prior, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		if ctx.Err() != nil {
			return server.reconcileCleanupAdmissionFailure(ctx.Err(), requestID, requested)
		}
		return operation, err
	}
	current, err := server.planCleanupWithRemoteOwner(ctx, requested.GetEnvironmentId(), requested.GetKind(), remoteOwner)
	if err != nil {
		if ctx.Err() != nil {
			return operation, cleanupAdmissionError(ctx.Err(), requested.GetEnvironmentId())
		}
		return operation, err
	}
	if current.GetRevision() != requested.GetRevision() {
		return operation, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", requested.GetEnvironmentId(), "environment resources changed; request a new cleanup plan")
	}
	if requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		expected := make([]string, 0, len(confirmedVolumes))
		for _, impact := range current.GetImpacts() {
			if impact.GetResource().GetKind() == "volume" {
				expected = append(expected, impact.GetResource().GetResourceId())
			}
		}
		sort.Strings(expected)
		confirmed := append([]string(nil), confirmedVolumes...)
		if authorizeCurrentVolumes {
			confirmed = append([]string(nil), expected...)
		}
		sort.Strings(confirmed)
		if !equalStrings(expected, confirmed) {
			return operation, resourceError(codes.FailedPrecondition, "VOLUME_CONFIRMATION_REQUIRED", "environment", requested.GetEnvironmentId(), "the exact current named volume set must be explicitly confirmed")
		}
	}
	if err := ctx.Err(); err != nil {
		return operation, cleanupAdmissionError(err, requested.GetEnvironmentId())
	}
	operation, existing, err := server.controller.beginOperation(ctx, "environment-"+requested.GetKind().String(), requested.GetEnvironmentId(), requestID)
	if err != nil {
		if ctx.Err() != nil {
			return operation, cleanupAdmissionError(ctx.Err(), requested.GetEnvironmentId())
		}
		return operation, err
	}
	if existing {
		return operation, nil
	}
	accepted, executionErr := server.executeAcceptedCleanup(operation, current, currentSessionID, beforeCleanup)
	if errors.Is(executionErr, errCleanupTerminalPersistence) {
		return accepted, resourceErrorWithMetadata(codes.Unavailable, "CLEANUP_TERMINAL_PERSIST_FAILED", "operation", accepted.ID,
			"cleanup was accepted but its terminal result could not be saved",
			map[string]string{"operation_id": accepted.ID, "request_id": accepted.RequestID})
	}
	return accepted, executionErr
}

func (server *operatorServer) reconcileCleanupAdmissionFailure(admissionErr error, requestID string, requested *hostv1.CleanupPlan) (store.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
	defer cancel()
	prior, err := server.controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return store.Operation{}, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", prior.ID, "request ID belongs to a different operation")
		}
		return prior, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return store.Operation{}, cleanupAdmissionError(admissionErr, requested.GetEnvironmentId())
	}
	return store.Operation{}, resourceErrorWithMetadata(codes.Unavailable, "CLEANUP_ADMISSION_OUTCOME_UNKNOWN", "environment", requested.GetEnvironmentId(),
		"cleanup admission timed out and the request result could not be read",
		map[string]string{"request_id": requestID})
}

func (server *operatorServer) executeAcceptedCleanup(operation store.Operation, plan *hostv1.CleanupPlan, currentSessionID string, beforeCleanup func(context.Context) error) (store.Operation, error) {
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), cleanupExecutionTimeout)
	defer cancelExecution()
	if beforeCleanup != nil {
		if err := beforeCleanup(executionCtx); err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	if err := server.controller.revokeEnvironmentSessions(executionCtx, plan.GetEnvironmentId(), currentSessionID); err != nil {
		return operation, server.failCleanupOperation(&operation, err)
	}
	if cleanupPlanHasMissingNamespace(plan) {
		finished, err := server.finishDiscardAfterMissingNamespace(executionCtx, operation, plan, currentSessionID)
		if err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
		return finished, nil
	}
	if err := server.performCleanup(executionCtx, &operation, plan); err != nil {
		return operation, server.failCleanupOperation(&operation, err)
	}
	return operation, server.completeCleanupOperation(&operation)
}

func cleanupAdmissionError(err error, environmentID string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return resourceError(codes.DeadlineExceeded, "CLEANUP_ADMISSION_TIMEOUT", "environment", environmentID, "cleanup admission timed out before an operation was accepted")
	}
	if errors.Is(err, context.Canceled) {
		return resourceError(codes.Canceled, "CLEANUP_ADMISSION_CANCELED", "environment", environmentID, "cleanup admission was canceled before an operation was accepted")
	}
	return err
}

func (server *operatorServer) failCleanupOperation(operation *store.Operation, failure error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
	defer cancel()
	if err := server.controller.failOperation(ctx, operation, "CLEANUP_FAILED", failure); err != nil {
		return errors.Join(errCleanupTerminalPersistence, failure, fmt.Errorf("persist cleanup failure: %w", err))
	}
	return nil
}

func (server *operatorServer) completeCleanupOperation(operation *store.Operation) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTerminalTimeout)
	defer cancel()
	if err := server.controller.completeOperation(ctx, operation, "complete"); err != nil {
		return errors.Join(errCleanupTerminalPersistence, fmt.Errorf("persist cleanup completion: %w", err))
	}
	return nil
}

func cleanupPlanHasMissingNamespace(plan *hostv1.CleanupPlan) bool {
	if plan.GetKind() != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return false
	}
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() == "environment" && impact.GetResource().GetGenerationId() == "namespace-missing" {
			return true
		}
	}
	return false
}

func (server *operatorServer) finishDiscardAfterMissingNamespace(ctx context.Context, operation store.Operation, plan *hostv1.CleanupPlan, currentSessionID string) (store.Operation, error) {
	environmentID := plan.GetEnvironmentId()
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return operation, err
	}
	if err == nil {
		if err := server.controller.revokeEnvironmentSessions(ctx, environmentID, currentSessionID); err != nil {
			return operation, err
		}
		grants, grantErr := server.controller.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory)
		if grantErr != nil {
			return operation, grantErr
		}
		if len(grants) > cleanupPlanMaxInventory {
			return operation, cleanupInventoryLimitError(environmentID)
		}
		var disclosed, actual []string
		for _, impact := range plan.GetImpacts() {
			if impact.GetResource().GetKind() == "grant" {
				disclosed = append(disclosed, impact.GetResource().GetResourceId())
			}
		}
		for _, grant := range grants {
			if !grant.Revoked || grant.Kind == "network-link" {
				actual = append(actual, grant.ID)
			}
		}
		sort.Strings(disclosed)
		sort.Strings(actual)
		if !equalStrings(disclosed, actual) {
			return operation, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", environmentID, "grant inventory changed; request a new cleanup plan")
		}
		if err := server.controller.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
			return operation, err
		}
		var networkGrants []store.Grant
		for _, grant := range grants {
			if grant.Kind == "network-link" && (grant.SourceNamespaceID == environment.NamespaceID || grant.TargetNamespaceID == environment.NamespaceID) {
				networkGrants = append(networkGrants, grant)
			}
		}
		if len(networkGrants) > 0 {
			if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
				return operation, errors.New("VM must be running to settle missing-namespace network links")
			}
			client, connection, guestErr := server.controller.guestClient(ctx)
			if guestErr != nil {
				return operation, guestErr
			}
			for _, grant := range networkGrants {
				_, deleteErr := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID(operation.RequestID + "-missing-link-" + grant.ID), LinkId: grant.ID})
				if deleteErr != nil && status.Code(deleteErr) != codes.NotFound {
					connection.Close()
					return operation, fmt.Errorf("delete missing-namespace network link %q: %w", grant.ID, deleteErr)
				}
				if compactErr := server.controller.store.CompactRevokedGrant(ctx, grant.ID); compactErr != nil {
					connection.Close()
					return operation, fmt.Errorf("compact missing-namespace network link %q: %w", grant.ID, compactErr)
				}
			}
			connection.Close()
		}
	}
	operation.State = int32(hostv1.OperationState_OPERATION_STATE_RUNNING)
	operation.Stage = "retrying-host-cleanup"
	operation.ErrorReason = ""
	operation.ErrorMessage = ""
	if err := server.controller.store.UpdateOperation(ctx, operation); err != nil {
		return operation, err
	}
	if err == nil {
		if err := server.controller.store.DiscardEnvironment(ctx, environmentID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	server.controller.forgetContainers(environmentID)
	if server.controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if err := server.controller.publishSourcePoliciesLocked(ctx, ""); err != nil {
			return operation, server.failCleanupOperation(&operation, err)
		}
	}
	return operation, server.completeCleanupOperation(&operation)
}

func (server *operatorServer) performCleanup(ctx context.Context, operation *store.Operation, plan *hostv1.CleanupPlan) error {
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	environment, err := server.controller.store.GetEnvironment(ctx, plan.GetEnvironmentId())
	if err != nil {
		return fmt.Errorf("load cleanup environment: %w", err)
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return fmt.Errorf("connect cleanup runtime: %w", err)
	}
	defer connection.Close()
	grants, err := server.controller.store.GrantsForNamespace(ctx, environment.NamespaceID, cleanupPlanMaxInventory)
	if err != nil {
		return fmt.Errorf("list cleanup grants: %w", err)
	}
	if len(grants) > cleanupPlanMaxInventory {
		return cleanupInventoryLimitError(environment.ID)
	}
	if err := server.controller.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
		return fmt.Errorf("revoke cleanup grants: %w", err)
	}
	for _, grant := range grants {
		if grant.Kind != "network-link" || grant.SourceNamespaceID != environment.NamespaceID && grant.TargetNamespaceID != environment.NamespaceID {
			continue
		}
		if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID(operation.RequestID + "-link-" + grant.ID), LinkId: grant.ID}); err != nil {
			if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
				// The desired Guest state is already present.
			} else {
				return fmt.Errorf("delete cleanup network link %q: %w", grant.ID, err)
			}
		}
		if err := server.controller.store.CompactRevokedGrant(ctx, grant.ID); err != nil {
			return fmt.Errorf("compact cleanup network link %q: %w", grant.ID, err)
		}
	}
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() != "container" {
			continue
		}
		containerID := impact.GetResource().GetResourceId()
		inspected, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: containerID})
		if err != nil {
			return fmt.Errorf("inspect cleanup container %q: %w", containerID, err)
		}
		container := inspected.GetContainer()
		if container.GetGenerationId() == "" || container.GetGenerationId() != impact.GetResource().GetGenerationId() {
			return resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "container", containerID, "container generation changed; request a new cleanup plan")
		}
		if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING || container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
			_, err := server.controller.stopWorkloadContainerLocked(ctx, client, environment.ID, &vmmdv1.StopContainerRequest{RequestId: derivedRequestID(operation.RequestID + "-stop-" + containerID), ActorNamespaceId: environment.NamespaceID, ContainerId: containerID})
			if err != nil {
				return fmt.Errorf("stop cleanup container %q: %w", containerID, err)
			}
		}
		if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: derivedRequestID(operation.RequestID + "-delete-" + containerID), ActorNamespaceId: environment.NamespaceID, ContainerId: containerID, ExpectedGenerationId: container.GetGenerationId()}); err != nil {
			return fmt.Errorf("delete cleanup container %q: %w", containerID, err)
		}
	}
	if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		return nil
	}
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() != "volume" {
			continue
		}
		volumeID := impact.GetResource().GetResourceId()
		if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: derivedRequestID(operation.RequestID + "-volume-" + volumeID), ActorNamespaceId: environment.NamespaceID, VolumeId: volumeID, ExpectedGenerationId: impact.GetResource().GetGenerationId()}); err != nil {
			return fmt.Errorf("delete cleanup volume %q: %w", volumeID, err)
		}
	}
	remainingContainers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID, Limit: 1})
	if err != nil {
		return fmt.Errorf("verify cleanup containers: %w", err)
	}
	if len(remainingContainers.GetContainers()) != 0 {
		return fmt.Errorf("verify cleanup containers: %d container records remain", len(remainingContainers.GetContainers()))
	}
	remainingVolumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID, Limit: 1, RelatedContainerLimit: 1})
	if err != nil {
		return fmt.Errorf("verify cleanup volumes: %w", err)
	}
	if len(remainingVolumes.GetVolumes()) != 0 {
		return fmt.Errorf("verify cleanup volumes: %d volume records remain", len(remainingVolumes.GetVolumes()))
	}
	if _, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: derivedRequestID(operation.RequestID + "-namespace"), NamespaceId: environment.NamespaceID}); err != nil {
		return fmt.Errorf("delete cleanup namespace: %w", err)
	}
	if err := server.controller.store.DiscardEnvironment(ctx, environment.ID); err != nil {
		return fmt.Errorf("discard cleanup host environment: %w", err)
	}
	server.controller.forgetContainers(environment.ID)
	if err := server.controller.publishSourcePoliciesLocked(ctx, ""); err != nil {
		return fmt.Errorf("publish cleanup source policies: %w", err)
	}
	return nil
}

func (server *operatorServer) validateEnvironmentDiscard(ctx context.Context, environmentID string) error {
	if err := server.controller.store.ValidateEnvironmentDiscard(ctx, environmentID); err != nil {
		if errors.Is(err, store.ErrEnvironmentHasAgentAccess) {
			return resourceError(codes.FailedPrecondition, "AGENT_ACCESS_ACTIVE", "environment", environmentID,
				"remove agent access before deleting the environment")
		}
		if errors.Is(err, store.ErrRemoteEnvironmentActive) {
			return resourceError(codes.FailedPrecondition, "REMOTE_ALLOCATION_ACTIVE", "environment", environmentID,
				"release the remote allocation before deleting the environment")
		}
		return err
	}
	return nil
}

func validateCleanupKind(kind hostv1.CleanupKind) error {
	if kind != hostv1.CleanupKind_CLEANUP_KIND_RESET && kind != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		return invalidField("plan.kind", "cleanup kind must be reset or discard")
	}
	return nil
}

func (controller *Controller) revokeEnvironmentSessions(ctx context.Context, environmentID, exceptID string) error {
	controller.mu.Lock()
	ids := make([]string, 0, len(controller.sessions))
	for id := range controller.sessions {
		ids = append(ids, id)
	}
	controller.mu.Unlock()
	for _, id := range ids {
		if id == exceptID {
			continue
		}
		session, err := controller.store.GetSession(ctx, id)
		if err != nil {
			continue
		}
		if session.EnvironmentID == environmentID {
			if err := controller.revokeSession(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (server *agentServer) PlanResetEnvironment(ctx context.Context, _ *hostv1.GetOwnEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_RESET)
	if err != nil {
		return nil, err
	}
	return &hostv1.CleanupPlanResponse{Plan: plan}, nil
}

func (server *agentServer) PlanDiscardEnvironment(ctx context.Context, _ *hostv1.GetOwnEnvironmentRequest) (*hostv1.CleanupPlanResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
	if err != nil {
		return nil, err
	}
	return &hostv1.CleanupPlanResponse{Plan: plan}, nil
}

func (server *agentServer) RequestReset(ctx context.Context, request *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	operation, err := (&operatorServer{controller: server.controller}).executeAgentCleanup(ctx, request.GetRequestId(), identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_RESET, request.GetPlanRevision(), identity.SessionID)
	if err != nil {
		return nil, err
	}
	go server.controller.revokeSessionAfterResponse(identity.SessionID, false)
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *agentServer) RequestDiscard(ctx context.Context, request *hostv1.EnvironmentOperationRequest) (*hostv1.OperationResponse, error) {
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	operation, err := (&operatorServer{controller: server.controller}).executeAgentCleanup(ctx, request.GetRequestId(), identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD, request.GetPlanRevision(), identity.SessionID)
	if err != nil {
		return nil, err
	}
	go server.controller.revokeSessionAfterResponse(identity.SessionID, true)
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}
