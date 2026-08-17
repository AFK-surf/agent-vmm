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

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
			return missingNamespaceCleanupPlan(environment), nil
		}
		return nil, err
	}
	volumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return nil, err
	}
	images, err := client.ListImages(ctx, &vmmdv1.ListImagesRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return nil, err
	}
	grants, err := server.controller.store.ActiveGrants(ctx)
	if err != nil {
		return nil, err
	}
	plan := &hostv1.CleanupPlan{EnvironmentId: environmentID, Kind: kind}
	for _, grant := range grants {
		if grant.SourceNamespaceID != environment.NamespaceID && grant.TargetNamespaceID != environment.NamespaceID {
			continue
		}
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "grant", grant.ID, "access will be revoked", false))
	}
	for _, container := range containers.GetContainers() {
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "container", container.GetId(), "writable snapshot and logs will be deleted", true))
	}
	for _, volume := range volumes.GetVolumes() {
		effect := "named volume will be retained"
		irreversible := false
		if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
			effect = "named volume and all contents will be deleted"
			irreversible = true
		}
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "volume", volume.GetId(), effect, irreversible))
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

func cleanupInventoryRevision(kind hostv1.CleanupKind, policyRevision uint64, namespaceID string, grants []store.Grant, containers []*vmmdv1.Container, volumes []*vmmdv1.Volume, images []*vmmdv1.Image) uint64 {
	parts := []string{kind.String(), strconv.FormatUint(policyRevision, 10)}
	for _, grant := range grants {
		if grant.SourceNamespaceID == namespaceID || grant.TargetNamespaceID == namespaceID {
			parts = append(parts, "grant:"+grant.ID+":"+grant.Payload)
		}
	}
	for _, container := range containers {
		parts = append(parts, "container:"+container.GetId())
	}
	for _, volume := range volumes {
		parts = append(parts, "volume:"+volume.GetId())
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

func missingNamespaceCleanupPlan(environment store.Environment) *hostv1.CleanupPlan {
	hash := fnv.New64a()
	writeRevisionPart(hash, hostv1.CleanupKind_CLEANUP_KIND_DISCARD.String())
	writeRevisionPart(hash, strconv.FormatUint(environment.PolicyRevision, 10))
	writeRevisionPart(hash, "namespace:missing")
	return &hostv1.CleanupPlan{
		EnvironmentId: environment.ID,
		Kind:          hostv1.CleanupKind_CLEANUP_KIND_DISCARD,
		Revision:      cleanupRevision(hash.Sum64()),
		Impacts: []*hostv1.CleanupImpact{
			cleanupImpact(environment.ID, "environment", environment.ID, "remaining host policy and retained metadata will be deleted", true),
		},
	}
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
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	return server.executeCleanupAdmissionLocked(ctx, requestID, requested, confirmedVolumes, currentSessionID)
}

func (server *operatorServer) executeCleanupAdmissionLocked(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string) (operation store.Operation, err error) {
	return server.executeCleanupAdmissionLockedWithFence(ctx, requestID, requested, confirmedVolumes, currentSessionID, false, nil)
}

func (server *operatorServer) executeCleanupAdmissionLockedWithFence(ctx context.Context, requestID string, requested *hostv1.CleanupPlan, confirmedVolumes []string, currentSessionID string, remoteOwner bool, beforeCleanup func() error) (operation store.Operation, err error) {
	if err := validateCleanupKind(requested.GetKind()); err != nil {
		return operation, err
	}
	unlock := server.controller.lockEnvironment(requested.GetEnvironmentId())
	defer unlock()
	existing := false
	prior, err := server.controller.store.OperationByRequestID(ctx, requestID)
	if err == nil {
		if prior.Kind != "environment-"+requested.GetKind().String() || prior.EnvironmentID != requested.GetEnvironmentId() {
			return operation, resourceError(codes.AlreadyExists, "REQUEST_ID_REUSED", "operation", prior.ID, "request ID belongs to a different operation")
		}
		if prior.State == int32(hostv1.OperationState_OPERATION_STATE_SUCCEEDED) || prior.State == int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
			return prior, nil
		}
		operation = prior
		existing = true
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return operation, err
	}
	current, err := server.planCleanupWithRemoteOwner(ctx, requested.GetEnvironmentId(), requested.GetKind(), remoteOwner)
	if err != nil {
		if existing && requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
			return server.finishDiscardAfterMissingNamespace(ctx, operation, requested.GetEnvironmentId(), currentSessionID)
		}
		return operation, err
	}
	if !existing && current.GetRevision() != requested.GetRevision() {
		return operation, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", requested.GetEnvironmentId(), "environment resources changed; request a new cleanup plan")
	}
	if requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		expected := make([]string, 0, len(confirmedVolumes))
		confirmationPlan := current
		if existing {
			confirmationPlan = requested
		}
		for _, impact := range confirmationPlan.GetImpacts() {
			if impact.GetResource().GetKind() == "volume" {
				expected = append(expected, impact.GetResource().GetResourceId())
			}
		}
		sort.Strings(expected)
		confirmed := append([]string(nil), confirmedVolumes...)
		sort.Strings(confirmed)
		if !equalStrings(expected, confirmed) {
			return operation, resourceError(codes.FailedPrecondition, "VOLUME_CONFIRMATION_REQUIRED", "environment", requested.GetEnvironmentId(), "the exact current named volume set must be explicitly confirmed")
		}
	}
	if beforeCleanup != nil {
		if err := beforeCleanup(); err != nil {
			return operation, err
		}
	}
	if err := server.controller.revokeEnvironmentSessions(ctx, requested.GetEnvironmentId(), currentSessionID); err != nil {
		return operation, err
	}
	if existing {
		operation.State = int32(hostv1.OperationState_OPERATION_STATE_RUNNING)
		operation.Stage = "retrying"
		operation.ErrorReason = ""
		operation.ErrorMessage = ""
		if err := server.controller.store.UpdateOperation(ctx, operation); err != nil {
			return operation, err
		}
	} else {
		operation, _, err = server.controller.beginOperation(ctx, "environment-"+requested.GetKind().String(), requested.GetEnvironmentId(), requestID)
		if err != nil {
			return operation, err
		}
	}
	if err := server.performCleanup(ctx, &operation, current); err != nil {
		server.controller.failOperation(ctx, &operation, "CLEANUP_FAILED", err)
		return operation, nil
	}
	server.controller.completeOperation(ctx, &operation, "complete")
	return operation, nil
}

func (server *operatorServer) finishDiscardAfterMissingNamespace(ctx context.Context, operation store.Operation, environmentID, currentSessionID string) (store.Operation, error) {
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
		if err := server.controller.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
			return operation, err
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
			server.controller.failOperation(ctx, &operation, "CLEANUP_FAILED", err)
			return operation, nil
		}
	}
	server.controller.forgetContainers(environmentID)
	if server.controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if err := server.controller.publishSourcePolicies(ctx); err != nil {
			server.controller.failOperation(ctx, &operation, "POLICY_RECONCILE_FAILED", err)
			return operation, nil
		}
	}
	server.controller.completeOperation(ctx, &operation, "complete")
	return operation, nil
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
	grants, err := server.controller.store.AllGrants(ctx)
	if err != nil {
		return fmt.Errorf("list cleanup grants: %w", err)
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
				continue
			}
			return fmt.Errorf("delete cleanup network link %q: %w", grant.ID, err)
		}
	}
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
			if err := server.controller.store.DiscardEnvironment(ctx, environment.ID); err != nil {
				return fmt.Errorf("discard cleanup host environment: %w", err)
			}
			server.controller.forgetContainers(environment.ID)
			if err := server.controller.publishSourcePolicies(ctx); err != nil {
				return fmt.Errorf("publish cleanup source policies: %w", err)
			}
			return nil
		}
		return fmt.Errorf("list cleanup containers: %w", err)
	}
	for _, container := range containers.GetContainers() {
		if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
			_, err := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: derivedRequestID(operation.RequestID + "-stop-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId()})
			if err != nil {
				return fmt.Errorf("stop cleanup container %q: %w", container.GetId(), err)
			}
		}
		if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: derivedRequestID(operation.RequestID + "-delete-" + container.GetId()), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), ExpectedGenerationId: container.GetGenerationId()}); err != nil {
			return fmt.Errorf("delete cleanup container %q: %w", container.GetId(), err)
		}
	}
	if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		return nil
	}
	volumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return fmt.Errorf("list cleanup volumes: %w", err)
	}
	for _, volume := range volumes.GetVolumes() {
		if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: derivedRequestID(operation.RequestID + "-volume-" + volume.GetId()), ActorNamespaceId: environment.NamespaceID, VolumeId: volume.GetId()}); err != nil {
			return fmt.Errorf("delete cleanup volume %q: %w", volume.GetId(), err)
		}
	}
	remainingContainers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return fmt.Errorf("verify cleanup containers: %w", err)
	}
	if len(remainingContainers.GetContainers()) != 0 {
		return fmt.Errorf("verify cleanup containers: %d container records remain", len(remainingContainers.GetContainers()))
	}
	remainingVolumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
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
	if err := server.controller.publishSourcePolicies(ctx); err != nil {
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
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_RESET)
	if err != nil {
		return nil, err
	}
	if plan.GetRevision() != request.GetPlanRevision() {
		return nil, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", identity.EnvironmentID, "environment resources changed")
	}
	operation, err := (&operatorServer{controller: server.controller}).executeCleanup(ctx, request.GetRequestId(), plan, nil, identity.SessionID)
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
	plan, err := (&operatorServer{controller: server.controller}).planCleanup(ctx, identity.EnvironmentID, hostv1.CleanupKind_CLEANUP_KIND_DISCARD)
	if err != nil {
		return nil, err
	}
	if plan.GetRevision() != request.GetPlanRevision() {
		return nil, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", identity.EnvironmentID, "environment resources changed")
	}
	var volumes []string
	for _, impact := range plan.GetImpacts() {
		if impact.GetResource().GetKind() == "volume" {
			volumes = append(volumes, impact.GetResource().GetResourceId())
		}
	}
	operation, err := (&operatorServer{controller: server.controller}).executeCleanup(ctx, request.GetRequestId(), plan, volumes, identity.SessionID)
	if err != nil {
		return nil, err
	}
	go server.controller.revokeSessionAfterResponse(identity.SessionID, true)
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}
