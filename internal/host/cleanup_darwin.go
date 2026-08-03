//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
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
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", environmentID, "agent environment does not exist")
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return nil, err
	}
	volumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return nil, err
	}
	grants, err := server.controller.store.ActiveGrants(ctx)
	if err != nil {
		return nil, err
	}
	plan := &hostv1.CleanupPlan{EnvironmentId: environmentID, Kind: kind}
	hash := fnv.New64a()
	writeRevisionPart(hash, kind.String())
	writeRevisionPart(hash, strconv.FormatUint(environment.PolicyRevision, 10))
	for _, grant := range grants {
		if grant.SourceNamespaceID != environment.NamespaceID && grant.TargetNamespaceID != environment.NamespaceID {
			continue
		}
		writeRevisionPart(hash, "grant:"+grant.ID+":"+grant.Payload)
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "grant", grant.ID, "access will be revoked", false))
	}
	for _, container := range containers.GetContainers() {
		writeRevisionPart(hash, "container:"+container.GetId()+":"+container.GetState().String())
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "container", container.GetId(), "writable snapshot and logs will be deleted", true))
	}
	for _, volume := range volumes.GetVolumes() {
		writeRevisionPart(hash, "volume:"+volume.GetId()+":"+stringsKey(volume.GetMountedBy()))
		effect := "named volume will be retained"
		irreversible := false
		if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
			effect = "named volume and all contents will be deleted"
			irreversible = true
		}
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "volume", volume.GetId(), effect, irreversible))
	}
	if kind == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		plan.Impacts = append(plan.Impacts, cleanupImpact(environmentID, "environment", environmentID, "namespace and host policy will be deleted", true))
	}
	plan.Revision = hash.Sum64()
	return plan, nil
}

func writeRevisionPart(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(value))
	_, _ = hash.Write([]byte{0})
}

func stringsKey(values []string) string {
	copy := append([]string(nil), values...)
	sort.Strings(copy)
	result := ""
	for _, value := range copy {
		result += value + "\x00"
	}
	return result
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
	current, err := server.planCleanup(ctx, requested.GetEnvironmentId(), requested.GetKind())
	if err != nil {
		if existing && requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD && status.Code(err) == codes.NotFound {
			return server.finishDiscardAfterMissingNamespace(ctx, operation, requested.GetEnvironmentId(), currentSessionID)
		}
		return operation, err
	}
	if !existing && current.GetRevision() != requested.GetRevision() {
		return operation, resourceError(codes.Aborted, "CLEANUP_PLAN_STALE", "environment", requested.GetEnvironmentId(), "environment resources changed; request a new cleanup plan")
	}
	confirmed := make(map[string]bool, len(confirmedVolumes))
	for _, volumeID := range confirmedVolumes {
		confirmed[volumeID] = true
	}
	if requested.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
		for _, impact := range current.GetImpacts() {
			if impact.GetResource().GetKind() == "volume" && !confirmed[impact.GetResource().GetResourceId()] {
				return operation, resourceError(codes.FailedPrecondition, "VOLUME_CONFIRMATION_REQUIRED", "volume", impact.GetResource().GetResourceId(), "every named volume must be explicitly confirmed")
			}
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
		if err := server.controller.store.DeleteEnvironment(ctx, environmentID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			server.controller.failOperation(ctx, &operation, "CLEANUP_FAILED", err)
			return operation, nil
		}
	}
	server.controller.forgetContainers(environmentID)
	server.controller.completeOperation(ctx, &operation, "complete")
	return operation, nil
}

func (server *operatorServer) performCleanup(ctx context.Context, operation *store.Operation, plan *hostv1.CleanupPlan) error {
	environment, err := server.controller.store.GetEnvironment(ctx, plan.GetEnvironmentId())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	grants, err := server.controller.store.AllGrants(ctx)
	if err != nil {
		return err
	}
	if err := server.controller.store.RevokeGrants(ctx, environment.NamespaceID); err != nil {
		return err
	}
	for _, grant := range grants {
		if grant.Kind != "network-link" || grant.SourceNamespaceID != environment.NamespaceID && grant.TargetNamespaceID != environment.NamespaceID {
			continue
		}
		if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: operation.RequestID + "-link-" + grant.ID, LinkId: grant.ID}); err != nil {
			return err
		}
	}
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return err
	}
	for _, container := range containers.GetContainers() {
		if container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
			_, err := client.StopContainer(ctx, &vmmdv1.StopContainerRequest{RequestId: operation.RequestID + "-stop-" + container.GetId(), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId()})
			if err != nil {
				return err
			}
		}
		if _, err := client.DeleteContainer(ctx, &vmmdv1.DeleteContainerRequest{RequestId: operation.RequestID + "-delete-" + container.GetId(), ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId()}); err != nil {
			return err
		}
	}
	if plan.GetKind() == hostv1.CleanupKind_CLEANUP_KIND_RESET {
		return nil
	}
	volumes, err := client.ListVolumes(ctx, &vmmdv1.ListVolumesRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return err
	}
	for _, volume := range volumes.GetVolumes() {
		if _, err := client.DeleteVolume(ctx, &vmmdv1.DeleteVolumeRequest{RequestId: operation.RequestID + "-volume-" + volume.GetId(), ActorNamespaceId: environment.NamespaceID, VolumeId: volume.GetId()}); err != nil {
			return err
		}
	}
	if _, err := client.DeleteNamespace(ctx, &vmmdv1.DeleteNamespaceRequest{RequestId: operation.RequestID + "-namespace", NamespaceId: environment.NamespaceID}); err != nil {
		return err
	}
	if err := server.controller.store.DeleteEnvironment(ctx, environment.ID); err != nil {
		return err
	}
	server.controller.forgetContainers(environment.ID)
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
