//go:build darwin

package host

import (
	"context"
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
	ctx, cancel := context.WithTimeout(ctx, cleanupPlanReadTimeout)
	defer cancel()
	environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", environmentID, "agent environment does not exist")
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
	return server.planCleanupWithClient(ctx, client, environmentID, kind, remoteOwner)
}

func (server *operatorServer) planCleanupWithClient(ctx context.Context, client vmmdv1.VMMDServiceClient, environmentID string, kind hostv1.CleanupKind, remoteOwner bool) (*hostv1.CleanupPlan, error) {
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
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID, Limit: cleanupPlanMaxInventory + 1})
	if err != nil {
		if status.Code(err) == codes.ResourceExhausted {
			return nil, cleanupInventoryLimitError(environmentID)
		}
		missing, confirmErr := confirmMissingNamespaceDiscard(ctx, client, environment.NamespaceID, kind, err)
		if confirmErr != nil {
			return nil, confirmErr
		}
		if missing {
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
		missing, confirmErr := confirmMissingNamespaceDiscard(ctx, client, environment.NamespaceID, kind, err)
		if confirmErr != nil {
			return nil, confirmErr
		}
		if missing {
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
		missing, confirmErr := confirmMissingNamespaceDiscard(ctx, client, environment.NamespaceID, kind, err)
		if confirmErr != nil {
			return nil, confirmErr
		}
		if missing {
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
	parts := []string{kind.String(), strconv.FormatUint(policyRevision, 10), namespaceID}
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
	sort.Strings(parts[3:])
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
	writeRevisionPart(hash, environment.NamespaceID)
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

func confirmMissingNamespaceDiscard(ctx context.Context, client vmmdv1.VMMDServiceClient, namespaceID string, kind hostv1.CleanupKind, inventoryErr error) (bool, error) {
	if kind != hostv1.CleanupKind_CLEANUP_KIND_DISCARD || status.Code(inventoryErr) != codes.NotFound {
		return false, nil
	}
	_, err := client.GetNamespace(ctx, &vmmdv1.GetNamespaceRequest{NamespaceId: namespaceID})
	if status.Code(err) == codes.NotFound {
		return true, nil
	}
	return false, err
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
