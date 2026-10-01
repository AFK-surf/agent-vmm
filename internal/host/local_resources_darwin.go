//go:build darwin

package host

import (
	"context"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

const localResourceFreshness = 15 * time.Second

type localResourceSample struct {
	namespace, boot string
	generation      uint64
	disk            *uint64
	sampledAt       time.Time
	observedAt      time.Time
	active          bool
}

// Cache one scalar observation per environment from its existing owner read.
// This does not add a sampler, timers, or Guest probes to the overview path.
func (controller *Controller) rememberLocalResourceSample(environment store.Environment, snapshot *hostv1.AgentEnvironment) {
	usage := snapshot.GetUsage()
	if usage == nil || usage.GetStale() || usage.GetUnavailableReason() != "" || usage.GetSampledAt() == nil {
		return
	}
	bytes := usage.GetDiskBytes()
	sample := localResourceSample{namespace: environment.NamespaceID, boot: controller.manager.Snapshot().GetBootId(), generation: environment.AllocationGeneration, disk: &bytes, sampledAt: usage.GetSampledAt().AsTime(), observedAt: snapshot.GetInventoryObservedAt().AsTime()}
	// Partial positive evidence may prove activity. It cannot prove that other work is stopped.
	for _, container := range snapshot.GetContainers() {
		if container.GetState() == hostv1.ContainerState_CONTAINER_STATE_RUNNING {
			sample.active = true
			break
		}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.localResourceCache == nil {
		controller.localResourceCache = make(map[string]localResourceSample)
	}
	controller.localResourceCache[environment.ID] = sample
}

func (server *operatorServer) GetLocalDiskSummary(ctx context.Context, _ *hostv1.GetLocalDiskSummaryRequest) (*hostv1.LocalDiskSummary, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	vm := server.controller.manager.Snapshot()
	result := &hostv1.LocalDiskSummary{CollectedAt: timestamppb.Now(), BootId: vm.GetBootId(), VmState: vm.GetState(), UnavailableReason: "guest state disk is unavailable"}
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return result, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return result, nil
	}
	defer connection.Close()
	usage, err := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{})
	if err != nil || usage.GetDisk() == nil {
		return result, nil
	}
	result.GuestStateFs = guestLayerResources(usage).GetDisk()
	result.UnavailableReason = ""
	return result, nil
}

func (server *operatorServer) ListLocalEnvironmentPage(ctx context.Context, request *hostv1.ListLocalEnvironmentPageRequest) (*hostv1.LocalEnvironmentPage, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if len(request.GetAfterId()) > 200 {
		return nil, invalidField("after_id", "environment cursor is too long")
	}
	rows, err := server.controller.store.ListLocalResourceEnvironmentPage(ctx, request.GetAfterId(), 32)
	if err != nil {
		return nil, disposalReadError(err, "environment-page")
	}
	vm := server.controller.manager.Snapshot()
	now := time.Now()
	result := &hostv1.LocalEnvironmentPage{CollectedAt: timestamppb.New(now), BootId: vm.GetBootId()}
	for _, row := range rows {
		item := &hostv1.LocalEnvironmentSummary{Id: row.ID, Origin: "local", State: "unknown", StateObservedAt: timestamppb.New(row.UpdatedAt), Partial: true, Revision: row.PolicyRevision, RegistrationId: row.RegistrationID, AllocationId: row.AllocationID, AllocationGeneration: row.AllocationGeneration}
		if row.OwnerKind == store.OwnerRemoteRegistration {
			item.Origin = "remote"
			if row.AllocationState == int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
				item.State = "retired"
			}
		}
		if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
			item.State = "vm_stopped"
			item.StateObservedAt = result.CollectedAt
		}
		server.controller.mu.Lock()
		sample, ok := server.controller.localResourceCache[row.ID]
		server.controller.mu.Unlock()
		if ok && sample.boot == vm.GetBootId() && sample.namespace == row.NamespaceID && sample.generation == row.AllocationGeneration && now.Sub(sample.sampledAt) >= 0 && now.Sub(sample.sampledAt) <= localResourceFreshness && vm.GetState() == hostv1.VMState_VM_STATE_RUNNING {
			item.PrivateDiskBytes = sample.disk
			item.SampledAt = timestamppb.New(sample.sampledAt)
			if sample.active && item.State == "unknown" {
				item.State = "observed_active"
				item.StateObservedAt = timestamppb.New(sample.observedAt)
			}
		}
		result.Environments = append(result.Environments, item)
	}
	if len(rows) == 32 {
		result.NextCursor = rows[len(rows)-1].ID
	}
	return result, nil
}

func (server *operatorServer) ListLocalRegistrationPage(ctx context.Context, request *hostv1.ListLocalRegistrationPageRequest) (*hostv1.LocalRegistrationPage, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if len(request.GetAfterId()) > 200 {
		return nil, invalidField("after_id", "registration cursor is too long")
	}
	rows, err := server.controller.store.ListRemoteRegistrationPage(ctx, request.GetAfterId(), 32)
	if err != nil {
		return nil, disposalReadError(err, "registration-page")
	}
	result := &hostv1.LocalRegistrationPage{}
	for _, row := range rows {
		if row.OwnerKind == store.RemoteRegistrationExternal {
			result.Registrations = append(result.Registrations, &hostv1.LocalRegistrationMetadata{Id: row.ID, Revision: row.Revision, State: hostv1.RemoteRegistrationState(row.State)})
		}
	}
	if len(rows) == 32 {
		result.NextCursor = rows[len(rows)-1].ID
	}
	return result, nil
}
