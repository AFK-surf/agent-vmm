//go:build darwin || linux

package host

import (
	"context"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/cleanup"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *operatorServer) GetLocalDiskSummary(ctx context.Context, _ *hostv1.GetLocalDiskSummaryRequest) (*hostv1.GetLocalDiskSummaryResponse, error) {
	vm := server.controller.manager.Snapshot()
	result := &hostv1.GetLocalDiskSummaryResponse{CollectedAt: timestamppb.Now(), BootId: vm.GetBootId(), VmState: vm.GetState(), UnavailableReason: "guest state disk is unavailable"}
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return result, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	client, err := server.controller.guestConnection.Client()
	if err != nil {
		return result, nil
	}

	return readLocalDiskSummary(ctx, result, client)
}

func readLocalDiskSummary(ctx context.Context, result *hostv1.GetLocalDiskSummaryResponse, client vmmdv1.VMMDServiceClient) (*hostv1.GetLocalDiskSummaryResponse, error) {
	usage, err := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{})
	if err != nil || usage.GetDisk() == nil {
		return result, nil
	}
	result.CollectedAt = timestamppb.Now()
	result.GuestStateFs = guestLayerResources(usage).GetDisk()
	result.UnavailableReason = ""
	return result, nil
}

func (server *operatorServer) ListLocalEnvironmentPage(ctx context.Context, request *hostv1.ListLocalEnvironmentPageRequest) (*hostv1.ListLocalEnvironmentPageResponse, error) {
	if len(request.GetAfterId()) > 200 {
		return nil, hostv1.InvalidField("after_id", "environment cursor is too long")
	}
	rows, err := server.controller.store.ListLocalResourceEnvironmentPage(ctx, request.GetAfterId(), 32)
	if err != nil {
		return nil, cleanup.ReadError(err)
	}
	vm := server.controller.manager.Snapshot()
	now := time.Now()
	result := &hostv1.ListLocalEnvironmentPageResponse{CollectedAt: timestamppb.New(now), BootId: vm.GetBootId()}
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
		server.controller.workloads.ApplyLocalResourceSample(item, row, vm, now)
		result.Environments = append(result.Environments, item)
	}
	if len(rows) == 32 {
		result.NextCursor = rows[len(rows)-1].ID
	}
	return result, nil
}

func (server *operatorServer) ListLocalRegistrationPage(ctx context.Context, request *hostv1.ListLocalRegistrationPageRequest) (*hostv1.ListLocalRegistrationPageResponse, error) {
	if len(request.GetAfterId()) > 200 {
		return nil, hostv1.InvalidField("after_id", "registration cursor is too long")
	}
	rows, err := server.controller.store.ListRemoteRegistrationPage(ctx, request.GetAfterId(), 32)
	if err != nil {
		return nil, cleanup.ReadError(err)
	}
	result := &hostv1.ListLocalRegistrationPageResponse{}
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
