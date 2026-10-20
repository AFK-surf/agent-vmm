//go:build linux

package control

import (
	"context"
	"errors"
	"path/filepath"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (supervisor *applianceSupervisor) deleteData(ctx context.Context, installationID, volumeID string) error {
	if err := validateID("installation_id", installationID); err != nil {
		return err
	}
	if err := validateID("data_volume_id", volumeID); err != nil {
		return err
	}
	if supervisor.runtime != nil {
		unlock, ok := supervisor.runtime.materializer.TryLockBuild()
		if !ok {
			return status.Error(codes.Unavailable, "system appliance data cleanup is waiting for materialization")
		}
		defer unlock()
	}
	supervisor.mu.Lock()
	if supervisor.records[installationID] != nil {
		supervisor.mu.Unlock()
		return errors.New("system appliance must stop before its data is deleted")
	}
	record := &systemApplianceRecord{stopping: true, status: &vmmdv1.SystemAppliance{
		Identity:     &vmmdv1.SystemApplianceIdentity{InstallationId: installationID, BootId: supervisor.bootID, NamespaceId: systemInstallationNamespace(installationID)},
		DataVolumeId: volumeID, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED, Status: "deleting data",
	}}
	record.cleanupMu.Lock()
	supervisor.records[installationID] = record
	supervisor.order.ReplaceOrInsert(installationID)
	supervisor.mu.Unlock()
	defer func() {
		supervisor.mu.Lock()
		delete(supervisor.records, installationID)
		supervisor.order.Delete(installationID)
		supervisor.mu.Unlock()
		record.cleanupMu.Unlock()
	}()
	if supervisor.runtime != nil {
		containers, err := supervisor.runtime.client.Containers(runtimeContext(ctx))
		if err != nil {
			return err
		}
		path := filepath.Join(supervisor.volumes.root, installationID, volumeID)
		for _, container := range containers {
			spec, err := container.Spec(runtimeContext(ctx))
			if err != nil {
				return err
			}
			for _, mount := range spec.Mounts {
				if mount.Source == path {
					task, err := container.Task(runtimeContext(ctx), nil)
					if errdefs.IsNotFound(err) {
						continue
					}
					if err != nil {
						return err
					}
					state, err := task.Status(runtimeContext(ctx))
					if err != nil {
						return err
					}
					if state.Status != containerd.Stopped {
						return errors.New("appliance data still has an active runtime writer")
					}
				}
			}
		}
	}
	if supervisor.runtime == nil {
		return supervisor.volumes.delete(installationID, volumeID)
	}
	if err := supervisor.runtime.releaseSystemInstallationImagesMaterializerHeld(ctx, installationID); err != nil {
		return err
	}
	namespaceID := systemInstallationNamespace(installationID)
	unlock := supervisor.runtime.lockNamespace(namespaceID)
	defer unlock()
	return supervisor.runtime.diskBudget.MutateInventory(func(*guestdisk.Transaction) error {
		if err := supervisor.volumes.delete(installationID, volumeID); err != nil {
			return err
		}
		return supervisor.runtime.deleteNamespaceUnderBudgetLocked(ctx, namespaceID)
	})
}
