//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/containerd/errdefs"
)

func (supervisor *applianceSupervisor) stop(ctx context.Context, request *vmmdv1.StopSystemApplianceRequest) (*vmmdv1.SystemAppliance, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	if err := validateSystemIdentity(request.GetIdentity(), supervisor.bootID, true); err != nil {
		return nil, err
	}
	supervisor.mu.Lock()
	record := supervisor.records[request.GetIdentity().GetInstallationId()]
	if record == nil || !sameSystemLaunch(record.status.GetIdentity(), request.GetIdentity()) {
		supervisor.mu.Unlock()
		return nil, errdefs.ErrNotFound
	}
	record.stopping = true
	record.status.Status = "stopping"
	supervisor.mu.Unlock()
	timeout := time.Duration(request.GetTimeoutSeconds()) * time.Second
	if timeout == 0 || timeout > 30*time.Second {
		timeout = 10 * time.Second
	}
	record.cleanupMu.Lock()
	defer record.cleanupMu.Unlock()
	supervisor.mu.Lock()
	current := supervisor.records[request.GetIdentity().GetInstallationId()]
	valid := current == record && sameSystemLaunch(record.status.GetIdentity(), request.GetIdentity())
	supervisor.mu.Unlock()
	if !valid {
		return nil, errdefs.ErrNotFound
	}
	return supervisor.stopRecord(ctx, record, timeout)
}

// The caller holds cleanupMu after fencing new launches under supervisor.mu.
func (supervisor *applianceSupervisor) stopRecord(ctx context.Context, record *systemApplianceRecord, timeout time.Duration) (*vmmdv1.SystemAppliance, error) {
	if record.containerDispatched {
		identity := record.status.GetIdentity()
		lanErr := supervisor.runtime.network.SetSystemLAN(identity.GetNamespaceId(), false)
		_, stopErr := supervisor.runtime.stopContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId(), timeout, true)
		if errdefs.IsNotFound(stopErr) {
			stopErr = nil
		}
		if err := errors.Join(lanErr, stopErr); err != nil {
			return supervisor.stopFailure(record, err), err
		}
	}
	if err := supervisor.releaseRecord(ctx, record); err != nil {
		return supervisor.stopFailure(record, err), err
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED
	record.status.Status = "stopped"
	return cloneSystemAppliance(record.status), nil
}

func (supervisor *applianceSupervisor) stopFailure(record *systemApplianceRecord, err error) *vmmdv1.SystemAppliance {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if record.status.State != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED {
		record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED
	}
	record.status.Status = "stop incomplete"
	record.status.BoundedLog = boundedStatus(err)
	return cloneSystemAppliance(record.status)
}

// The caller serializes cleanup of this record.
func (supervisor *applianceSupervisor) releaseRecord(ctx context.Context, record *systemApplianceRecord) error {
	supervisor.mu.Lock()
	current := supervisor.records[record.status.GetIdentity().GetInstallationId()]
	supervisor.mu.Unlock()
	if current != record {
		return nil
	}
	if record.containerDispatched {
		identity := record.status.GetIdentity()
		if err := supervisor.replaceStoppedContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId()); err != nil {
			return err
		}
	}
	if record.contentPath != "" {
		if err := os.RemoveAll(record.contentPath); err != nil {
			return err
		}
	}
	if record.runtimePath != "" {
		if err := os.RemoveAll(record.runtimePath); err != nil {
			return errors.New("remove appliance private runtime directory")
		}
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	id := record.status.GetIdentity().GetInstallationId()
	if supervisor.records[id] == record {
		delete(supervisor.records, id)
		supervisor.order.Delete(id)
	}
	return nil
}

func (supervisor *applianceSupervisor) cleanupFailedRecord(record *systemApplianceRecord) {
	record.cleanupMu.Lock()
	defer record.cleanupMu.Unlock()
	supervisor.releaseFailedRecord(record)
}

func (supervisor *applianceSupervisor) releaseFailedRecord(record *systemApplianceRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	supervisor.mu.Lock()
	current := supervisor.records[record.status.GetIdentity().GetInstallationId()] == record
	supervisor.mu.Unlock()
	if !current {
		return
	}
	if record.containerDispatched {
		identity := record.status.GetIdentity()
		if err := supervisor.runtime.network.SetSystemLAN(identity.GetNamespaceId(), false); err != nil {
			supervisor.stopFailure(record, errors.New("system appliance failed launch LAN disable failed"))
			return
		}
		_, err := supervisor.runtime.stopContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId(), 10*time.Second, true)
		if err != nil && !errdefs.IsNotFound(err) {
			supervisor.stopFailure(record, errors.New("system appliance failed launch stop incomplete"))
			return
		}
	}
	if err := supervisor.releaseRecord(ctx, record); err != nil {
		supervisor.mu.Lock()
		record.status.BoundedLog = boundedStatus(err)
		supervisor.mu.Unlock()
	}
}
