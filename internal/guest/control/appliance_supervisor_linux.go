//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"github.com/google/btree"
	"google.golang.org/protobuf/proto"
)

const (
	systemContainerUID = 1000
	systemLogLimit     = 1 << 20
	systemRestartLimit = 3
)

type systemApplianceRecord struct {
	status              *vmmdv1.SystemAppliance
	volume              applianceVolume
	runtimePath         string
	contentPath         string
	streams             map[string]uint32
	bootstrapNonces     map[string]struct{}
	inFlightNonces      map[string]struct{}
	stopping            bool
	cleanupMu           sync.Mutex
	launchFingerprint   [32]byte
	containerDispatched bool
}

type applianceSupervisor struct {
	runtime *Runtime
	bootID  string
	volumes applianceVolumeManager

	mu      sync.Mutex
	records map[string]*systemApplianceRecord
	order   *btree.BTreeG[string]
}

func newApplianceSupervisor(runtime *Runtime, bootID string) *applianceSupervisor {
	return &applianceSupervisor{runtime: runtime, bootID: bootID, volumes: newApplianceVolumeManager(runtime.stateDevice), records: make(map[string]*systemApplianceRecord),
		order: btree.NewG(8, func(left, right string) bool { return left < right })}
}

func (supervisor *applianceSupervisor) ensure(ctx context.Context, request *vmmdv1.EnsureSystemApplianceRequest) (*vmmdv1.SystemAppliance, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	identity := request.GetIdentity()
	if err := validateSystemIdentity(identity, supervisor.bootID, false); err != nil {
		return nil, err
	}
	if request.GetConfigRevision() == 0 || request.GetImageReference() == "" || identity.GetImageManifestDigest() == "" || request.GetDataVolumeId() == "" || request.GetLimits() == nil || request.GetEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
		return nil, errors.New("system appliance launch configuration is incomplete")
	}
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(requestBytes)
	clearBytes(requestBytes)
	supervisor.mu.Lock()
	if record := supervisor.records[identity.GetInstallationId()]; record != nil {
		status := cloneSystemAppliance(record.status)
		matches := record.launchFingerprint == fingerprint
		pending := record.stopping && (status.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED || status.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED)
		supervisor.mu.Unlock()
		if !matches {
			return nil, errors.New("installation already has a different system appliance launch")
		}
		if pending {
			record.cleanupMu.Lock()
			supervisor.mu.Lock()
			current := supervisor.records[identity.GetInstallationId()] == record && record.launchFingerprint == fingerprint
			supervisor.mu.Unlock()
			if current {
				supervisor.releaseFailedRecord(record)
			}
			supervisor.mu.Lock()
			retained := supervisor.records[identity.GetInstallationId()]
			if retained == record {
				status = cloneSystemAppliance(record.status)
			}
			supervisor.mu.Unlock()
			record.cleanupMu.Unlock()
			if retained == nil {
				return supervisor.ensure(ctx, request)
			}
			if retained != record {
				return nil, errors.New("system appliance launch changed during cleanup")
			}
			return status, errors.New("system appliance launch cleanup incomplete")
		}
		return status, nil
	}

	record := &systemApplianceRecord{launchFingerprint: fingerprint, status: &vmmdv1.SystemAppliance{Identity: cloneSystemIdentity(identity), DataVolumeId: request.GetDataVolumeId(), State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STARTING, ConfigRevision: request.GetConfigRevision(), Status: "starting"}, streams: make(map[string]uint32), bootstrapNonces: make(map[string]struct{}), inFlightNonces: make(map[string]struct{})}
	supervisor.records[identity.GetInstallationId()] = record
	supervisor.order.ReplaceOrInsert(identity.GetInstallationId())
	supervisor.mu.Unlock()

	if err := supervisor.replaceStoppedContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId()); err != nil {
		err = atRuntimeStage("stopped_container_cleanup", err)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}

	volume, err := supervisor.prepareData(identity.GetInstallationId(), request.GetDataVolumeId(), request.GetDataVolumeBytes())
	record.volume = volume
	if err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	runtimePath := filepath.Join(applianceMountRoot, identity.GetInstallationId(), "run")
	if err := ensureDirectory(runtimePath, 0o700); err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	if err := os.Chown(runtimePath, systemContainerUID, systemContainerUID); err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	if err := os.Chown(volume.mountPath, systemContainerUID, systemContainerUID); err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	record.runtimePath = runtimePath

	namespaceRequest := &vmmdv1.EnsureNamespaceRequest{RequestId: request.GetRequestId(), NamespaceId: identity.GetNamespaceId(), Limits: request.GetLimits(), EgressMode: request.GetEgressMode()}
	namespace, err := supervisor.runtime.ensureNamespace(ctx, namespaceRequest, true)
	if err != nil {
		err = atRuntimeStage("namespace", err)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	supervisor.mu.Lock()
	record.status.NetworkSubnet = namespace.GetSubnet()
	supervisor.mu.Unlock()
	if err := supervisor.runtime.network.SetSystemLAN(identity.GetNamespaceId(), request.GetLanEgress()); err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	var canonical, digest string
	if isBundledSystemImage(request.GetImageReference()) {
		canonical, digest, _, err = supervisor.runtime.importBundledSystemImage(ctx, identity.GetNamespaceId(), request.GetImageReference())
	} else {
		canonical, digest, _, err = supervisor.runtime.pullImage(ctx, identity.GetNamespaceId(), request.GetImageReference(), true)
	}
	if err != nil {
		err = atRuntimeStage("image_materialization", err)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	if digest != identity.GetImageManifestDigest() {
		return supervisor.seal(identity.GetInstallationId(), "IMAGE_DIGEST_MISMATCH"), errors.New("system appliance image digest does not match the selected artifact")
	}
	containerRequest := &vmmdv1.CreateContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.GetNamespaceId(), ContainerId: identity.GetContainerId(), Image: canonical,
		ReadOnlyRoot: true, User: stringPointer("1000:1000"), LogLimitBytes: systemLogLimit,
		Limits: &vmmdv1.ResourceLimits{Pids: uint32Pointer(request.GetLimits().GetPids())}}
	contentPath, err := supervisor.runtime.projectApplianceContent(ctx, request)
	if err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	record.contentPath = contentPath
	record.containerDispatched = true
	if _, err := supervisor.runtime.createContainer(ctx, containerRequest, true, systemSpecOption(runtimePath, volume.mountPath, contentPath)); err != nil {
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	container, err := supervisor.runtime.startContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId(), true)
	if err != nil {
		err = atRuntimeStage("container_start", err)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	supervisor.mu.Lock()
	record = supervisor.records[identity.GetInstallationId()]
	record.status.Identity.ContainerInstanceId = container.GetInstanceId()
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING
	record.status.Status = "running"
	result := cloneSystemAppliance(record.status)
	supervisor.mu.Unlock()
	go supervisor.observe(identity.GetInstallationId(), identity.GetNamespaceId(), identity.GetContainerId(), container.GetInstanceId())
	return result, nil
}

func (supervisor *applianceSupervisor) prepareData(installationID, volumeID string, size uint64) (applianceVolume, error) {
	var volume applianceVolume
	err := supervisor.runtime.diskBudget.MutateInventory(func(*guestdisk.Transaction) error {
		var old uint64
		path := filepath.Join(supervisor.volumes.root, installationID, volumeID)
		if _, err := os.Lstat(path); err == nil {
			project, err := supervisor.volumes.project(path)
			if err != nil {
				return err
			}
			state, err := guestquota.Read(supervisor.volumes.device, project)
			if err != nil {
				return err
			}
			old = state.HardLimitBytes
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := supervisor.runtime.admitPoolGrowth(context.Background(), size, old); err != nil {
			return err
		}
		var err error
		volume, err = supervisor.volumes.prepare(installationID, volumeID, size)
		return err
	})
	return volume, err
}

func (supervisor *applianceSupervisor) get(installationID string) (*vmmdv1.SystemAppliance, error) {
	if err := validateID("installation_id", installationID); err != nil {
		return nil, err
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	record := supervisor.records[installationID]
	if record == nil {
		return nil, errdefs.ErrNotFound
	}
	return cloneSystemAppliance(record.status), nil
}

func (supervisor *applianceSupervisor) list(after string, limit int) ([]*vmmdv1.SystemAppliance, string) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	result := make([]*vmmdv1.SystemAppliance, 0, limit)
	next := ""
	supervisor.order.AscendGreaterOrEqual(after, func(installationID string) bool {
		if installationID == after {
			return true
		}
		if len(result) == limit {
			next = installationID
			return false
		}
		result = append(result, cloneSystemAppliance(supervisor.records[installationID].status))
		return true
	})
	if next != "" && len(result) != 0 {
		next = result[len(result)-1].GetIdentity().GetInstallationId()
	}
	return result, next
}

func (supervisor *applianceSupervisor) observe(installationID, namespaceID, containerID, instanceID string) {
	exitCode, waitErr := supervisor.runtime.waitContainerForRole(context.Background(), namespaceID, containerID, true)
	supervisor.mu.Lock()
	record := supervisor.records[installationID]
	if record == nil || record.stopping || record.status.GetIdentity().GetContainerInstanceId() != instanceID {
		supervisor.mu.Unlock()
		return
	}
	if record.status.GetRestartCount() >= systemRestartLimit {
		record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED
		record.status.Status = "restart limit reached"
		record.status.BoundedLog = boundedStatus(fmt.Errorf("system appliance payload exited with code %d after %d restarts: %v", exitCode, record.status.GetRestartCount(), waitErr))
		record.stopping = true
		supervisor.mu.Unlock()
		supervisor.cleanupFailedRecord(record)
		return
	}
	record.status.RestartCount++
	restartCount := record.status.GetRestartCount()
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STARTING
	record.status.Status = fmt.Sprintf("restart backoff %d/%d", restartCount, systemRestartLimit)
	record.status.BoundedLog = boundedStatus(fmt.Errorf("system appliance payload exited with code %d: %v", exitCode, waitErr))
	supervisor.mu.Unlock()
	go supervisor.restartAfterBackoff(installationID, namespaceID, containerID, instanceID, restartCount)
}

func (supervisor *applianceSupervisor) restartAfterBackoff(installationID, namespaceID, containerID, previousInstanceID string, restartCount uint32) {
	delay := time.Second << (restartCount - 1)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	supervisor.mu.Lock()
	record := supervisor.records[installationID]
	supervisor.mu.Unlock()
	if record == nil {
		return
	}
	record.cleanupMu.Lock()
	defer record.cleanupMu.Unlock()
	supervisor.mu.Lock()
	if supervisor.records[installationID] != record {
		supervisor.mu.Unlock()
		return
	}
	if record == nil || record.stopping || record.status.GetIdentity().GetContainerInstanceId() != previousInstanceID || record.status.GetRestartCount() != restartCount {
		supervisor.mu.Unlock()
		return
	}
	supervisor.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	container, err := supervisor.runtime.startContainer(ctx, namespaceID, containerID, true)
	cancel()
	if err != nil {
		supervisor.mu.Lock()
		record = supervisor.records[installationID]
		if record != nil && !record.stopping && record.status.GetIdentity().GetContainerInstanceId() == previousInstanceID && record.status.GetRestartCount() == restartCount {
			record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED
			record.status.Status = "restart failed"
			record.status.BoundedLog = boundedStatus(err)
			record.stopping = true
			supervisor.mu.Unlock()
			supervisor.releaseFailedRecord(record)
			return
		}
		supervisor.mu.Unlock()
		return
	}

	supervisor.mu.Lock()
	record = supervisor.records[installationID]
	if record == nil || record.stopping || record.status.GetIdentity().GetContainerInstanceId() != previousInstanceID || record.status.GetRestartCount() != restartCount {
		supervisor.mu.Unlock()
		return
	}
	record.status.Identity.ContainerInstanceId = container.GetInstanceId()
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING
	record.status.Status = "running after restart"
	record.streams = make(map[string]uint32)
	record.bootstrapNonces = make(map[string]struct{})
	record.inFlightNonces = make(map[string]struct{})
	newInstanceID := container.GetInstanceId()
	supervisor.mu.Unlock()
	go supervisor.observe(installationID, namespaceID, containerID, newInstanceID)
}

func (supervisor *applianceSupervisor) replaceStoppedContainer(ctx context.Context, namespaceID, containerID string) error {
	nsctx := runtimeContext(ctx)
	container, err := ownedRuntimeContainer(ctx, supervisor.runtime.client, namespaceID, containerID)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	current, err := supervisor.runtime.inspectContainer(nsctx, namespaceID, container)
	if err != nil {
		return err
	}
	if current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
		return errors.New("system appliance container stop is unconfirmed")
	}
	if err := container.Delete(nsctx, containerd.WithSnapshotCleanup); err != nil {
		return err
	}
	inventory, err := supervisor.runtime.requireRuntimeInventory()
	if err != nil {
		return err
	}
	return inventory.removeContainer(namespaceID, containerID)
}

func (supervisor *applianceSupervisor) fail(installationID string, err error) *vmmdv1.SystemAppliance {
	supervisor.mu.Lock()
	record := supervisor.records[installationID]
	if record == nil {
		supervisor.mu.Unlock()
		return &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: installationID}, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED, Status: "launch failed", BoundedLog: boundedStatus(err)}
	}
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_FAILED
	record.status.Status = "launch failed"
	record.status.BoundedLog = boundedStatus(err)
	result := cloneSystemAppliance(record.status)
	record.stopping = true
	supervisor.mu.Unlock()
	supervisor.cleanupFailedRecord(record)
	return result
}

func (supervisor *applianceSupervisor) seal(installationID, reason string) *vmmdv1.SystemAppliance {
	supervisor.mu.Lock()
	record := supervisor.records[installationID]
	if record == nil {
		supervisor.mu.Unlock()
		return &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: installationID}, State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED, Status: reason, BoundedLog: []byte(reason)}
	}
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED
	record.status.Status = reason
	record.status.BoundedLog = []byte(reason)
	result := cloneSystemAppliance(record.status)
	record.stopping = true
	supervisor.mu.Unlock()
	supervisor.cleanupFailedRecord(record)
	return result
}

func boundedStatus(err error) []byte {
	value := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(value) > 4096 {
		value = value[:4096]
	}
	return []byte(value)
}

func stringPointer(value string) *string { return &value }
func uint32Pointer(value uint32) *uint32 { return &value }
