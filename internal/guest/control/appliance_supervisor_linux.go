//go:build linux

package control

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/proto"
)

const (
	systemContainerUID = 1000
	systemLogLimit     = 1 << 20
	systemRestartLimit = 3
	systemNonceLimit   = 1024
)

type systemApplianceRecord struct {
	status       *vmmdv1.SystemAppliance
	volume       applianceVolume
	runtimePath  string
	bootstrapUse uint32
	streams      map[string]uint32
	streamUses   map[string]uint32
	usedNonces   map[string]struct{}
	stopping     bool
}

type applianceSupervisor struct {
	runtime *Runtime
	bootID  string
	volumes applianceVolumeManager

	mu      sync.Mutex
	records map[string]*systemApplianceRecord
}

func newApplianceSupervisor(runtime *Runtime, bootID string) *applianceSupervisor {
	return &applianceSupervisor{runtime: runtime, bootID: bootID, volumes: newApplianceVolumeManager(), records: make(map[string]*systemApplianceRecord)}
}

func (supervisor *applianceSupervisor) ensure(ctx context.Context, request *vmmdv1.EnsureSystemApplianceRequest) (*vmmdv1.SystemAppliance, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	identity := request.GetIdentity()
	if err := validateSystemIdentity(identity, supervisor.bootID, false); err != nil {
		clearBytes(request.DataVolumeKey)
		return nil, err
	}
	if request.GetConfigRevision() == 0 || request.GetImageReference() == "" || identity.GetImageManifestDigest() == "" || request.GetDataVolumeId() == "" || request.GetLimits() == nil || request.GetEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
		clearBytes(request.DataVolumeKey)
		return nil, errors.New("system appliance launch configuration is incomplete")
	}
	supervisor.mu.Lock()
	if record := supervisor.records[identity.GetInstallationId()]; record != nil {
		status := cloneSystemAppliance(record.status)
		supervisor.mu.Unlock()
		clearBytes(request.DataVolumeKey)
		if !sameSystemLaunch(status.GetIdentity(), identity, false) || status.GetConfigRevision() != request.GetConfigRevision() || status.GetDataVolumeId() != request.GetDataVolumeId() || status.GetDataVolumeIdentity() != request.GetDataVolumeIdentity() {
			return nil, errors.New("installation already has a different system appliance instance")
		}
		return status, nil
	}
	record := &systemApplianceRecord{status: &vmmdv1.SystemAppliance{Identity: cloneSystemIdentity(identity), DataVolumeId: request.GetDataVolumeId(), DataVolumeIdentity: request.GetDataVolumeIdentity(), State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STARTING, ConfigRevision: request.GetConfigRevision(), Status: "starting"}, streams: make(map[string]uint32), streamUses: make(map[string]uint32), usedNonces: make(map[string]struct{})}
	supervisor.records[identity.GetInstallationId()] = record
	supervisor.mu.Unlock()

	volume, err := supervisor.volumes.unlock(ctx, identity.GetInstallationId(), request.GetDataDeviceIdentifier(), request.GetDataVolumeIdentity(), request.DataVolumeKey, request.GetInitializeDataVolume())
	if err != nil {
		var sealed sealedVolumeError
		if errors.As(err, &sealed) {
			return supervisor.seal(identity.GetInstallationId(), sealed.reason), err
		}
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	record.volume = volume
	runtimePath := filepath.Join(applianceMountRoot, identity.GetInstallationId(), "run")
	if err := ensureDirectory(runtimePath, 0o700); err != nil {
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	if err := os.Chown(runtimePath, systemContainerUID, systemContainerUID); err != nil {
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	if err := os.Chown(volume.mountPath, systemContainerUID, systemContainerUID); err != nil {
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	record.runtimePath = runtimePath

	namespaceRequest := &vmmdv1.EnsureNamespaceRequest{RequestId: request.GetRequestId(), NamespaceId: identity.GetNamespaceId(), Limits: request.GetLimits(), EgressMode: request.GetEgressMode()}
	if _, err := supervisor.runtime.ensureNamespace(ctx, namespaceRequest, true); err != nil {
		err = atRuntimeStage("namespace", err)
		_ = supervisor.volumes.close(context.Background(), volume)
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
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	if digest != identity.GetImageManifestDigest() {
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.seal(identity.GetInstallationId(), "IMAGE_DIGEST_MISMATCH"), errors.New("system appliance image digest does not match verified artifact")
	}
	if err := supervisor.replaceStoppedContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId()); err != nil {
		err = atRuntimeStage("stopped_container_cleanup", err)
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	containerRequest := &vmmdv1.CreateContainerRequest{RequestId: request.GetRequestId(), ActorNamespaceId: identity.GetNamespaceId(), ContainerId: identity.GetContainerId(), Image: canonical,
		ReadOnlyRoot: true, User: stringPointer("1000:1000"), LogLimitBytes: systemLogLimit,
		Limits: &vmmdv1.ResourceLimits{CpuMillis: uint32Pointer(request.GetLimits().GetCpuMillis()), MemoryBytes: uint64Pointer(request.GetLimits().GetMemoryBytes()), Pids: uint32Pointer(request.GetLimits().GetPids())}}
	if _, err := supervisor.runtime.createContainer(ctx, containerRequest, true, systemSpecOption(runtimePath, volume.mountPath)); err != nil {
		_ = supervisor.volumes.close(context.Background(), volume)
		return supervisor.fail(identity.GetInstallationId(), err), err
	}
	container, err := supervisor.runtime.startContainer(ctx, identity.GetNamespaceId(), identity.GetContainerId(), true)
	if err != nil {
		err = atRuntimeStage("container_start", err)
		_ = supervisor.volumes.close(context.Background(), volume)
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

func (supervisor *applianceSupervisor) stop(ctx context.Context, request *vmmdv1.StopSystemApplianceRequest) (*vmmdv1.SystemAppliance, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	if err := validateSystemIdentity(request.GetIdentity(), supervisor.bootID, true); err != nil {
		return nil, err
	}
	supervisor.mu.Lock()
	record := supervisor.records[request.GetIdentity().GetInstallationId()]
	if record == nil || !sameSystemLaunch(record.status.GetIdentity(), request.GetIdentity(), true) {
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
	_, stopErr := supervisor.runtime.stopContainer(ctx, request.GetIdentity().GetNamespaceId(), request.GetIdentity().GetContainerId(), timeout, true)
	closeErr := supervisor.volumes.close(ctx, record.volume)
	_ = os.RemoveAll(record.runtimePath)
	supervisor.mu.Lock()
	record.status.State = vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_STOPPED
	record.status.Status = "stopped"
	result := cloneSystemAppliance(record.status)
	delete(supervisor.records, request.GetIdentity().GetInstallationId())
	supervisor.mu.Unlock()
	return result, errors.Join(stopErr, closeErr)
}

func (supervisor *applianceSupervisor) list() []*vmmdv1.SystemAppliance {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	result := make([]*vmmdv1.SystemAppliance, 0, len(supervisor.records))
	for _, record := range supervisor.records {
		result = append(result, cloneSystemAppliance(record.status))
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].GetIdentity().GetInstallationId() < result[right].GetIdentity().GetInstallationId()
	})
	return result
}

func (supervisor *applianceSupervisor) shutdown(ctx context.Context) error {
	supervisor.mu.Lock()
	identities := make([]*vmmdv1.SystemApplianceIdentity, 0, len(supervisor.records))
	for _, record := range supervisor.records {
		if record.status.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING {
			identities = append(identities, cloneSystemIdentity(record.status.GetIdentity()))
		}
	}
	supervisor.mu.Unlock()
	var result error
	for index, identity := range identities {
		_, err := supervisor.stop(ctx, &vmmdv1.StopSystemApplianceRequest{RequestId: fmt.Sprintf("shutdown-%d", index), Identity: identity, TimeoutSeconds: 5})
		result = errors.Join(result, err)
	}
	return result
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
		volume, runtimePath := record.volume, record.runtimePath
		delete(supervisor.records, installationID)
		supervisor.mu.Unlock()
		if volume.mapper != "" {
			_ = supervisor.volumes.close(context.Background(), volume)
		}
		if runtimePath != "" {
			_ = os.RemoveAll(runtimePath)
		}
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
			volume, runtimePath := record.volume, record.runtimePath
			delete(supervisor.records, installationID)
			supervisor.mu.Unlock()
			if volume.mapper != "" {
				_ = supervisor.volumes.close(context.Background(), volume)
			}
			if runtimePath != "" {
				_ = os.RemoveAll(runtimePath)
			}
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
	record.bootstrapUse = 0
	record.streams = make(map[string]uint32)
	record.streamUses = make(map[string]uint32)
	record.usedNonces = make(map[string]struct{})
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
	if current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return errors.New("an unowned system appliance task is still running")
	}
	return supervisor.runtime.guardOCIMetadataGrowth(ctx, func() error {
		return container.Delete(nsctx, containerd.WithSnapshotCleanup)
	})
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
	volume, runtimePath := record.volume, record.runtimePath
	delete(supervisor.records, installationID)
	supervisor.mu.Unlock()
	// A fallible launch must not leave a FAILED record that rejects the next
	// exact activation. Keep the durable data volume, but release its mount and
	// remove the private runtime directory; a subsequent launch will recreate
	// the container after replaceStoppedContainer fences any partial task.
	if volume.mapper != "" {
		_ = supervisor.volumes.close(context.Background(), volume)
	}
	if runtimePath != "" {
		_ = os.RemoveAll(runtimePath)
	}
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
	volume, runtimePath := record.volume, record.runtimePath
	delete(supervisor.records, installationID)
	supervisor.mu.Unlock()
	if volume.mapper != "" {
		_ = supervisor.volumes.close(context.Background(), volume)
	}
	if runtimePath != "" {
		_ = os.RemoveAll(runtimePath)
	}
	return result
}

func systemSpecOption(runtimePath, dataPath string) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, spec *oci.Spec) error {
		spec.Root.Readonly = true
		spec.Process.User = specs.User{UID: systemContainerUID, GID: systemContainerUID}
		spec.Process.NoNewPrivileges = true
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
		spec.Process.Rlimits = append(spec.Process.Rlimits, specs.POSIXRlimit{Type: "RLIMIT_CORE", Hard: 0, Soft: 0})
		if spec.Linux == nil {
			spec.Linux = &specs.Linux{}
		}
		spec.Linux.Devices = nil
		if spec.Linux.Resources == nil {
			spec.Linux.Resources = &specs.LinuxResources{}
		}
		spec.Linux.Resources.Devices = []specs.LinuxDeviceCgroup{{Allow: false, Access: "rwm"}}
		spec.Mounts = append(spec.Mounts,
			specs.Mount{Destination: "/run/agent-vmm", Type: "bind", Source: runtimePath, Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
			specs.Mount{Destination: "/var/lib/agent-vmm", Type: "bind", Source: dataPath, Options: []string{"rbind", "rw", "nosuid", "nodev", "noexec"}},
		)
		return nil
	}
}

func validateSystemIdentity(identity *vmmdv1.SystemApplianceIdentity, bootID string, requireInstance bool) error {
	if identity == nil || validateID("installation_id", identity.GetInstallationId()) != nil || validateSystemNamespace(identity.GetNamespaceId()) != nil || validateID("container_id", identity.GetContainerId()) != nil || identity.GetBootId() != bootID || identity.GetImageManifestDigest() == "" || len(identity.GetBootNonce()) != 32 {
		return errors.New("system appliance identity is incomplete or belongs to another boot")
	}
	if requireInstance && identity.GetContainerInstanceId() == "" {
		return errors.New("system appliance container instance is required")
	}
	return nil
}

func sameSystemLaunch(left, right *vmmdv1.SystemApplianceIdentity, includeInstance bool) bool {
	if left == nil || right == nil || left.GetInstallationId() != right.GetInstallationId() || left.GetBootId() != right.GetBootId() || left.GetNamespaceId() != right.GetNamespaceId() || left.GetContainerId() != right.GetContainerId() || left.GetImageManifestDigest() != right.GetImageManifestDigest() || subtle.ConstantTimeCompare(left.GetBootNonce(), right.GetBootNonce()) != 1 {
		return false
	}
	return !includeInstance || left.GetContainerInstanceId() == right.GetContainerInstanceId()
}

func cloneSystemIdentity(value *vmmdv1.SystemApplianceIdentity) *vmmdv1.SystemApplianceIdentity {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*vmmdv1.SystemApplianceIdentity)
}

func cloneSystemAppliance(value *vmmdv1.SystemAppliance) *vmmdv1.SystemAppliance {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*vmmdv1.SystemAppliance)
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
func uint64Pointer(value uint64) *uint64 { return &value }
