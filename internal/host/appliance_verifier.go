package host

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

var canonicalOCIDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type applianceArtifactVerifier struct{}

func (applianceArtifactVerifier) verify(manifest *appliancev1.ControllerApplianceManifest, grant *appliancev1.ApprovedGrant) ([]byte, []byte, error) {
	if manifest == nil || manifest.GetContractVersion() != "1" || manifest.GetProductId() == "" {
		return nil, nil, errors.New("manifest identity is incomplete")
	}
	if manifest.GetPlatform() != "linux/arm64" || !canonicalOCIDigest.MatchString(manifest.GetImageManifestDigest()) {
		return nil, nil, errors.New("manifest platform or image digest is invalid")
	}
	if manifest.GetDataVolumeBytes() < 1<<30 || manifest.GetDataVolumeBytes() > 64<<30 || manifest.GetSystemResources() == nil ||
		!limitsPositive(manifest.GetSystemResources().GetFloor()) || !limitsPositive(manifest.GetSystemResources().GetCeiling()) ||
		manifest.GetRequestedCapacity() == nil || manifest.GetRequestedCapacity().GetMaxEnvironments() == 0 ||
		!limitsPositive(manifest.GetRequestedCapacity().GetPerEnvironmentLimits()) {
		return nil, nil, errors.New("manifest resource declaration is incomplete")
	}
	if manifest.GetBootstrapProtocolVersion() == "" || manifest.GetControllerProtocolVersion() == "" ||
		manifest.GetStartupDeadlineSeconds() == 0 || manifest.GetStartupDeadlineSeconds() > 300 ||
		manifest.GetProbationSeconds() == 0 || manifest.GetProbationSeconds() > 60 ||
		manifest.GetHealthTimeoutSeconds() == 0 || manifest.GetHealthTimeoutSeconds() > 60 || manifest.GetMinimumApplianceRuntime() == "" {
		return nil, nil, errors.New("manifest lifecycle declaration is incomplete or outside host ceilings")
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	if err != nil {
		return nil, nil, err
	}
	if len(canonical) > 1<<20 {
		return nil, nil, errors.New("canonical manifest exceeds size limit")
	}
	digest := sha256.Sum256(canonical)
	if grant != nil {
		if err := validateApprovedGrant(manifest, grant); err != nil {
			return nil, nil, err
		}
	}
	return canonical, digest[:], nil
}

func validateApprovedGrant(manifest *appliancev1.ControllerApplianceManifest, grant *appliancev1.ApprovedGrant) error {
	requested := manifest.GetRequestedCapacity()
	approved := grant.GetCapacity()
	if requested == nil || approved == nil || grant.GetSystemResources() == nil || manifest.GetSystemResources() == nil {
		return errors.New("resource grant is incomplete")
	}
	requestedSystem, approvedSystem := manifest.GetSystemResources(), grant.GetSystemResources()
	if !limitsLTE(requestedSystem.GetFloor(), approvedSystem.GetFloor()) ||
		!limitsLTE(approvedSystem.GetFloor(), approvedSystem.GetCeiling()) ||
		!limitsLTE(approvedSystem.GetCeiling(), requestedSystem.GetCeiling()) {
		return errors.New("approved system resources expand or violate the manifest range")
	}
	if approved.GetMaxEnvironments() > requested.GetMaxEnvironments() ||
		!limitsLTE(approved.GetPerEnvironmentLimits(), requested.GetPerEnvironmentLimits()) ||
		approved.GetMaxEgressMode() > requested.GetMaxEgressMode() || grant.GetEgressMode() > manifest.GetRequestedEgressMode() ||
		approved.GetMaxLeaseSeconds() > requested.GetMaxLeaseSeconds() ||
		approved.GetDisconnectGraceSeconds() > requested.GetDisconnectGraceSeconds() {
		return errors.New("approved capacity expands manifest capabilities")
	}
	if len(grant.GetFrontends()) > len(manifest.GetRequestedFrontends()) {
		return errors.New("approved frontends expand manifest capabilities")
	}
	requestedFrontends := make(map[string]*appliancev1.ControllerFrontend, len(manifest.GetRequestedFrontends()))
	for _, frontend := range manifest.GetRequestedFrontends() {
		if frontend.GetName() == "" || requestedFrontends[frontend.GetName()] != nil {
			return errors.New("manifest frontend identity is invalid")
		}
		requestedFrontends[frontend.GetName()] = frontend
	}
	for _, frontend := range grant.GetFrontends() {
		requestedFrontend := requestedFrontends[frontend.GetName()]
		if requestedFrontend == nil || !frontendWithin(frontend, requestedFrontend) {
			return fmt.Errorf("approved frontend %q expands manifest capabilities", frontend.GetName())
		}
	}
	requestedDynamic := manifest.GetRequestedDynamicHostLoopback()
	if requestedDynamic != nil && !dynamicHostLoopbackWithin(requestedDynamic, requestedDynamic) {
		return errors.New("manifest dynamic host loopback grant exceeds host ceilings")
	}
	if !dynamicHostLoopbackWithin(grant.GetDynamicHostLoopback(), requestedDynamic) {
		return errors.New("approved dynamic host loopback grant expands manifest capabilities or host ceilings")
	}
	return nil
}

// validateApplianceCapacity keeps a signed grant from advertising resources
// that can never fit in this VM.
func validateApplianceCapacity(grant *appliancev1.ApprovedGrant, host store.ResourceAmount) error {
	if grant == nil || grant.GetSystemResources() == nil || grant.GetSystemResources().GetCeiling() == nil ||
		grant.GetCapacity() == nil || grant.GetCapacity().GetPerEnvironmentLimits() == nil {
		return errors.New("resource grant is incomplete")
	}
	system := grant.GetSystemResources().GetCeiling()
	perEnvironment := grant.GetCapacity().GetPerEnvironmentLimits()
	if uint64(system.GetCpuMillis()) > host.CPUMillis || system.GetMemoryBytes() > host.MemoryBytes ||
		uint64(system.GetPids()) > host.PIDs || system.GetDiskBytes() > host.DiskBytes ||
		uint64(perEnvironment.GetCpuMillis()) > host.CPUMillis || perEnvironment.GetMemoryBytes() > host.MemoryBytes ||
		uint64(perEnvironment.GetPids()) > host.PIDs || perEnvironment.GetDiskBytes() > host.DiskBytes {
		return errors.New("approved resources exceed VM capacity")
	}
	return nil
}

func dynamicHostLoopbackWithin(approved, requested *appliancev1.DynamicHostLoopbackGrant) bool {
	if approved == nil {
		return true
	}
	if requested == nil || approved.GetConnectionBounds() == nil || requested.GetConnectionBounds() == nil {
		return false
	}
	a, r := approved, requested
	bounds := a.GetConnectionBounds()
	return a.GetMaxActiveLeases() > 0 && a.GetMaxActiveLeases() <= r.GetMaxActiveLeases() && a.GetMaxActiveLeases() <= 32 &&
		a.GetMaxActiveLeasesPerAllocation() > 0 && a.GetMaxActiveLeasesPerAllocation() <= a.GetMaxActiveLeases() && a.GetMaxActiveLeasesPerAllocation() <= r.GetMaxActiveLeasesPerAllocation() && a.GetMaxActiveLeasesPerAllocation() <= 8 &&
		a.GetMaxLeaseSeconds() > 0 && a.GetMaxLeaseSeconds() <= r.GetMaxLeaseSeconds() && a.GetMaxLeaseSeconds() <= 3600 &&
		a.GetMaxConcurrentConnections() > 0 && a.GetMaxConcurrentConnections() <= r.GetMaxConcurrentConnections() && a.GetMaxConcurrentConnections() <= 128 &&
		a.GetMaxConcurrentConnectionsPerLease() > 0 && a.GetMaxConcurrentConnectionsPerLease() <= a.GetMaxConcurrentConnections() && a.GetMaxConcurrentConnectionsPerLease() <= r.GetMaxConcurrentConnectionsPerLease() && a.GetMaxConcurrentConnectionsPerLease() <= 16 &&
		bounds.GetMaxFrameBytes() > 0 && bounds.GetMaxFrameBytes() <= r.GetConnectionBounds().GetMaxFrameBytes() && bounds.GetMaxFrameBytes() <= 64<<10 &&
		bounds.GetMaxTotalBytes() >= uint64(bounds.GetMaxFrameBytes()) && bounds.GetMaxTotalBytes() <= r.GetConnectionBounds().GetMaxTotalBytes() && bounds.GetMaxTotalBytes() <= 1<<30 &&
		bounds.GetDeadlineSeconds() > 0 && bounds.GetDeadlineSeconds() <= r.GetConnectionBounds().GetDeadlineSeconds() && bounds.GetDeadlineSeconds() <= 86400 &&
		bounds.GetIdleSeconds() > 0 && bounds.GetIdleSeconds() <= r.GetConnectionBounds().GetIdleSeconds() && bounds.GetIdleSeconds() <= 300 &&
		bounds.GetMaxConcurrentStreams() == a.GetMaxConcurrentConnectionsPerLease() && r.GetConnectionBounds().GetMaxConcurrentStreams() == r.GetMaxConcurrentConnectionsPerLease() &&
		bounds.GetMaxUseCount() > 0 && bounds.GetMaxUseCount() <= r.GetConnectionBounds().GetMaxUseCount() && bounds.GetMaxUseCount() <= 4096
}

func limitsLTE(left, right *hostv1.ResourceLimits) bool {
	return left != nil && right != nil && left.GetCpuMillis() <= right.GetCpuMillis() &&
		left.GetMemoryBytes() <= right.GetMemoryBytes() && left.GetPids() <= right.GetPids() && left.GetDiskBytes() <= right.GetDiskBytes()
}

func limitsPositive(value *hostv1.ResourceLimits) bool {
	return value != nil && value.GetCpuMillis() > 0 && value.GetMemoryBytes() > 0 && value.GetPids() > 0 && value.GetDiskBytes() > 0
}

func frontendWithin(approved, requested *appliancev1.ControllerFrontend) bool {
	if approved.GetTransport() != requested.GetTransport() ||
		approved.GetExposure() != requested.GetExposure() || approved.GetBounds() == nil || requested.GetBounds() == nil {
		return false
	}
	a, r := approved.GetBounds(), requested.GetBounds()
	return a.GetMaxFrameBytes() <= r.GetMaxFrameBytes() && a.GetMaxTotalBytes() <= r.GetMaxTotalBytes() &&
		a.GetDeadlineSeconds() <= r.GetDeadlineSeconds() && a.GetIdleSeconds() <= r.GetIdleSeconds() &&
		a.GetMaxConcurrentStreams() <= r.GetMaxConcurrentStreams() && a.GetMaxUseCount() <= r.GetMaxUseCount()
}
