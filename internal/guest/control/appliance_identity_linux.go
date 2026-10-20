//go:build linux

package control

import (
	"crypto/subtle"
	"errors"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

func validateSystemIdentity(identity *vmmdv1.SystemApplianceIdentity, bootID string, requireInstance bool) error {
	if identity == nil || validateID("installation_id", identity.GetInstallationId()) != nil || validateSystemNamespace(identity.GetNamespaceId()) != nil || validateID("container_id", identity.GetContainerId()) != nil || identity.GetBootId() != bootID || identity.GetImageManifestDigest() == "" || len(identity.GetBootNonce()) != 32 {
		return errors.New("system appliance identity is incomplete or belongs to another boot")
	}
	if requireInstance && identity.GetContainerInstanceId() == "" {
		return errors.New("system appliance container instance is required")
	}
	return nil
}

func sameSystemLaunch(left, right *vmmdv1.SystemApplianceIdentity) bool {
	if left == nil || right == nil || left.GetInstallationId() != right.GetInstallationId() || left.GetBootId() != right.GetBootId() || left.GetNamespaceId() != right.GetNamespaceId() || left.GetContainerId() != right.GetContainerId() || left.GetImageManifestDigest() != right.GetImageManifestDigest() || subtle.ConstantTimeCompare(left.GetBootNonce(), right.GetBootNonce()) != 1 {
		return false
	}
	return left.GetContainerInstanceId() == right.GetContainerInstanceId()
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
