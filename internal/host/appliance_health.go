//go:build darwin || linux

package host

import (
	"strings"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func (controller *Controller) setApplianceHealth(health *appliancev1.ApplianceHealth) {
	if health == nil || health.GetInstallationId() == "" {
		return
	}
	controller.applianceHealthMu.Lock()
	if controller.applianceHealth == nil {
		controller.applianceHealth = make(map[string]*appliancev1.ApplianceHealth)
	}
	controller.applianceHealth[health.GetInstallationId()] = proto.Clone(health).(*appliancev1.ApplianceHealth)
	controller.applianceHealthMu.Unlock()
	if !health.GetHealthy() {
		controller.stopApplianceFrontends(health.GetInstallationId())
	}
}

func (controller *Controller) setPendingApplianceHealth(health *appliancev1.ApplianceHealth) {
	if health == nil || health.GetInstallationId() == "" {
		return
	}
	controller.applianceHealthMu.Lock()
	if controller.pendingApplianceHealth == nil {
		controller.pendingApplianceHealth = make(map[string]*appliancev1.ApplianceHealth)
	}
	controller.pendingApplianceHealth[health.GetInstallationId()] = proto.Clone(health).(*appliancev1.ApplianceHealth)
	controller.applianceHealthMu.Unlock()
}

func (controller *Controller) pendingApplianceHealthProjection(installationID string) *appliancev1.ApplianceHealth {
	controller.applianceHealthMu.Lock()
	defer controller.applianceHealthMu.Unlock()
	if health := controller.pendingApplianceHealth[installationID]; health != nil {
		return proto.Clone(health).(*appliancev1.ApplianceHealth)
	}
	return nil
}

func (controller *Controller) clearPendingApplianceHealth(installationID string) {
	controller.applianceHealthMu.Lock()
	delete(controller.pendingApplianceHealth, installationID)
	controller.applianceHealthMu.Unlock()
}

func (controller *Controller) setApplianceFailure(installation store.ApplianceInstallation, reason string) {
	controller.setApplianceHealth(&appliancev1.ApplianceHealth{InstallationId: installation.ID,
		BootId: installation.CurrentBootID, ContainerInstance: installation.CurrentContainerInstance,
		ConfigRevision: installation.Revision, Healthy: false, Status: reason})
}

func bootstrapFailureReason(err error) string {
	message := err.Error()
	for _, known := range []struct{ fragment, reason string }{
		{"launch claim is expired", "BOOTSTRAP_CLAIM_EXPIRED"},
		{"launch claim lifetime exceeds", "BOOTSTRAP_CLAIM_LIFETIME_INVALID"},
		{"launch claim is incomplete", "BOOTSTRAP_CLAIM_INCOMPLETE"},
		{"launch claim signer is invalid", "BOOTSTRAP_CLAIM_SIGNER_INVALID"},
		{"launch claim signature is invalid", "BOOTSTRAP_CLAIM_SIGNATURE_INVALID"},
		{"durable appliance state is unavailable", "BOOTSTRAP_STATE_UNAVAILABLE"},
		{"core dumps are not disabled", "BOOTSTRAP_CORE_LIMIT_INVALID"},
		{"bootstrap secret leaked", "BOOTSTRAP_SECRET_SURFACE_INVALID"},
		{"secret scan file exceeds", "BOOTSTRAP_SECRET_SCAN_BOUNDED"},
		{"bootstrap envelope was already consumed", "BOOTSTRAP_REPLAY_REJECTED"},
		{"bootstrap seed was already consumed", "BOOTSTRAP_REPLAY_REJECTED"},
		{"bootstrap stream was already consumed", "BOOTSTRAP_REPLAY_REJECTED"},
		{"bootstrap envelope identity is invalid", "BOOTSTRAP_ENVELOPE_IDENTITY_INVALID"},
		{"bootstrap stream was not closed", "BOOTSTRAP_STREAM_CLOSE_INVALID"},
		{"bootstrap stream has an unexpected frame", "BOOTSTRAP_FRAME_INVALID"},
		{"bootstrap receipt or health fence", "BOOTSTRAP_RESPONSE_FENCE_FAILED"},
		{"initial bootstrap envelope is unavailable", "BOOTSTRAP_ENVELOPE_UNAVAILABLE"},
		{"device root signer is unavailable", "BOOTSTRAP_DEVICE_ROOT_UNAVAILABLE"},
		{"system appliance endpoint is unavailable", "BOOTSTRAP_ENDPOINT_UNAVAILABLE"},
	} {
		if strings.Contains(message, known.fragment) {
			return known.reason
		}
	}
	return "BOOTSTRAP_FAILED"
}

func (controller *Controller) applianceHealthProjection(installationID string) *appliancev1.ApplianceHealth {
	controller.applianceHealthMu.Lock()
	defer controller.applianceHealthMu.Unlock()
	if health := controller.applianceHealth[installationID]; health != nil {
		return proto.Clone(health).(*appliancev1.ApplianceHealth)
	}
	return nil
}

func (controller *Controller) clearApplianceHealth(installationID string) {
	controller.stopApplianceFrontends(installationID)
	controller.applianceHealthMu.Lock()
	delete(controller.applianceHealth, installationID)
	delete(controller.applianceTrustRetry, installationID)
	controller.applianceHealthMu.Unlock()
}

func (controller *Controller) clearAllApplianceHealth() {
	controller.applianceHealthMu.Lock()
	clear(controller.applianceHealth)
	clear(controller.pendingApplianceHealth)
	clear(controller.applianceTrustRetry)
	controller.applianceHealthMu.Unlock()
}
