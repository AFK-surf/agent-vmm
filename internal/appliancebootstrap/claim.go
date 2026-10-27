package appliancebootstrap

import (
	"errors"
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"time"
)

func VerifyLaunchClaim(claim *appliancev1.ApplianceLaunchClaim, now time.Time) error {
	device := claim.GetDeviceIdentity()
	if device.GetDeviceId() == "" || device.GetSignatureSuite() != trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 || claim.GetInstallationId() == "" || claim.GetArtifactId() == "" || claim.GetRegistrationId() == "" || claim.GetBootId() == "" || claim.GetSystemNamespace() == "" || claim.GetContainerInstance() == "" || len(claim.GetBootNonce()) != 32 || claim.GetConfigRevision() == 0 || claim.GetApprovedGrant() == nil || device.GetRootKeyRevision() == 0 {
		return errors.New("launch claim is incomplete")
	}
	if claim.GetExpiresUnixMillis() <= now.UnixMilli() || claim.GetExpiresUnixMillis() > now.Add(5*time.Minute).UnixMilli() {
		return errors.New("launch claim lifetime is invalid")
	}
	return nil
}

func ValidateEnvelope(claim *appliancev1.ApplianceLaunchClaim, envelope *appliancev1.OpaqueBootstrapEnvelope) error {
	if envelope.GetBootstrapId() == "" || envelope.GetInstallationId() != claim.GetInstallationId() || envelope.GetBootstrapId() != claim.GetBootstrapId() || len(envelope.GetNonce()) != 32 || len(envelope.GetCiphertext()) == 0 || len(envelope.GetCiphertext()) > 1<<20 || envelope.GetExpiresUnixMillis() != claim.GetExpiresUnixMillis() {
		return errors.New("bootstrap envelope identity is invalid")
	}
	return nil
}
