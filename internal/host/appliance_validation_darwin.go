//go:build darwin

package host

import (
	"crypto/subtle"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

func validateBootstrapReceipt(receipt *appliancev1.BootstrapReceipt, installationID, bootstrapID string, nonce []byte) bool {
	if receipt == nil {
		return false
	}
	return receipt.GetInstallationId() == installationID && receipt.GetBootstrapId() == bootstrapID &&
		subtle.ConstantTimeCompare(receipt.GetNonce(), nonce) == 1
}
