//go:build darwin

package host

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"errors"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Controller) validateAppliancePublisher(ctx context.Context, installation store.ApplianceInstallation, artifact store.ApplianceArtifact) error {
	if artifact.PublisherKeyID == "" || artifact.PublisherKeyID != installation.PublisherKeyID {
		return errors.New("artifact publisher identity does not match the installation")
	}
	publisher, err := controller.store.GetAppliancePublisher(ctx, artifact.PublisherKeyID)
	if err != nil || publisher.Revoked || publisher.KeyID != artifact.PublisherKeyID || len(publisher.PublicKey) != ed25519.PublicKeySize {
		return errors.Join(errors.New("artifact publisher is not trusted"), err)
	}
	return nil
}

func validateBootstrapReceipt(receipt *appliancev1.BootstrapReceipt, installationID, bootstrapID string, nonce []byte) bool {
	if receipt == nil {
		return false
	}
	return receipt.GetInstallationId() == installationID && receipt.GetBootstrapId() == bootstrapID &&
		subtle.ConstantTimeCompare(receipt.GetNonce(), nonce) == 1
}
