//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func (controller *Owner) removeApplianceData(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation) error {
	_, deleteErr := client.DeleteSystemApplianceData(ctx, &vmmdv1.DeleteSystemApplianceDataRequest{
		RequestId:      opreceipt.DerivedRequestID("delete-system-data-" + installation.ID + "-" + installation.DataVolumeID),
		InstallationId: installation.ID, DataVolumeId: installation.DataVolumeID,
	})
	if deleteErr != nil {
		return deleteErr
	}
	return controller.applianceSecrets.DeleteInstallation(ctx, installation.ID)
}

func systemNamespaceID(installationID string) string {
	digest := sha256.Sum256([]byte(installationID))
	return "system." + hex.EncodeToString(digest[:16])
}

func systemContainerID() string { return "payload" }
