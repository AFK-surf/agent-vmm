//go:build darwin

package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
)

func (controller *Controller) removeApplianceData(ctx context.Context, connection *grpc.ClientConn, installation store.ApplianceInstallation) error {
	client := vmmdv1.NewVMMDServiceClient(connection)
	_, deleteErr := client.DeleteSystemApplianceData(ctx, &vmmdv1.DeleteSystemApplianceDataRequest{
		RequestId:      derivedRequestID("delete-system-data-" + installation.ID + "-" + installation.DataVolumeID),
		InstallationId: installation.ID, DataVolumeId: installation.DataVolumeID,
	})
	closeErr := connection.Close()
	if deleteErr != nil || closeErr != nil {
		return errors.Join(deleteErr, closeErr)
	}
	return controller.applianceSecrets.DeleteInstallation(ctx, installation.ID)
}

func systemNamespaceID(installationID string) string {
	digest := sha256.Sum256([]byte(installationID))
	return "system." + hex.EncodeToString(digest[:16])
}

func systemContainerID() string { return "payload" }
