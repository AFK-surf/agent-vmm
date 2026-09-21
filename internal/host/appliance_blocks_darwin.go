//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func (controller *Controller) configureApplianceBlocks(ctx context.Context) error {
	// Host blocks exist only for the schema-23 conversion from the old layout.
	// Remove this attachment path after supported installations contain no
	// ApplianceVolumeLayoutHostBlock rows.
	installations, err := controller.store.ListApplianceInstallations(ctx)
	if err != nil {
		return err
	}
	blocks := make([]vmm.ApplianceBlock, 0, len(installations))
	for _, installation := range installations {
		if installation.RegistrationID == "" {
			continue
		}
		volume, err := controller.store.GetApplianceVolume(ctx, installation.ID)
		if err != nil {
			log.Printf("skip appliance block installation=%s: %v", installation.ID, controller.failAppliance(ctx, installation, "DATA_VOLUME_CONVERSION_FAILED", err))
			continue
		}
		if volume.Layout == store.ApplianceVolumeLayoutGuestImage {
			if err := controller.removeLegacyApplianceImage(volume.VolumeID); err != nil {
				log.Printf("retry legacy appliance image cleanup installation=%s: %v", installation.ID, err)
			}
			continue
		}
		if volume.Layout != store.ApplianceVolumeLayoutHostBlock {
			log.Printf("skip appliance block installation=%s: %v", installation.ID, controller.failAppliance(ctx, installation, "DATA_VOLUME_LAYOUT_INVALID", errors.New("appliance data volume layout is invalid")))
			continue
		}
		block, err := controller.legacyApplianceBlock(installation, volume)
		if err != nil {
			log.Printf("skip appliance block installation=%s: %v", installation.ID, controller.failAppliance(ctx, installation, "DATA_VOLUME_CONVERSION_FAILED", err))
			continue
		}
		if block.Initializing {
			if err := controller.store.MarkApplianceVolumeGuestImage(ctx, installation.ID, volume.VolumeID); err != nil {
				log.Printf("skip appliance block installation=%s: %v", installation.ID, controller.failAppliance(ctx, installation, "DATA_VOLUME_CONVERSION_FAILED", err))
				continue
			}
			if err := controller.removeLegacyApplianceImage(volume.VolumeID); err != nil {
				log.Printf("retry legacy appliance image cleanup installation=%s: %v", installation.ID, err)
			}
			continue
		}
		blocks = append(blocks, block)
	}
	return controller.manager.ConfigureApplianceBlocks(blocks)
}

func (controller *Controller) legacyApplianceBlock(installation store.ApplianceInstallation, volume store.ApplianceVolume) (vmm.ApplianceBlock, error) {
	if volume.Bytes < 1<<30 || volume.Bytes > 64<<30 {
		return vmm.ApplianceBlock{}, errors.New("appliance data volume is outside the fixed size bound")
	}
	path := filepath.Join(controller.instanceDirectory, "appliances", volume.VolumeID+".luks")
	marker := path + ".initializing"
	info, err := os.Stat(path)
	if err != nil {
		return vmm.ApplianceBlock{}, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(volume.Bytes) {
		return vmm.ApplianceBlock{}, errors.New("existing appliance data image has an unexpected size")
	}
	_, markerErr := os.Stat(marker)
	initializing := markerErr == nil
	if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
		return vmm.ApplianceBlock{}, markerErr
	}
	return vmm.ApplianceBlock{InstallationID: installation.ID, VolumeID: volume.VolumeID, Identity: volume.Identity,
		Path: path, DeviceIdentifier: applianceDeviceIdentifier(volume.VolumeID), Bytes: int64(volume.Bytes), Initializing: initializing}, nil
}

func (controller *Controller) finishApplianceBlockConversion(ctx context.Context, volume store.ApplianceVolume) error {
	if volume.Layout != store.ApplianceVolumeLayoutHostBlock {
		return nil
	}
	if err := controller.store.MarkApplianceVolumeGuestImage(ctx, volume.InstallationID, volume.VolumeID); err != nil {
		return err
	}
	return controller.removeLegacyApplianceImage(volume.VolumeID)
}

func (controller *Controller) removeLegacyApplianceImage(volumeID string) error {
	path := filepath.Join(controller.instanceDirectory, "appliances", volumeID+".luks")
	var result error
	for _, target := range []string{path + ".initializing", path} {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	if result != nil {
		return result
	}
	if _, err := os.Stat(filepath.Dir(path)); err == nil {
		return syncHostDirectory(filepath.Dir(path))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (controller *Controller) removeApplianceAssets(ctx context.Context, installationID, volumeID string) error {
	result := controller.removeLegacyApplianceImage(volumeID)
	if controller.applianceSecrets != nil {
		result = errors.Join(result, controller.applianceSecrets.DeleteInstallation(ctx, installationID))
	}
	return result
}

func (controller *Controller) removeApplianceData(ctx context.Context, installation store.ApplianceInstallation) (result error) {
	volume, err := controller.store.GetApplianceVolume(ctx, installation.ID)
	if err != nil {
		return err
	}
	switch volume.Layout {
	case store.ApplianceVolumeLayoutHostBlock:
	case store.ApplianceVolumeLayoutGuestImage:
		startedVM := false
		if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
			if _, err := controller.vmLifecycle.Start(ctx); err != nil {
				return err
			}
			startedVM = true
		}
		defer func() {
			if startedVM {
				_, err := controller.vmLifecycle.Stop(context.Background(), false)
				result = errors.Join(result, err)
			}
		}()
		client, connection, err := controller.guestClient(ctx)
		if err != nil {
			return err
		}
		_, deleteErr := client.DeleteSystemApplianceData(ctx, &vmmdv1.DeleteSystemApplianceDataRequest{
			RequestId:      derivedRequestID("delete-system-data-" + installation.ID + "-" + volume.VolumeID),
			InstallationId: installation.ID, DataVolumeId: volume.VolumeID,
		})
		closeErr := connection.Close()
		if deleteErr != nil || closeErr != nil {
			return errors.Join(deleteErr, closeErr)
		}
	default:
		return errors.New("appliance data volume layout is invalid")
	}
	return controller.removeApplianceAssets(ctx, installation.ID, volume.VolumeID)
}

func applianceDeviceIdentifier(volumeID string) string {
	digest := sha256.Sum256([]byte(volumeID))
	return "AVMM_" + hex.EncodeToString(digest[:7])
}

func systemNamespaceID(installationID string) string {
	digest := sha256.Sum256([]byte(installationID))
	return "system." + hex.EncodeToString(digest[:16])
}

func systemContainerID() string { return "payload" }

func newLUKSIdentity() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:]), nil
}
