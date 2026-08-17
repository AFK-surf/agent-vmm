//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
)

func (controller *Controller) configureApplianceBlocks(ctx context.Context) error {
	installations, err := controller.store.ListApplianceInstallations(ctx)
	if err != nil {
		return err
	}
	blocks := make([]vmm.ApplianceBlock, 0, len(installations))
	for _, installation := range installations {
		if installation.RegistrationID == "" {
			continue
		}
		block, err := controller.prepareApplianceBlock(ctx, installation)
		if err != nil {
			return err
		}
		blocks = append(blocks, block)
	}
	return controller.manager.ConfigureApplianceBlocks(blocks)
}

func (controller *Controller) prepareApplianceBlock(ctx context.Context, installation store.ApplianceInstallation) (vmm.ApplianceBlock, error) {
	volume, err := controller.store.GetApplianceVolume(ctx, installation.ID)
	if err != nil {
		return vmm.ApplianceBlock{}, err
	}
	if volume.Bytes < 1<<30 || volume.Bytes > 64<<30 {
		return vmm.ApplianceBlock{}, errors.New("appliance data volume is outside the fixed size bound")
	}
	directory := filepath.Join(controller.instanceDirectory, "appliances")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return vmm.ApplianceBlock{}, err
	}
	path := filepath.Join(directory, volume.VolumeID+".luks")
	marker := path + ".initializing"
	initializing := false
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err == nil {
		if err := file.Truncate(int64(volume.Bytes)); err != nil {
			file.Close()
			return vmm.ApplianceBlock{}, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return vmm.ApplianceBlock{}, err
		}
		if err := file.Close(); err != nil {
			return vmm.ApplianceBlock{}, err
		}
		markerFile, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return vmm.ApplianceBlock{}, err
		}
		if err := markerFile.Close(); err != nil {
			return vmm.ApplianceBlock{}, err
		}
		initializing = true
	} else if !errors.Is(err, os.ErrExist) {
		return vmm.ApplianceBlock{}, err
	} else {
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != int64(volume.Bytes) {
			return vmm.ApplianceBlock{}, errors.New("existing appliance data image has an unexpected size")
		}
		_, markerErr := os.Stat(marker)
		initializing = markerErr == nil
	}
	return vmm.ApplianceBlock{InstallationID: installation.ID, VolumeID: volume.VolumeID, Identity: volume.Identity,
		Path: path, DeviceIdentifier: applianceDeviceIdentifier(volume.VolumeID), Bytes: int64(volume.Bytes), Initializing: initializing}, nil
}

func (controller *Controller) finishApplianceBlock(block vmm.ApplianceBlock) error {
	if !block.Initializing {
		return nil
	}
	if err := os.Remove(block.Path + ".initializing"); err != nil {
		return err
	}
	return syncHostDirectory(filepath.Dir(block.Path))
}

func (controller *Controller) removeApplianceAssets(ctx context.Context, installationID, volumeID string) error {
	path := filepath.Join(controller.instanceDirectory, "appliances", volumeID+".luks")
	var result error
	for _, target := range []string{path + ".initializing", path} {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	if controller.applianceSecrets != nil {
		result = errors.Join(result, controller.applianceSecrets.DeleteInstallation(ctx, installationID))
	}
	if result == nil {
		if _, err := os.Stat(filepath.Dir(path)); err == nil {
			result = syncHostDirectory(filepath.Dir(path))
		} else if !errors.Is(err, os.ErrNotExist) {
			result = err
		}
	}
	return result
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
