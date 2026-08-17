//go:build linux

package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

const applianceMountRoot = "/run/agent-vmm/appliances"

var luksIdentity = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type applianceVolume struct {
	device, mapper, mountPath, identity string
}

type applianceCommand func(context.Context, []byte, string, ...string) ([]byte, error)

type applianceVolumeManager struct {
	deviceByIdentifier func(string) (string, error)
	command            applianceCommand
	mkdir              func(string, os.FileMode) error
	mount              func(string, string, string, uintptr, string) error
	unmount            func(string, int) error
}

func newApplianceVolumeManager() applianceVolumeManager {
	return applianceVolumeManager{
		deviceByIdentifier: applianceDeviceByIdentifier,
		command: func(ctx context.Context, input []byte, name string, arguments ...string) ([]byte, error) {
			command := exec.CommandContext(ctx, name, arguments...)
			command.Stdin = bytes.NewReader(input)
			output, err := command.CombinedOutput()
			if len(output) > 4096 {
				output = output[:4096]
			}
			return output, err
		},
		mkdir: os.MkdirAll, mount: unix.Mount, unmount: unix.Unmount,
	}
}

func (manager applianceVolumeManager) unlock(ctx context.Context, installationID, deviceIdentifier, identity string, key []byte, initialize bool) (applianceVolume, error) {
	if err := validateID("installation_id", installationID); err != nil {
		return applianceVolume{}, err
	}
	if len(deviceIdentifier) == 0 || len(deviceIdentifier) > 20 || !opaqueID.MatchString(deviceIdentifier) {
		return applianceVolume{}, errors.New("data device identifier is invalid")
	}
	if !luksIdentity.MatchString(identity) {
		return applianceVolume{}, errors.New("data volume identity is invalid")
	}
	if len(key) != 32 && len(key) != 64 {
		return applianceVolume{}, errors.New("data volume key must contain 32 or 64 bytes")
	}
	defer clearBytes(key)
	device, err := manager.deviceByIdentifier(deviceIdentifier)
	if err != nil {
		return applianceVolume{}, newSealedVolumeError("DATA_DEVICE_MISSING", err)
	}
	current, probeErr := manager.command(ctx, nil, "/sbin/cryptsetup", "luksUUID", device)
	currentIdentity := strings.TrimSpace(string(current))
	if probeErr != nil {
		if !initialize {
			return applianceVolume{}, newSealedVolumeError("LUKS_HEADER_MISSING", probeErr)
		}
		if _, err := manager.command(ctx, key, "/sbin/cryptsetup", "luksFormat", "--batch-mode", "--type", "luks2", "--uuid", identity, "--key-file", "-", device); err != nil {
			return applianceVolume{}, newSealedVolumeError("LUKS_FORMAT_FAILED", err)
		}
		currentIdentity = identity
	}
	if currentIdentity != identity {
		return applianceVolume{}, newSealedVolumeError("VOLUME_IDENTITY_MISMATCH", errors.New("attached LUKS identity does not match installation"))
	}
	mapper := applianceMapperName(installationID)
	if _, err := manager.command(ctx, key, "/sbin/cryptsetup", "open", "--type", "luks2", "--key-file", "-", device, mapper); err != nil {
		return applianceVolume{}, newSealedVolumeError("VOLUME_UNLOCK_FAILED", err)
	}
	mapped := filepath.Join("/dev/mapper", mapper)
	if initialize {
		if _, err := manager.command(ctx, nil, "/sbin/mkfs.ext4", "-F", "-q", "-E", "lazy_itable_init=0,lazy_journal_init=0", mapped); err != nil {
			_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
			return applianceVolume{}, newSealedVolumeError("FILESYSTEM_FORMAT_FAILED", err)
		}
	}
	mountPath := filepath.Join(applianceMountRoot, installationID, "data")
	if err := manager.mkdir(mountPath, 0o700); err != nil {
		_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
		return applianceVolume{}, err
	}
	if err := manager.mount(mapped, mountPath, "ext4", unix.MS_NOATIME|unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
		_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
		return applianceVolume{}, newSealedVolumeError("FILESYSTEM_MOUNT_FAILED", err)
	}
	return applianceVolume{device: device, mapper: mapper, mountPath: mountPath, identity: identity}, nil
}

func (manager applianceVolumeManager) close(ctx context.Context, volume applianceVolume) error {
	var result error
	if err := manager.unmount(volume.mountPath, 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		result = fmt.Errorf("unmount appliance data: %w", err)
	}
	if _, err := manager.command(ctx, nil, "/sbin/cryptsetup", "close", volume.mapper); err != nil && result == nil {
		result = errors.New("close appliance data mapping")
	}
	return result
}

func applianceDeviceByIdentifier(identifier string) (string, error) {
	serials, err := filepath.Glob("/sys/block/vd*/serial")
	if err != nil {
		return "", err
	}
	for _, serial := range serials {
		value, readErr := os.ReadFile(serial)
		if readErr == nil && strings.Trim(strings.TrimSpace(string(value)), "\x00") == identifier {
			return "/dev/" + filepath.Base(filepath.Dir(serial)), nil
		}
	}
	return "", errors.New("bound appliance block device is unavailable")
}

func applianceMapperName(installationID string) string {
	digest := sha256.Sum256([]byte(installationID))
	return "avmm-" + hex.EncodeToString(digest[:12])
}

type sealedVolumeError struct{ reason string }

func (err sealedVolumeError) Error() string { return "appliance data volume is sealed: " + err.reason }

func newSealedVolumeError(reason string, _ error) error { return sealedVolumeError{reason: reason} }

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
