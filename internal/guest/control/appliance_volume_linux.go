//go:build linux

package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

const applianceMountRoot = "/run/agent-vmm/appliances"
const applianceImageRoot = "/state/appliances"

var luksIdentity = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type applianceVolume struct {
	device, mapper, mountPath, identity, imagePath string
}

type applianceCommand func(context.Context, []byte, string, ...string) ([]byte, error)

type applianceVolumeManager struct {
	deviceByIdentifier func(string) (string, error)
	command            applianceCommand
	mkdir              func(string, os.FileMode) error
	mount              func(string, string, string, uintptr, string) error
	unmount            func(string, int) error
	imageRoot          string
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
		mkdir: os.MkdirAll, mount: unix.Mount, unmount: unix.Unmount, imageRoot: applianceImageRoot,
	}
}

func (manager applianceVolumeManager) unlock(ctx context.Context, installationID, volumeID, legacyDeviceIdentifier, identity string, size uint64, key []byte) (applianceVolume, error) {
	if err := validateID("installation_id", installationID); err != nil {
		return applianceVolume{}, err
	}
	if err := validateID("data_volume_id", volumeID); err != nil {
		return applianceVolume{}, err
	}
	if size < 1<<30 || size > 64<<30 {
		return applianceVolume{}, errors.New("data volume size is invalid")
	}
	if legacyDeviceIdentifier != "" && (len(legacyDeviceIdentifier) > 20 || !opaqueID.MatchString(legacyDeviceIdentifier)) {
		return applianceVolume{}, errors.New("legacy data device identifier is invalid")
	}
	if !luksIdentity.MatchString(identity) {
		return applianceVolume{}, errors.New("data volume identity is invalid")
	}
	if len(key) != 32 && len(key) != 64 {
		return applianceVolume{}, errors.New("data volume key must contain 32 or 64 bytes")
	}
	defer clearBytes(key)
	image, initialize, err := manager.prepareImage(ctx, installationID, volumeID, legacyDeviceIdentifier, int64(size))
	if err != nil {
		return applianceVolume{}, err
	}
	loopOutput, err := manager.command(ctx, nil, "/sbin/losetup", "--find", "--show", image)
	if err != nil {
		return applianceVolume{}, newSealedVolumeError("LOOP_ATTACH_FAILED", err)
	}
	device := strings.TrimSpace(string(loopOutput))
	if !strings.HasPrefix(device, "/dev/loop") {
		return applianceVolume{}, newSealedVolumeError("LOOP_ATTACH_FAILED", errors.New("loop device response is invalid"))
	}
	detach := func() { _, _ = manager.command(context.Background(), nil, "/sbin/losetup", "-d", device) }
	current, probeErr := manager.command(ctx, nil, "/sbin/cryptsetup", "luksUUID", device)
	currentIdentity := strings.TrimSpace(string(current))
	if probeErr != nil {
		if !initialize {
			detach()
			return applianceVolume{}, newSealedVolumeError("LUKS_HEADER_MISSING", probeErr)
		}
		if _, err := manager.command(ctx, key, "/sbin/cryptsetup", "luksFormat", "--batch-mode", "--type", "luks2", "--uuid", identity, "--key-file", "-", device); err != nil {
			detach()
			return applianceVolume{}, newSealedVolumeError("LUKS_FORMAT_FAILED", err)
		}
		currentIdentity = identity
	}
	if currentIdentity != identity {
		detach()
		return applianceVolume{}, newSealedVolumeError("VOLUME_IDENTITY_MISMATCH", errors.New("attached LUKS identity does not match installation"))
	}
	mapper := applianceMapperName(installationID)
	if _, err := manager.command(ctx, key, "/sbin/cryptsetup", "open", "--type", "luks2", "--key-file", "-", device, mapper); err != nil {
		detach()
		return applianceVolume{}, newSealedVolumeError("VOLUME_UNLOCK_FAILED", err)
	}
	mapped := filepath.Join("/dev/mapper", mapper)
	if initialize {
		if _, err := manager.command(ctx, nil, "/sbin/mkfs.ext4", "-F", "-q", "-E", "lazy_itable_init=0,lazy_journal_init=0", mapped); err != nil {
			_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
			detach()
			return applianceVolume{}, newSealedVolumeError("FILESYSTEM_FORMAT_FAILED", err)
		}
	}
	mountPath := filepath.Join(applianceMountRoot, installationID, "data")
	if err := manager.mkdir(mountPath, 0o700); err != nil {
		_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
		detach()
		return applianceVolume{}, err
	}
	if err := manager.mount(mapped, mountPath, "ext4", unix.MS_NOATIME|unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
		_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
		detach()
		return applianceVolume{}, newSealedVolumeError("FILESYSTEM_MOUNT_FAILED", err)
	}
	if initialize {
		if err := os.Remove(image + ".initializing"); err != nil {
			_ = manager.unmount(mountPath, 0)
			_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
			detach()
			return applianceVolume{}, err
		}
		if err := syncFilesystem(filepath.Dir(image)); err != nil {
			_ = manager.unmount(mountPath, 0)
			_, _ = manager.command(context.Background(), nil, "/sbin/cryptsetup", "close", mapper)
			detach()
			return applianceVolume{}, err
		}
	}
	return applianceVolume{device: device, mapper: mapper, mountPath: mountPath, identity: identity, imagePath: image}, nil
}

func (manager applianceVolumeManager) close(ctx context.Context, volume applianceVolume) error {
	var result error
	if err := manager.unmount(volume.mountPath, 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		result = fmt.Errorf("unmount appliance data: %w", err)
	}
	if _, err := manager.command(ctx, nil, "/sbin/cryptsetup", "close", volume.mapper); err != nil && result == nil {
		result = errors.New("close appliance data mapping")
	}
	if _, err := manager.command(ctx, nil, "/sbin/losetup", "-d", volume.device); err != nil && result == nil {
		result = errors.New("detach appliance data image")
	}
	return result
}

func (manager applianceVolumeManager) delete(installationID, volumeID string) error {
	if err := validateID("installation_id", installationID); err != nil {
		return err
	}
	if err := validateID("data_volume_id", volumeID); err != nil {
		return err
	}
	directory := filepath.Join(manager.imageRoot, installationID)
	path := filepath.Join(directory, volumeID+".luks")
	var result error
	for _, target := range []string{path + ".converting", path + ".initializing", path} {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	if result == nil {
		if _, err := os.Stat(directory); err == nil {
			result = syncFilesystem(directory)
		} else if !errors.Is(err, os.ErrNotExist) {
			result = err
		}
	}
	if result == nil {
		if err := os.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = err
		} else if err == nil {
			result = syncFilesystem(manager.imageRoot)
		}
	}
	return result
}

func (manager applianceVolumeManager) prepareImage(ctx context.Context, installationID, volumeID, legacyDeviceIdentifier string, size int64) (string, bool, error) {
	if err := manager.mkdir(manager.imageRoot, 0o700); err != nil {
		return "", false, err
	}
	directory := filepath.Join(manager.imageRoot, installationID)
	if err := manager.mkdir(directory, 0o700); err != nil {
		return "", false, err
	}
	path := filepath.Join(directory, volumeID+".luks")
	if info, err := os.Stat(path); err == nil {
		if !info.Mode().IsRegular() || info.Size() != size {
			return "", false, newSealedVolumeError("VOLUME_IMAGE_MISMATCH", errors.New("existing data image has an unexpected size"))
		}
		_, markerErr := os.Stat(path + ".initializing")
		return path, markerErr == nil, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	if legacyDeviceIdentifier != "" {
		device, err := manager.deviceByIdentifier(legacyDeviceIdentifier)
		if err != nil {
			return "", false, newSealedVolumeError("LEGACY_DATA_DEVICE_MISSING", err)
		}
		temporary := path + ".converting"
		_ = os.Remove(temporary)
		if err := copySparseImage(ctx, device, temporary, size); err != nil {
			_ = os.Remove(temporary)
			return "", false, newSealedVolumeError("VOLUME_CONVERSION_FAILED", err)
		}
		if err := os.Rename(temporary, path); err != nil {
			return "", false, err
		}
		if err := syncFilesystem(directory); err != nil {
			return "", false, err
		}
		return path, false, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return "", false, err
	}
	if err := file.Truncate(size); err != nil {
		file.Close()
		return "", false, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", false, err
	}
	if err := file.Close(); err != nil {
		return "", false, err
	}
	marker, err := os.OpenFile(path+".initializing", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", false, err
	}
	if err := marker.Close(); err != nil {
		return "", false, err
	}
	if err := syncFilesystem(directory); err != nil {
		return "", false, err
	}
	return path, true, nil
}

func copySparseImage(ctx context.Context, source, target string, size int64) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer output.Close()
	if err := output.Truncate(size); err != nil {
		return err
	}
	buffer := make([]byte, 1<<20)
	zero := make([]byte, len(buffer))
	for offset := int64(0); offset < size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := int64(len(buffer))
		if remaining := size - offset; remaining < length {
			length = remaining
		}
		read, err := input.ReadAt(buffer[:length], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if int64(read) != length {
			return io.ErrUnexpectedEOF
		}
		if !bytes.Equal(buffer[:length], zero[:length]) {
			if _, err := output.WriteAt(buffer[:length], offset); err != nil {
				return err
			}
		}
		offset += length
	}
	return output.Sync()
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
