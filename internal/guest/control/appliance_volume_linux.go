//go:build linux

package control

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	guestquota "github.com/AFK-surf/agent-vmm/internal/guest/quota"
)

const applianceMountRoot = "/run/agent-vmm/appliances"
const applianceDataRoot = "/state/appliance-data"
const applianceStagingPrefix = ".agent-vmm-appliance-"

type applianceVolume struct{ mountPath string }

type applianceVolumeManager struct {
	root, device string
	mu           *sync.Mutex
	project      func(string) (uint32, error)
	assign       func(string, uint32) error
	limit        func(string, uint32, uint64) error
}

func newApplianceVolumeManager(device string) applianceVolumeManager {
	return applianceVolumeManager{root: applianceDataRoot, device: device, mu: new(sync.Mutex), project: projectID, assign: assignProject, limit: setProjectQuota}
}

func (manager applianceVolumeManager) prepare(installationID, volumeID string, size uint64) (applianceVolume, error) {
	if err := validateID("installation_id", installationID); err != nil {
		return applianceVolume{}, err
	}
	if err := validateID("data_volume_id", volumeID); err != nil {
		return applianceVolume{}, err
	}
	if size < 1<<30 || size > 64<<30 {
		return applianceVolume{}, errors.New("data volume size is invalid")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	directory := filepath.Join(manager.root, installationID)
	for _, path := range []string{manager.root, directory} {
		if err := ensureDirectory(path, 0o700); err != nil {
			return applianceVolume{}, err
		}
	}
	path := filepath.Join(directory, volumeID)
	info, err := os.Lstat(path)
	var project uint32
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return applianceVolume{}, errors.New("appliance data path is not a directory")
		}
		project, err = manager.project(path)
		if err != nil {
			return applianceVolume{}, err
		}
		if project < applianceProjectBase {
			return applianceVolume{}, errors.New("appliance data quota ownership is invalid")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		project, err = manager.allocateProject()
		if err != nil {
			return applianceVolume{}, err
		}
		staging, err := os.MkdirTemp(directory, applianceStagingPrefix)
		if err != nil {
			return applianceVolume{}, err
		}
		published := false
		defer func() {
			if !published {
				_ = os.RemoveAll(staging)
				_ = manager.limit(manager.device, project, 0)
			}
		}()
		if err := manager.assign(staging, project); err != nil {
			return applianceVolume{}, err
		}
		if err := manager.limit(manager.device, project, size); err != nil {
			return applianceVolume{}, err
		}
		if err := os.Chown(staging, systemContainerUID, systemContainerUID); err != nil {
			return applianceVolume{}, err
		}
		if err := os.Rename(staging, path); err != nil {
			return applianceVolume{}, err
		}
		published = true
		if err := syncFilesystem(directory); err != nil {
			return applianceVolume{}, err
		}
		return applianceVolume{mountPath: path}, nil
	} else {
		return applianceVolume{}, err
	}
	if err := manager.limit(manager.device, project, size); err != nil {
		return applianceVolume{}, err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return applianceVolume{}, err
	}
	if err := os.Chown(path, systemContainerUID, systemContainerUID); err != nil {
		return applianceVolume{}, err
	}
	return applianceVolume{mountPath: path}, nil
}

func (manager applianceVolumeManager) allocateProject() (uint32, error) {
	installations, err := os.ReadDir(manager.root)
	if err != nil {
		return 0, err
	}
	used := make(map[uint32]bool)
	for _, installation := range installations {
		if !installation.IsDir() {
			return 0, errors.New("appliance data owner is not a directory")
		}
		directory := filepath.Join(manager.root, installation.Name())
		volumes, err := os.ReadDir(directory)
		if err != nil {
			return 0, err
		}
		for _, volume := range volumes {
			if strings.HasPrefix(volume.Name(), applianceStagingPrefix) {
				staging := filepath.Join(directory, volume.Name())
				project, err := manager.project(staging)
				if err != nil {
					return 0, err
				}
				if err := os.RemoveAll(staging); err != nil {
					return 0, err
				}
				if project >= applianceProjectBase {
					if err := manager.limit(manager.device, project, 0); err != nil {
						return 0, err
					}
				}
				continue
			}
			if !volume.IsDir() {
				return 0, errors.New("appliance data volume is not a directory")
			}
			project, err := manager.project(filepath.Join(directory, volume.Name()))
			if err != nil {
				return 0, err
			}
			used[project] = true
		}
	}
	for project := uint32(applianceProjectBase); project != 0; project++ {
		if !used[project] {
			return project, nil
		}
	}
	return 0, errors.New("appliance data quota projects exhausted")
}

func (manager applianceVolumeManager) delete(installationID, volumeID string) error {
	if err := validateID("installation_id", installationID); err != nil {
		return err
	}
	if err := validateID("data_volume_id", volumeID); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	directory := filepath.Join(manager.root, installationID)
	if info, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	} else if !info.IsDir() {
		return errors.New("appliance data owner is not a directory")
	}
	path := filepath.Join(directory, volumeID)
	if info, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	} else if !info.IsDir() {
		return errors.New("appliance data volume is not a directory")
	}
	project, err := manager.project(path)
	if err != nil {
		return err
	}
	if project < applianceProjectBase {
		return errors.New("appliance data quota ownership is invalid")
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := syncFilesystem(directory); err != nil {
		return err
	}
	return manager.limit(manager.device, project, 0)
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func (manager applianceVolumeManager) commitments() (map[uint32]namespaceDiskCommitment, error) {
	facts := make(map[uint32]namespaceDiskCommitment)
	installations, err := os.ReadDir(manager.root)
	if errors.Is(err, os.ErrNotExist) {
		return facts, nil
	}
	if err != nil {
		return nil, err
	}
	for _, installation := range installations {
		if !installation.IsDir() {
			return nil, errors.New("appliance data owner is not a directory")
		}
		directory := filepath.Join(manager.root, installation.Name())
		volumes, err := os.ReadDir(directory)
		if err != nil {
			return nil, err
		}
		for _, volume := range volumes {
			if !volume.IsDir() {
				return nil, errors.New("appliance data volume is not a directory")
			}
			project, err := manager.project(filepath.Join(directory, volume.Name()))
			if err != nil {
				return nil, err
			}
			state, err := guestquota.Read(manager.device, project)
			if err != nil {
				return nil, err
			}
			facts[project] = namespaceDiskCommitment{namespace: installation.Name() + "/" + volume.Name(), limit: state.HardLimitBytes, usage: state.UsageBytes}
		}
	}
	return facts, nil
}
