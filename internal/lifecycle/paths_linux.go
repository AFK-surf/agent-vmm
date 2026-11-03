package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"

	"golang.org/x/sys/unix"
)

const (
	linuxServiceUser    = "agent-vmm"
	linuxDeviceGroup    = "kvm"
	linuxUnitName       = "agent-vmm-host.service"
	linuxInstallRoot    = "/opt/agent-vmm"
	linuxStateDirectory = "/var/lib/agent-vmm"
	linuxUnitDirectory  = "/etc/systemd/system"
)

// paths is the fixed Linux layout. app is the current release link; source
// is the release directory that contains the running executable.
type paths struct {
	correlationRequestID string
	owner                *serviceOwner
	source               string
	installRoot          string
	app                  string
	unit                 string
	applicationData      string
	logs                 string
	serviceLogs          string
	serviceConfigFile    string
	service              serviceConfig
}

func pathsUnder(root, source string, service serviceConfig) paths {
	installRoot := filepath.Join(root, linuxInstallRoot)
	data := filepath.Join(root, linuxStateDirectory)
	return paths{
		source: source, installRoot: installRoot, app: filepath.Join(installRoot, "current"),
		unit:              filepath.Join(root, linuxUnitDirectory, linuxUnitName),
		applicationData:   data,
		logs:              filepath.Join(data, "logs"),
		serviceLogs:       filepath.Join(root, "/var/log/agent-vmm"),
		serviceConfigFile: filepath.Join(data, "service.json"),
		service:           service,
	}
}

// lifecyclePaths prepares the fixed service account and state directory
// before a mutation, because the operation journal lives in that directory.
func lifecyclePaths(request serviceRequest) (paths, error) {
	source, err := fixedServiceSource(request)
	if err != nil {
		return paths{}, err
	}
	if err := ensureServiceAccount(context.Background()); err != nil {
		return paths{}, err
	}
	service, err := linuxServiceConfig(linuxStateDirectory)
	if err != nil {
		return paths{}, err
	}
	value := pathsUnder("", source, service)
	return value, prepareServiceStateDirectories(value)
}

func observationPathsForRequest(request serviceRequest) (paths, error) {
	source, err := fixedServiceSource(request)
	if err != nil {
		return paths{}, err
	}
	service, err := linuxServiceConfig(linuxStateDirectory)
	if err != nil {
		service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: linuxServiceUser}
	}
	return pathsUnder("", source, service), nil
}

func fixedServiceSource(request serviceRequest) (string, error) {
	if request.mode != "" || request.user != "" || request.app != "" || request.sharedHost {
		return "", errors.New("Linux uses the fixed agent-vmm system service; service selection options are not accepted")
	}
	executable, err := os.Executable()
	if err != nil {
		return "", errors.New("agent-vmm location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return "", errors.New("agent-vmm location is invalid")
	}
	return filepath.Dir(executable), nil
}

func linuxServiceConfig(stateDirectory string) (serviceConfig, error) {
	account, err := user.Lookup(linuxServiceUser)
	if err != nil {
		return serviceConfig{}, errors.New("the agent-vmm service user is unavailable")
	}
	uid, uidErr := strconv.Atoi(account.Uid)
	gid, gidErr := strconv.Atoi(account.Gid)
	group, groupErr := user.LookupGroupId(account.Gid)
	if uidErr != nil || gidErr != nil || groupErr != nil || uid == 0 {
		return serviceConfig{}, errors.New("the agent-vmm service user must be a non-root user with a primary group")
	}
	configured, _ := readServiceConfig(filepath.Join(stateDirectory, "service.json"))
	return serviceConfig{Version: 1, Mode: serviceModeDaemon, User: account.Username, Group: group.Name, UID: uid, GID: gid,
		ReleaseID: releaseID, DisablePairing: configured.DisablePairing}, nil
}

// ensureServiceAccount creates the system user that runs the Host. KVM
// access comes only from its kvm group membership.
func ensureServiceAccount(ctx context.Context) error {
	group, err := user.LookupGroup(linuxDeviceGroup)
	if err != nil {
		return errors.New("the kvm group is missing; install KVM support before Agent VMM")
	}
	account, err := user.Lookup(linuxServiceUser)
	var unknown user.UnknownUserError
	if errors.As(err, &unknown) {
		return runCommand(ctx, "useradd", "--system", "--user-group", "--home-dir", linuxStateDirectory, "--no-create-home",
			"--shell", "/usr/sbin/nologin", "--groups", linuxDeviceGroup, linuxServiceUser)
	}
	if err != nil {
		return fmt.Errorf("read the agent-vmm service user: %w", err)
	}
	groups, err := account.GroupIds()
	if err != nil {
		return fmt.Errorf("read the agent-vmm service groups: %w", err)
	}
	if slices.Contains(groups, group.Gid) {
		return nil
	}
	return runCommand(ctx, "usermod", "--append", "--groups", linuxDeviceGroup, linuxServiceUser)
}

func prepareServiceStateDirectories(value paths) error {
	for _, directory := range []string{value.applicationData, value.logs} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", directory, err)
		}
		if err := os.Chmod(directory, 0o700); err != nil {
			return err
		}
		if err := os.Chown(directory, value.service.UID, value.service.GID); err != nil {
			return errors.New("service user directory ownership could not be applied")
		}
	}
	if err := os.MkdirAll(value.serviceLogs, 0750); err != nil {
		return err
	}
	fd, err := unix.Open(value.serviceLogs, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	if stat.Uid != 0 {
		return errors.New("service log directory must be owned by root")
	}
	if err := unix.Fchown(fd, 0, value.service.GID); err != nil {
		return err
	}
	return unix.Fchmod(fd, 0750)
}

func openApplicationDataDirectory(value paths, owner *operationFileOwner) (*os.File, error) {
	return openExistingServiceStateDirectory(value.applicationData, owner)
}

func openExistingApplicationDataDirectory(value paths) (*os.File, error) {
	return openExistingServiceStateDirectory(value.applicationData, nil)
}

func preflightDataDirectory() (string, error) { return linuxStateDirectory, nil }

func builtinMCPPackageDirectory(value paths) string {
	return filepath.Join(value.app, "guest", "mcp-appliance")
}
