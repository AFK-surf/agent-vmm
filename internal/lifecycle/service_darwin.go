package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"github.com/AFK-surf/agent-vmm/internal/launchdservice"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
)

func resolveServiceConfig(request serviceRequest) (serviceConfig, error) {
	account, err := lookupServiceAccount(request.user)
	if err != nil {
		return serviceConfig{}, errors.New("service user is unavailable")
	}
	uid, uidErr := strconv.Atoi(account.Uid)
	gid, gidErr := strconv.Atoi(account.Gid)
	if uidErr != nil || gidErr != nil || uid == 0 || !filepath.IsAbs(account.HomeDir) || account.HomeDir == string(filepath.Separator) {
		return serviceConfig{}, errors.New("service user must be a non-root macOS user with an absolute home directory")
	}
	group, err := user.LookupGroupId(account.Gid)
	if err != nil || group.Name == "" {
		return serviceConfig{}, errors.New("service user's primary group is unavailable")
	}
	if os.Geteuid() != 0 && uid != os.Geteuid() {
		return serviceConfig{}, errors.New("a non-root installer can only manage its own Agent VMM service")
	}
	configPath := filepath.Join(account.HomeDir, "Library", "Application Support", "AgentVMM", "service.json")
	configured, configErr := readServiceConfig(configPath)
	if configErr != nil && !errors.Is(configErr, os.ErrNotExist) {
		return serviceConfig{}, configErr
	}
	mode := request.mode
	if mode == "" && configured.Mode != "" {
		mode = configured.Mode
	}
	if mode == "" {
		mode = serviceModeAgent
	}
	mode, err = selectServiceModeForAction(mode, configured.Mode, request.action, os.Geteuid(), uid, func(uid int) bool {
		return launchctl("print", fmt.Sprintf("gui/%d", uid)) == nil
	})
	if err != nil {
		return serviceConfig{}, err
	}
	return serviceConfig{
		Version: 1, Mode: mode, User: account.Username, Group: group.Name,
		UID: uid, GID: gid, ReleaseID: releaseID,
		DisablePairing: configured.DisablePairing,
	}, nil
}

func selectServiceModeForAction(requested, configured serviceMode, action string, euid, uid int, guiAvailable func(int) bool) (serviceMode, error) {
	// An existing service is observed or removed through its persisted owner.
	// Only install/repair may reinterpret auto and switch ownership.
	if requested == serviceModeAuto && configured != "" && action != "install" && action != "repair" {
		return configured, nil
	}
	return selectServiceMode(requested, configured, euid, uid, guiAvailable)
}

// HostServiceOwnership.tla models the bounded auto choice and the invariant
// enforced here: one user-owned Host is loaded through either launchd domain.
func selectServiceMode(requested, configured serviceMode, euid, uid int, guiAvailable func(int) bool) (serviceMode, error) {
	if requested != serviceModeAuto {
		return requested, nil
	}
	if euid == 0 {
		return serviceModeDaemon, nil
	}
	if configured != "" {
		return configured, nil
	}
	if guiAvailable(uid) {
		return serviceModeAgent, nil
	}
	return "", errors.New("automatic service selection requires administrator access or an active GUI login for the service user")
}

func (value paths) serviceDomain() string {
	if value.service.Mode == serviceModeDaemon {
		return "system"
	}
	return fmt.Sprintf("gui/%d", value.service.UID)
}

func (value paths) alternateServiceDomain() string {
	if value.service.Mode == serviceModeDaemon {
		return fmt.Sprintf("gui/%d", value.service.UID)
	}
	return "system"
}

func (value paths) alternateServicePlist() string {
	if value.service.Mode == serviceModeDaemon {
		return value.agentPlist
	}
	return value.daemonPlist
}

func renderServiceDefinition(value paths, disablePairing bool) ([]byte, error) {
	return launchdservice.RenderHostPlist(launchdservice.HostParams{Home: value.home, App: value.app, Label: hostLabel, User: value.service.User, Group: value.service.Group, Daemon: value.service.Mode == serviceModeDaemon, DisablePairing: disablePairing}), nil
}

func serviceDefinitionCurrent(ctx context.Context, value paths, desired []byte, disablePairing bool, run func(context.Context, ...string) error,
	observe func(context.Context, string) (hostServiceFacts, error)) bool {
	current, err := os.ReadFile(value.hostPlist)
	if err != nil || string(current) != string(desired) || !definitionAbsent(value.alternateServicePlist()) {
		return false
	}
	service, err := observe(ctx, value.serviceDomain()+"/"+hostLabel)
	if err != nil || !service.Running || service.PID <= 0 {
		return false
	}
	if alternateErr := run(ctx, "print", value.alternateServiceDomain()+"/"+hostLabel); !errors.Is(alternateErr, errHostServiceNotFound) {
		return false
	}
	// The fixed root executor owns the system definition and its non-root
	// identity. Exact plist equality above is the daemon declaration fence;
	// lifecycle must not require root to write service-user business state.
	if value.service.Mode == serviceModeDaemon {
		return true
	}
	configured, err := readServiceConfig(value.serviceConfigFile)
	return err == nil && configured.Mode == value.service.Mode && configured.User == value.service.User && configured.Group == value.service.Group &&
		configured.UID == value.service.UID && configured.GID == value.service.GID && configured.ReleaseID == releaseID &&
		configured.DisablePairing == disablePairing
}

func definitionAbsent(path string) bool {
	_, err := os.Lstat(path)
	return errors.Is(err, os.ErrNotExist)
}

func validateAlternateDefinitionPermission(value paths, euid int) error {
	if value.service.Mode == serviceModeAgent && euid != 0 && !definitionAbsent(value.daemonPlist) {
		return errors.New("switching to the Agent VMM login agent requires an administrator to remove the existing system LaunchDaemon definition")
	}
	return nil
}

// lookupServiceAccount returns the named user, or the current user for an
// empty name.
func lookupServiceAccount(name string) (*user.User, error) {
	account, err := user.Current()
	if name != "" {
		account, err = user.Lookup(name)
	}
	if err != nil {
		return nil, err
	}
	return serviceAccount(account), nil
}
