package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

type serviceMode string

// HostServiceOwnership.tla models the bounded auto choice and the invariant
// enforced here: one user-owned Host is loaded through either launchd domain.

const (
	serviceModeAgent  serviceMode = "agent"
	serviceModeDaemon serviceMode = "daemon"
	serviceModeAuto   serviceMode = "auto"
)

type serviceRequest struct {
	mode   serviceMode
	user   string
	app    string
	action string
}

type serviceConfig struct {
	Version        int         `json:"version"`
	Mode           serviceMode `json:"mode"`
	User           string      `json:"user"`
	Group          string      `json:"group"`
	UID            int         `json:"uid"`
	GID            int         `json:"gid"`
	ReleaseID      string      `json:"release_id"`
	DisablePairing bool        `json:"disable_personal_mesh_pairing"`
}

func extractServiceRequest(arguments []string) (serviceRequest, []string, error) {
	var request serviceRequest
	remaining := make([]string, 0, len(arguments))
	for index := 0; index < len(arguments); index++ {
		switch arguments[index] {
		case "--service-type":
			if request.mode != "" {
				return serviceRequest{}, nil, errors.New("--service-type may be specified only once")
			}
			if index+1 >= len(arguments) {
				return serviceRequest{}, nil, errors.New("--service-type requires agent, daemon, or auto")
			}
			index++
			request.mode = serviceMode(arguments[index])
			if request.mode != serviceModeAgent && request.mode != serviceModeDaemon && request.mode != serviceModeAuto {
				return serviceRequest{}, nil, errors.New("--service-type must be agent, daemon, or auto")
			}
		case "--service-user":
			if request.user != "" {
				return serviceRequest{}, nil, errors.New("--service-user may be specified only once")
			}
			if index+1 >= len(arguments) || strings.TrimSpace(arguments[index+1]) == "" {
				return serviceRequest{}, nil, errors.New("--service-user requires a macOS user name")
			}
			index++
			request.user = arguments[index]
		case "--service-app":
			if request.app != "" {
				return serviceRequest{}, nil, errors.New("--service-app may be specified only once")
			}
			if index+1 >= len(arguments) || strings.TrimSpace(arguments[index+1]) == "" {
				return serviceRequest{}, nil, errors.New("--service-app requires an absolute Agent VMM Host.app path")
			}
			index++
			request.app = arguments[index]
			if !filepath.IsAbs(request.app) || filepath.Base(request.app) != bundle.HostAppName {
				return serviceRequest{}, nil, errors.New("--service-app requires an absolute Agent VMM Host.app path")
			}
		default:
			remaining = append(remaining, arguments[index])
		}
	}
	return request, remaining, nil
}

func resolveServiceConfig(request serviceRequest) (serviceConfig, error) {
	account, err := user.Current()
	if request.user != "" {
		account, err = user.Lookup(request.user)
	}
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
	if mode == serviceModeDaemon && os.Geteuid() != 0 && configured.Mode != serviceModeDaemon {
		return serviceConfig{}, errors.New("initial daemon installation requires administrator access")
	}
	return serviceConfig{Version: 1, Mode: mode, User: account.Username, Group: group.Name, UID: uid, GID: gid, ReleaseID: releaseID}, nil
}

func selectServiceModeForAction(requested, configured serviceMode, action string, euid, uid int, guiAvailable func(int) bool) (serviceMode, error) {
	// An existing service is observed or removed through its persisted owner.
	// Only install/repair may reinterpret auto and switch ownership.
	if requested == serviceModeAuto && configured != "" && action != "install" && action != "repair" {
		return configured, nil
	}
	return selectServiceMode(requested, configured, euid, uid, guiAvailable)
}

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

func readServiceConfig(path string) (serviceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return serviceConfig{}, err
	}
	var config serviceConfig
	if json.Unmarshal(data, &config) != nil || config.Version != 1 ||
		(config.Mode != serviceModeAgent && config.Mode != serviceModeDaemon) ||
		config.User == "" || config.Group == "" || config.UID <= 0 || config.GID < 0 {
		return serviceConfig{}, errors.New("Agent VMM service configuration is invalid")
	}
	return config, nil
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
	template, err := os.ReadFile(filepath.Join(value.resources, hostTemplateResource))
	if err != nil {
		return nil, errors.New("bundled service definition is missing")
	}
	rendered := strings.ReplaceAll(string(template), "__HOME__", xmlEscape(value.home))
	rendered = strings.ReplaceAll(rendered, "__APP__", xmlEscape(value.app))
	if disablePairing {
		rendered = addProgramArgument(rendered, "--disable-personal-mesh-pairing")
	}
	if value.service.Mode == serviceModeDaemon {
		rendered = strings.Replace(rendered, "<string>Interactive</string>", "<string>Background</string>", 1)
		marker := "</dict>"
		index := strings.LastIndex(rendered, marker)
		if index < 0 {
			return nil, errors.New("bundled service definition is invalid")
		}
		identity := fmt.Sprintf("\t<key>UserName</key>\n\t<string>%s</string>\n\t<key>GroupName</key>\n\t<string>%s</string>\n", xmlEscape(value.service.User), xmlEscape(value.service.Group))
		rendered = rendered[:index] + identity + rendered[index:]
	}
	return []byte(rendered), nil
}

func serviceDefinitionCurrent(ctx context.Context, value paths, desired []byte, disablePairing bool, run func(context.Context, ...string) error) bool {
	current, err := os.ReadFile(value.hostPlist)
	if err != nil || string(current) != string(desired) || !definitionAbsent(value.alternateServicePlist()) || run(ctx, "print", value.serviceDomain()+"/"+hostLabel) != nil {
		return false
	}
	if alternateErr := run(ctx, "print", value.alternateServiceDomain()+"/"+hostLabel); !errors.Is(alternateErr, errLaunchctlServiceNotFound) {
		return false
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

func persistServiceConfig(value paths, disablePairing bool) error {
	config := value.service
	config.DisablePairing = disablePairing
	config.ReleaseID = releaseID
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := atomicWrite(value.serviceConfigFile, data, 0o600); err != nil {
		return err
	}
	if os.Geteuid() == 0 {
		return os.Chown(value.serviceConfigFile, value.service.UID, value.service.GID)
	}
	return nil
}
