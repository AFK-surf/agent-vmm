//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

type serviceMode string

const (
	serviceModeAgent  serviceMode = "agent"
	serviceModeDaemon serviceMode = "daemon"
	serviceModeAuto   serviceMode = "auto"
)

type serviceRequest struct {
	mode       serviceMode
	user       string
	app        string
	action     string
	sharedHost bool
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
		case "--shared-host":
			if request.sharedHost {
				return serviceRequest{}, nil, errors.New("--shared-host may be specified only once")
			}
			request.sharedHost = true
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
				return serviceRequest{}, nil, errors.New("--service-user requires a user name")
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
	if request.sharedHost && request.app != "" {
		return serviceRequest{}, nil, errors.New("--shared-host and --service-app are mutually exclusive")
	}
	return request, remaining, nil
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
