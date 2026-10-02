package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

func sharedHostApp(home string) string {
	return filepath.Join(home, "Library", "Application Support", "Agent VMM Host", "current", bundle.HostAppName)
}

type publishHostResult struct {
	Version   int    `json:"version"`
	Action    string `json:"action"`
	TargetApp string `json:"targetApp"`
	ReleaseID string `json:"releaseId"`
	Outcome   string `json:"outcome"`
}

type publicationActions struct {
	validate func(string, bool) (string, error)
	copy     func(string, string) error
}

func defaultPublicationActions() publicationActions {
	return publicationActions{
		validate: func(app string, staged bool) (string, error) {
			canonical, err := filepath.EvalSymlinks(app)
			if err != nil || canonical != app {
				return "", errors.New("publication requires a canonical bundle without symlink ancestors")
			}
			id, err := hostBundleReleaseID(app, staged)
			if err != nil {
				return "", err
			}
			if err = exec.Command("/usr/bin/codesign", "--verify", "--deep", "--strict", app).Run(); err != nil {
				return "", err
			}
			return id, nil
		},
		copy: func(source, target string) error { return exec.Command("/usr/bin/ditto", source, target).Run() },
	}
}

// Hold the same external lock as maintenance through verification and actual
// publication. A preflight check cannot protect a later caller-owned rename.
func publishHost(value paths, source string, actions publicationActions) (publishHostResult, error) {
	result := publishHostResult{Version: 1, Action: "publish-host", TargetApp: value.app}
	if value.app != sharedHostApp(value.home) || !filepath.IsAbs(source) || filepath.Base(source) != bundle.HostAppName || source == value.app {
		return result, errors.New("publication requires a separate Agent VMM Host.app source and the standard shared target")
	}
	release, err := acquireMaintenanceMutation(value, "publish-host", "")
	if err != nil {
		return result, err
	}
	defer release()
	if err = validateMaintenancePath(value.home, value.app); err != nil {
		return result, err
	}
	result.ReleaseID, err = actions.validate(source, false)
	if err != nil {
		return result, fmt.Errorf("invalid publication source: %w", err)
	}
	if _, err = os.Lstat(value.app); err == nil {
		id, validateErr := actions.validate(value.app, false)
		if validateErr != nil || id != result.ReleaseID {
			return result, errors.New("shared Host target already exists; use update or maintenance to replace it")
		}
		result.Outcome = "already_present"
		return result, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err = os.MkdirAll(filepath.Dir(value.app), 0700); err != nil {
		return result, err
	}
	directory, err := os.MkdirTemp(filepath.Dir(value.app), ".host-publication-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(directory)
	stage := filepath.Join(directory, bundle.HostAppName)
	if err = actions.copy(source, stage); err != nil {
		return result, err
	}
	id, err := actions.validate(stage, false)
	if err != nil || id != result.ReleaseID {
		return result, errors.New("copied publication bundle failed release or signature verification")
	}
	if err = atomicPublishApp(stage, value.app); err != nil {
		return result, fmt.Errorf("shared Host publication failed without replacing the existing target: %w", err)
	}
	parent, err := os.Open(filepath.Dir(value.app))
	if err != nil {
		return result, err
	}
	defer parent.Close()
	if err = parent.Sync(); err != nil {
		return result, err
	}
	result.Outcome = "published"
	return result, nil
}

func executePublishHost(request serviceRequest, args []string) {
	if len(args) != 2 || args[0] != "--source-app" || request.app != "" {
		fatal("publish-host requires --source-app and uses only the standard shared target")
	}
	config, err := resolveServiceConfig(request)
	if err != nil {
		fatal(err.Error())
	}
	if config.UID != os.Geteuid() || config.UID == 0 {
		fatal("publication must run as the configured non-root service user")
	}
	account, err := user.Lookup(config.User)
	if err != nil {
		fatal("service user is unavailable")
	}
	value := paths{home: account.HomeDir, app: sharedHostApp(account.HomeDir), service: config}
	result, err := publishHost(value, args[1], defaultPublicationActions())
	if err != nil {
		fatal(err.Error())
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatal(err.Error())
	}
}
