package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"golang.org/x/sys/unix"
)

type paths struct {
	owner             *serviceOwner
	home              string
	app               string
	resources         string
	hostPlist         string
	applicationData   string
	logs              string
	serviceLogs       string
	serviceConfigFile string
	agentPlist        string
	daemonPlist       string
	service           serviceConfig
}

func lifecyclePaths(request serviceRequest) (paths, error) {
	executable, err := os.Executable()
	if err != nil {
		return paths{}, errors.New("helper location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return paths{}, errors.New("helper location is invalid")
	}
	return lifecyclePathsForRequest(request, executable)
}

func observationPathsForRequest(request serviceRequest) (paths, error) {
	executable, err := os.Executable()
	if err != nil {
		return paths{}, errors.New("helper location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return paths{}, errors.New("helper location is invalid")
	}
	config, err := resolveServiceConfig(request)
	if err != nil {
		return paths{}, err
	}
	account, err := lookupServiceAccount(config.User)
	if err != nil {
		return paths{}, errors.New("service user is unavailable")
	}
	if request.app != "" && os.Geteuid() != 0 {
		return paths{}, errors.New("--service-app is restricted to an administrator installer")
	}
	if request.sharedHost {
		request.app = sharedHostApp(account.HomeDir)
	}
	return observationPathsForConfig(account.HomeDir, executable, config, request.app)
}

func observationPathsForConfig(home, executable string, service serviceConfig, targetApp string) (paths, error) {
	if !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return paths{}, errors.New("user home directory is invalid")
	}
	helpers := filepath.Dir(executable)
	contents := filepath.Dir(helpers)
	if filepath.Base(helpers) != "Helpers" || filepath.Base(contents) != "Contents" {
		return paths{}, errors.New("helper is not inside an Agent VMM application bundle")
	}
	sourceApp := filepath.Dir(contents)
	if filepath.Base(sourceApp) != bundle.HostAppName {
		return paths{}, fmt.Errorf("application bundle must be %q", bundle.HostAppName)
	}
	app := sourceApp
	if targetApp != "" {
		if !filepath.IsAbs(targetApp) || filepath.Base(targetApp) != bundle.HostAppName {
			return paths{}, errors.New("service app must be an absolute Agent VMM Host.app path")
		}
		app = targetApp
	}
	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	agentPlist := filepath.Join(launchAgents, hostLabel+".plist")
	daemonPlist := filepath.Join("/Library/LaunchDaemons", hostLabel+".plist")
	hostPlist := agentPlist
	if service.Mode == serviceModeDaemon {
		hostPlist = daemonPlist
	}
	return paths{
		home: home, app: app, resources: filepath.Join(sourceApp, "Contents", "Resources"),
		hostPlist: hostPlist, agentPlist: agentPlist, daemonPlist: daemonPlist, service: service,
		applicationData:   filepath.Join(home, "Library", "Application Support", "AgentVMM"),
		logs:              filepath.Join(home, "Library", "Logs", "AgentVMM"),
		serviceLogs:       serviceLogPath(home, service.Mode),
		serviceConfigFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	}, nil
}

func lifecyclePathsFor(home, executable string) (paths, error) {
	return lifecyclePathsForConfig(home, executable, serviceConfig{Version: 1, Mode: serviceModeAgent, User: "test", Group: "staff", UID: os.Getuid(), GID: os.Getgid(), ReleaseID: releaseID})
}

func lifecyclePathsForRequest(request serviceRequest, executable string) (paths, error) {
	config, err := resolveServiceConfig(request)
	if err != nil {
		return paths{}, err
	}
	account, err := lookupServiceAccount(config.User)
	if err != nil {
		return paths{}, errors.New("service user is unavailable")
	}
	if request.app != "" && os.Geteuid() != 0 {
		return paths{}, errors.New("--service-app is restricted to an administrator installer")
	}
	if request.sharedHost {
		request.app = sharedHostApp(account.HomeDir)
		if err := validateMaintenancePath(account.HomeDir, request.app); err != nil {
			return paths{}, err
		}
	}
	return lifecyclePathsForConfigWithApp(account.HomeDir, executable, config, request.app)
}

func lifecyclePathsForConfig(home, executable string, service serviceConfig) (paths, error) {
	return lifecyclePathsForConfigWithApp(home, executable, service, "")
}

func lifecyclePathsForConfigWithApp(home, executable string, service serviceConfig, targetApp string) (paths, error) {
	if !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return paths{}, errors.New("user home directory is invalid")
	}
	helpers := filepath.Dir(executable)
	if filepath.Base(helpers) != "Helpers" || filepath.Base(executable) != "agent-vmm-lifecycle" {
		return paths{}, errors.New("helper is not inside an Agent VMM application bundle")
	}
	contents := filepath.Dir(helpers)
	if filepath.Base(contents) != "Contents" {
		return paths{}, errors.New("application bundle layout is invalid")
	}
	sourceApp := filepath.Dir(contents)
	if filepath.Base(sourceApp) != bundle.HostAppName {
		return paths{}, fmt.Errorf("application bundle must be %q", bundle.HostAppName)
	}
	app := sourceApp
	if targetApp != "" {
		if !filepath.IsAbs(targetApp) || filepath.Base(targetApp) != bundle.HostAppName {
			return paths{}, errors.New("service app must be an absolute Agent VMM Host.app path")
		}
		app = targetApp
	}
	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	agentPlist := filepath.Join(launchAgents, hostLabel+".plist")
	daemonPlist := filepath.Join("/Library/LaunchDaemons", hostLabel+".plist")
	hostPlist := agentPlist
	if service.Mode == serviceModeDaemon {
		hostPlist = daemonPlist
	}
	return paths{
		home: home, app: app, resources: filepath.Join(sourceApp, "Contents", "Resources"),
		hostPlist:         hostPlist,
		agentPlist:        agentPlist,
		daemonPlist:       daemonPlist,
		service:           service,
		applicationData:   filepath.Join(home, "Library", "Application Support", "AgentVMM"),
		logs:              filepath.Join(home, "Library", "Logs", "AgentVMM"),
		serviceLogs:       serviceLogPath(home, service.Mode),
		serviceConfigFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	}, nil
}

func preflightDataDirectory() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", errors.New("preflight cannot resolve the home directory")
	}
	return filepath.Join(home, "Library", "Application Support", "AgentVMM"), nil
}

func openApplicationDataDirectory(value paths, owner *operationFileOwner) (*os.File, error) {
	home, err := openExistingServiceStateDirectory(value.home, nil)
	if err != nil {
		return nil, fmt.Errorf("open service home: %w", err)
	}
	defer home.Close()
	library, err := openOrCreateAncestorDirectoryAt(home, "Library")
	if err != nil {
		return nil, fmt.Errorf("open service Library: %w", err)
	}
	defer library.Close()
	applicationSupport, err := openOrCreateAncestorDirectoryAt(library, "Application Support")
	if err != nil {
		return nil, fmt.Errorf("open service Application Support: %w", err)
	}
	defer applicationSupport.Close()
	return openOrCreateStateDirectoryAt(applicationSupport, "AgentVMM", owner)
}

func openExistingApplicationDataDirectory(value paths) (*os.File, error) {
	home, err := openExistingServiceStateDirectory(value.home, nil)
	if err != nil {
		return nil, err
	}
	defer home.Close()
	library, err := openExistingStateDirectoryAt(home, "Library")
	if err != nil {
		return nil, err
	}
	defer library.Close()
	applicationSupport, err := openExistingStateDirectoryAt(library, "Application Support")
	if err != nil {
		return nil, err
	}
	defer applicationSupport.Close()
	return openExistingStateDirectoryAt(applicationSupport, "AgentVMM")
}

func openOrCreateAncestorDirectoryAt(parent *os.File, name string) (*os.File, error) {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return nil, errors.New("state ancestor name is invalid")
	}
	created := false
	if err := unix.Mkdirat(int(parent.Fd()), name, 0o700); err == nil {
		created = true
	} else if !errors.Is(err, unix.EEXIST) {
		return nil, err
	}
	directory, err := openExistingStateDirectoryAt(parent, name)
	if err != nil {
		return nil, err
	}
	if created {
		if err := parent.Sync(); err != nil {
			directory.Close()
			return nil, err
		}
	}
	return directory, nil
}

func serviceLogPath(home string, mode serviceMode) string {
	if mode == serviceModeDaemon {
		return "/Library/Logs/AgentVMM"
	}
	return filepath.Join(home, "Library", "Logs", "AgentVMM")
}

func builtinMCPPackageDirectory(value paths) string {
	return filepath.Join(value.app, "Contents", "Resources", "Guest", "mcp-appliance")
}
