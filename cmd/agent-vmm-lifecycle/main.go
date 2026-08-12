package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

const (
	version        = "0.1.0"
	hostLabel      = "com.afk-surf.agent-vmm"
	connectorLabel = "com.afk-surf.agent-vmm-connect"
)

type paths struct {
	home            string
	app             string
	resources       string
	hostPlist       string
	connectorPlist  string
	applicationData string
	logs            string
}

type serviceStatus struct {
	AppInstalled       bool `json:"appInstalled"`
	HostInstalled      bool `json:"hostInstalled"`
	MCPInstalled       bool `json:"mcpInstalled"`
	ConnectorInstalled bool `json:"connectorInstalled"`
	HostLoaded         bool `json:"hostLoaded"`
	ConnectorLoaded    bool `json:"connectorLoaded"`
	SalixReady         bool `json:"salixReady"`
	SalixRevoked       bool `json:"salixRevoked"`
}

func main() {
	if len(os.Args) < 2 {
		fatal("usage: agent-vmm-lifecycle version --json|status|repair [--disable-personal-mesh-pairing]|drain|uninstall [--purge]")
	}
	if os.Args[1] == "version" {
		if len(os.Args) != 3 || os.Args[2] != "--json" {
			fatal("version requires --json")
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"component": "agent-vmm", "version": version}); err != nil {
			fatal("version encoding failed")
		}
		return
	}
	resolved, err := lifecyclePaths()
	if err != nil {
		fatal(err.Error())
	}
	switch os.Args[1] {
	case "status":
		if err := json.NewEncoder(os.Stdout).Encode(readStatus(resolved)); err != nil {
			fatal("status encoding failed")
		}
	case "repair":
		disablePairing := len(os.Args) == 3 && os.Args[2] == "--disable-personal-mesh-pairing"
		if len(os.Args) > 3 || (len(os.Args) == 3 && !disablePairing) {
			fatal("repair accepts only --disable-personal-mesh-pairing")
		}
		if err := repairWithPolicy(resolved, disablePairing); err != nil {
			fatal(err.Error())
		}
	case "drain":
		if err := drain(resolved); err != nil {
			fatal(err.Error())
		}
	case "uninstall":
		purge := len(os.Args) == 3 && os.Args[2] == "--purge"
		if len(os.Args) > 3 || (len(os.Args) == 3 && !purge) {
			fatal("uninstall accepts only --purge")
		}
		if err := uninstall(resolved, purge); err != nil {
			fatal(err.Error())
		}
	default:
		fatal("unknown lifecycle command")
	}
}

func lifecyclePaths() (paths, error) {
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return paths{}, errors.New("user home directory is invalid")
	}
	executable, err := os.Executable()
	if err != nil {
		return paths{}, errors.New("helper location is unavailable")
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return paths{}, errors.New("helper location is invalid")
	}
	helpers := filepath.Dir(executable)
	if filepath.Base(helpers) != "Helpers" || filepath.Base(executable) != "agent-vmm-lifecycle" {
		return paths{}, errors.New("helper is not inside an Agent VMM application bundle")
	}
	contents := filepath.Dir(helpers)
	if filepath.Base(contents) != "Contents" {
		return paths{}, errors.New("application bundle layout is invalid")
	}
	app := filepath.Dir(contents)
	if filepath.Ext(app) != ".app" {
		return paths{}, errors.New("application bundle is invalid")
	}
	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	return paths{
		home: home, app: app, resources: filepath.Join(contents, "Resources"),
		hostPlist:       filepath.Join(launchAgents, hostLabel+".plist"),
		connectorPlist:  filepath.Join(launchAgents, connectorLabel+".plist"),
		applicationData: filepath.Join(home, "Library", "Application Support", "AgentVMM"),
		logs:            filepath.Join(home, "Library", "Logs", "AgentVMM"),
	}, nil
}

func readStatus(value paths) serviceStatus {
	helper := filepath.Join(value.app, "Contents", "Helpers")
	result := serviceStatus{
		AppInstalled:       directory(value.app),
		HostInstalled:      regular(filepath.Join(helper, "agent-vmm-host")) && regular(value.hostPlist),
		MCPInstalled:       regular(filepath.Join(helper, "agent-vmm-mcp")),
		ConnectorInstalled: regular(filepath.Join(helper, "agent-vmm-connect")) && regular(value.connectorPlist),
		HostLoaded:         launchctl("print", domain()+"/"+hostLabel) == nil,
		ConnectorLoaded:    launchctl("print", domain()+"/"+connectorLabel) == nil,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	registration, err := remoteconnector.ObserveRegistration(ctx, filepath.Join(value.applicationData, "remote-compute.sock"))
	if err == nil {
		result.SalixReady = registration.Ready
		result.SalixRevoked = registration.Revoked
	}
	return result
}

func repair(value paths) error { return repairWith(value, launchctl) }

func repairWithPolicy(value paths, disablePairing bool) error {
	return repairWithOptions(value, launchctl, disablePairing)
}

func repairWith(value paths, run func(...string) error) error {
	return repairWithOptions(value, run, false)
}

func repairWithOptions(value paths, run func(...string) error, disablePairing bool) error {
	if !directory(value.app) {
		return errors.New("Agent VMM application bundle is missing")
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		return errors.New("LaunchAgents directory could not be created")
	}
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		return errors.New("application data directory could not be created")
	}
	if err := os.MkdirAll(value.logs, 0o700); err != nil {
		return errors.New("log directory could not be created")
	}
	for _, item := range []struct{ resource, destination string }{
		{hostLabel + ".plist", value.hostPlist},
		{connectorLabel + ".plist", value.connectorPlist},
	} {
		template, err := os.ReadFile(filepath.Join(value.resources, item.resource))
		if err != nil {
			return errors.New("bundled service definition is missing")
		}
		rendered := strings.ReplaceAll(string(template), "__HOME__", xmlEscape(value.home))
		rendered = strings.ReplaceAll(rendered, "__APP__", xmlEscape(value.app))
		if item.destination == value.hostPlist && disablePairing {
			rendered = addProgramArgument(rendered, "--disable-personal-mesh-pairing")
		}
		if err := atomicWrite(item.destination, []byte(rendered), 0o600); err != nil {
			return errors.New("service definition could not be installed")
		}
	}
	_ = run("bootout", domain()+"/"+hostLabel)
	_ = run("bootout", domain()+"/"+connectorLabel)
	if err := run("bootstrap", domain(), value.hostPlist); err != nil {
		return errors.New("host service could not be loaded")
	}
	if err := run("enable", domain()+"/"+hostLabel); err != nil {
		return errors.New("host service could not be enabled")
	}
	if err := run("bootstrap", domain(), value.connectorPlist); err != nil {
		return errors.New("connector service could not be loaded")
	}
	if err := run("enable", domain()+"/"+connectorLabel); err != nil {
		return errors.New("connector service could not be enabled")
	}
	return nil
}

func addProgramArgument(plist, argument string) string {
	marker := "</array>"
	index := strings.Index(plist, marker)
	if index < 0 || strings.Contains(plist, "<string>"+argument+"</string>") {
		return plist
	}
	return plist[:index] + "\t\t<string>" + xmlEscape(argument) + "</string>\n\t" + plist[index:]
}

func uninstall(value paths, purge bool) error { return uninstallWith(value, purge, launchctl) }

func drain(value paths) error { return drainWith(value, launchctl) }

// drain closes connector admission before stopping the host. launchctl bootout
// waits for each LaunchAgent to terminate, so lifecycle callers receive an
// acknowledgement only after the current runtime owner has exited.
func drainWith(value paths, run func(...string) error) error {
	if err := run("bootout", domain()+"/"+connectorLabel); err != nil && regular(value.connectorPlist) {
		return errors.New("connector service could not be drained")
	}
	if err := run("bootout", domain()+"/"+hostLabel); err != nil && regular(value.hostPlist) {
		return errors.New("host service could not be drained")
	}
	return nil
}

func uninstallWith(value paths, purge bool, run func(...string) error) error {
	_ = run("bootout", domain()+"/"+hostLabel)
	_ = run("bootout", domain()+"/"+connectorLabel)
	if err := os.Remove(value.hostPlist); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("host service definition could not be removed")
	}
	if err := os.Remove(value.connectorPlist); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("connector service definition could not be removed")
	}
	if purge {
		if err := os.RemoveAll(value.applicationData); err != nil {
			return errors.New("application data could not be removed")
		}
		if err := os.RemoveAll(value.logs); err != nil {
			return errors.New("logs could not be removed")
		}
	}
	if err := os.RemoveAll(value.app); err != nil {
		return errors.New("application bundle could not be removed")
	}
	return nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".agent-vmm-*.plist")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func launchctl(arguments ...string) error {
	command := exec.Command("/bin/launchctl", arguments...)
	command.Stdout = nil
	command.Stderr = nil
	return command.Run()
}

func domain() string { return fmt.Sprintf("gui/%d", os.Getuid()) }
func regular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
func directory(path string) bool { info, err := os.Stat(path); return err == nil && info.IsDir() }

func xmlEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
