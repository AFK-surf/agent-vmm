package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestExtractServiceRequestLeavesLifecycleArguments(t *testing.T) {
	request, remaining, err := extractServiceRequest([]string{
		"--operation-stdin", "--service-user", "bftvmm", "--request-id", "operation", "--service-type", "daemon",
		"--service-app", "/Users/bftvmm/Agent VMM Host.app",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.mode != serviceModeDaemon || request.user != "bftvmm" || request.app != "/Users/bftvmm/Agent VMM Host.app" {
		t.Fatalf("request=%+v", request)
	}
	if want := []string{"--operation-stdin", "--request-id", "operation"}; !reflect.DeepEqual(remaining, want) {
		t.Fatalf("remaining=%v want=%v", remaining, want)
	}
}

func TestExtractServiceRequestRejectsUntrustedServiceAppShape(t *testing.T) {
	for _, value := range []string{"Agent VMM Host.app", "/tmp/Agent VMM.app"} {
		if _, _, err := extractServiceRequest([]string{"--service-app", value}); err == nil {
			t.Fatalf("service app %q was accepted", value)
		}
	}
}

func TestExtractServiceRequestRejectsUnknownMode(t *testing.T) {
	if _, _, err := extractServiceRequest([]string{"--service-type", "login"}); err == nil {
		t.Fatal("unknown service type was accepted")
	}
}

func TestAutoServiceSelectionUsesCurrentInstallerCapability(t *testing.T) {
	mode, err := selectServiceMode(serviceModeAuto, serviceModeAgent, 0, 502, func(int) bool { return false })
	if err != nil || mode != serviceModeDaemon {
		t.Fatalf("root auto mode=%q err=%v", mode, err)
	}
	mode, err = selectServiceMode(serviceModeAuto, serviceModeDaemon, 502, 502, func(int) bool { return false })
	if err != nil || mode != serviceModeDaemon {
		t.Fatalf("configured daemon mode=%q err=%v", mode, err)
	}
}

func TestExistingOwnerActionsAreDistinguishedFromInstall(t *testing.T) {
	for _, action := range []string{"status", "drain", "uninstall", "registration-state"} {
		mode, err := selectServiceModeForAction(serviceModeAuto, serviceModeAgent, action, 0, 502, func(int) bool { return false })
		if err != nil || mode != serviceModeAgent {
			t.Fatalf("existing-owner action %q selected %q: %v", action, mode, err)
		}
	}
	for _, action := range []string{"install", "repair"} {
		mode, err := selectServiceModeForAction(serviceModeAuto, serviceModeAgent, action, 0, 502, func(int) bool { return false })
		if err != nil || mode != serviceModeDaemon {
			t.Fatalf("ownership-changing action %q selected %q: %v", action, mode, err)
		}
	}
}

func TestDaemonDefinitionDropsPrivilegesAndIsBackground(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "bftvmm", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	template := `<plist><dict><key>ProcessType</key><string>Interactive</string><key>ProgramArguments</key><array><string>__APP__/Contents/MacOS/agent-vmm-host</string></array><key>Home</key><string>__HOME__</string></dict></plist>`
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte(template), 0o600); err != nil {
		t.Fatal(err)
	}
	rendered, err := renderServiceDefinition(value, false)
	if err != nil {
		t.Fatal(err)
	}
	definition := string(rendered)
	for _, expected := range []string{"<string>Background</string>", "<key>UserName</key>", "<string>bftvmm</string>", "<key>GroupName</key>", "<string>staff</string>"} {
		if !strings.Contains(definition, expected) {
			t.Errorf("daemon definition is missing %q: %s", expected, definition)
		}
	}
	if strings.Contains(definition, "<string>Interactive</string>") {
		t.Fatal("daemon retained interactive process type")
	}
}

func TestServiceDefinitionUsesFreshBundleResourceGeometry(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	template := `<plist><dict><key>ProgramArguments</key><array><string>host</string><string>--cpus</string><string>8</string><string>--memory</string><string>12884901888</string><string>--state-size</string><string>34359738368</string></array></dict></plist>`
	current := `<plist><dict><key>ProgramArguments</key><array><string>host</string><string>--cpus</string><string>8</string><string>--memory</string><string>12884901888</string><string>--state-size</string><string>17179869184</string></array></dict></plist>`
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte(template), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	rendered, err := renderServiceDefinition(value, false)
	if err != nil {
		t.Fatal(err)
	}
	definition := string(rendered)
	for _, expected := range []string{
		"<string>--cpus</string><string>8</string>",
		"<string>--memory</string><string>12884901888</string>",
		"<string>--state-size</string><string>34359738368</string>",
	} {
		if !strings.Contains(definition, expected) {
			t.Errorf("updated definition did not retain %q: %s", expected, definition)
		}
	}
	if strings.Contains(definition, "17179869184") {
		t.Fatal("fresh profile inherited old state geometry")
	}
}

func TestRepairRetainsConfiguredPairingRestriction(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service.DisablePairing = true
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte(`<plist><dict><key>ProgramArguments</key><array><string>host</string></array></dict></plist>`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := repairWithOptionsContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" || arguments[0] == "bootout" {
			return errLaunchctlServiceNotFound
		}
		return nil
	}, false); err != nil {
		t.Fatal(err)
	}
	definition, err := os.ReadFile(value.hostPlist)
	if err != nil || !strings.Contains(string(definition), "<string>--disable-personal-mesh-pairing</string>") {
		t.Fatalf("repair loosened configured pairing policy: definition=%s err=%v", definition, err)
	}
	configured, err := readServiceConfig(value.serviceConfigFile)
	if err != nil || !configured.DisablePairing {
		t.Fatalf("repair did not retain configured pairing policy: config=%+v err=%v", configured, err)
	}
}

func TestCurrentDaemonCanBeObservedWithoutReload(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "bftvmm", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	value.hostPlist = value.daemonPlist
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte(`<plist><dict><key>ProcessType</key><string>Interactive</string></dict></plist>`), 0o600); err != nil {
		t.Fatal(err)
	}
	desired, err := renderServiceDefinition(value, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, desired, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.serviceConfigFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := persistServiceConfig(value, false); err != nil {
		t.Fatal(err)
	}
	loaded := func(_ context.Context, arguments ...string) error {
		if arguments[0] != "print" {
			t.Fatalf("unexpected launchctl call: %v", arguments)
		}
		if arguments[1] == "system/"+hostLabel {
			return nil
		}
		if arguments[1] == "gui/502/"+hostLabel {
			return errLaunchctlServiceNotFound
		}
		t.Fatalf("unexpected launchctl target: %v", arguments)
		return nil
	}
	if !serviceDefinitionCurrent(context.Background(), value, desired, false, loaded) {
		t.Fatal("exact running daemon was not treated as current")
	}
	if err := os.MkdirAll(filepath.Dir(value.agentPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.agentPlist, []byte("stale alternate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if serviceDefinitionCurrent(context.Background(), value, desired, false, loaded) {
		t.Fatal("current daemon accepted an unloaded alternate definition")
	}
}

func TestCurrentDefinitionRejectsLoadedAlternateOwner(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "bftvmm", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	value.hostPlist = value.daemonPlist
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte(`<plist><dict><key>ProcessType</key><string>Interactive</string></dict></plist>`), 0o600); err != nil {
		t.Fatal(err)
	}
	desired, err := renderServiceDefinition(value, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, desired, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.serviceConfigFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := persistServiceConfig(value, false); err != nil {
		t.Fatal(err)
	}
	if serviceDefinitionCurrent(context.Background(), value, desired, false, func(context.Context, ...string) error { return nil }) {
		t.Fatal("exact selected definition accepted a loaded alternate owner")
	}
}

func TestAgentSwitchRequiresAdminToRemoveDaemonDefinition(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(value.daemonPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.daemonPlist, []byte("stale daemon"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateAlternateDefinitionPermission(value, 502); err == nil || !strings.Contains(err.Error(), "administrator") {
		t.Fatalf("non-admin agent switch error=%v", err)
	}
	if err := validateAlternateDefinitionPermission(value, 0); err != nil {
		t.Fatalf("administrator agent switch rejected: %v", err)
	}
}
