package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAutoServiceSelectionUsesCurrentInstallerCapability(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "vmmuser", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	rendered, err := renderServiceDefinition(value, false)
	if err != nil {
		t.Fatal(err)
	}
	definition := string(rendered)
	for _, expected := range []string{"<string>Background</string>", "<key>UserName</key>", "<string>vmmuser</string>", "<key>GroupName</key>", "<string>staff</string>"} {
		if !strings.Contains(definition, expected) {
			t.Errorf("daemon definition is missing %q: %s", expected, definition)
		}
	}
	if strings.Contains(definition, "<string>Interactive</string>") {
		t.Fatal("daemon retained interactive process type")
	}
}

func TestRepairRetainsConfiguredPairingRestriction(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	value.service.DisablePairing = true
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := repairWithOptionsContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" || arguments[0] == "bootout" {
			return errHostServiceNotFound
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
	t.Parallel()
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "vmmuser", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	value.hostPlist = value.daemonPlist
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
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
			return errHostServiceNotFound
		}
		t.Fatalf("unexpected launchctl target: %v", arguments)
		return nil
	}
	running := func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{Loaded: true, Running: true, PID: 42}, nil
	}
	if !serviceDefinitionCurrent(context.Background(), value, desired, false, loaded, running) {
		t.Fatal("exact running daemon was not treated as current")
	}
	if err := os.Remove(value.serviceConfigFile); err != nil {
		t.Fatal(err)
	}
	if !serviceDefinitionCurrent(context.Background(), value, desired, false, loaded, running) {
		t.Fatal("fixed executor daemon incorrectly required service-user config")
	}
	stopped := func(context.Context, string) (hostServiceFacts, error) {
		status := 1
		return hostServiceFacts{Loaded: true, LastExitStatus: &status}, nil
	}
	if serviceDefinitionCurrent(context.Background(), value, desired, false, loaded, stopped) {
		t.Fatal("loaded but exited daemon was treated as current")
	}
	if err := os.MkdirAll(filepath.Dir(value.agentPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.agentPlist, []byte("stale alternate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if serviceDefinitionCurrent(context.Background(), value, desired, false, loaded, running) {
		t.Fatal("current daemon accepted an unloaded alternate definition")
	}
}

func TestServiceUserAdoptsOnlyExactRunningAdministratorDaemon(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "vmmuser", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	value.hostPlist = value.daemonPlist
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	desired, err := renderServiceDefinition(value, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, desired, 0o644); err != nil {
		t.Fatal(err)
	}
	run := func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" && arguments[1] == "gui/502/"+hostLabel {
			return errHostServiceNotFound
		}
		t.Fatalf("unexpected launchctl mutation: %v", arguments)
		return nil
	}
	observe := func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{Loaded: true, Running: true, PID: 42}, nil
	}
	if err := repairWithOptionsContextObserved(context.Background(), value, run, false, 502, observe); err != nil {
		t.Fatal(err)
	}
	configured, err := readServiceConfig(value.serviceConfigFile)
	if err != nil || configured.Mode != serviceModeDaemon || configured.UID != 502 || !configured.DisablePairing {
		t.Fatalf("service config=%+v err=%v", configured, err)
	}
}

func TestServiceUserRejectsStaleAdministratorDaemonWithoutPersistingConfig(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "vmmuser", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	value.hostPlist = value.daemonPlist
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, []byte("stale definition"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := repairWithOptionsContextObserved(context.Background(), value, func(context.Context, ...string) error {
		return errHostServiceNotFound
	}, false, 502, func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{Loaded: true, Running: true, PID: 42}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "administrator access") {
		t.Fatalf("error=%v", err)
	}
	if _, statErr := os.Stat(value.serviceConfigFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("stale daemon persisted service config: %v", statErr)
	}
}

func TestCurrentDefinitionRejectsLoadedAlternateOwner(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	value.service = serviceConfig{Version: 1, Mode: serviceModeDaemon, User: "vmmuser", Group: "staff", UID: 502, GID: 20, ReleaseID: releaseID}
	value.hostPlist = value.daemonPlist
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
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
	if serviceDefinitionCurrent(context.Background(), value, desired, false, func(context.Context, ...string) error { return nil }, func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{Loaded: true, Running: true, PID: 42}, nil
	}) {
		t.Fatal("exact selected definition accepted a loaded alternate owner")
	}
}

func TestAgentSwitchRequiresAdminToRemoveDaemonDefinition(t *testing.T) {
	t.Parallel()
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
