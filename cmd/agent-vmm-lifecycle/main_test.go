package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestXMLEscape(t *testing.T) {
	if got := xmlEscape("/Users/a&b<c>"); got != "/Users/a&amp;b&lt;c&gt;" {
		t.Fatalf("xmlEscape() = %q", got)
	}
}

func TestManagedRepairArgumentIsIdempotent(t *testing.T) {
	input := "<array>\n<string>agent-vmm-host</string>\n</array>"
	once := addProgramArgument(input, "--disable-personal-mesh-pairing")
	twice := addProgramArgument(once, "--disable-personal-mesh-pairing")
	if once != twice || strings.Count(once, "--disable-personal-mesh-pairing") != 1 {
		t.Fatalf("managed argument render is not idempotent: %q", twice)
	}
}

func TestRepairInstallsDefinitionsBeforeLoadingServices(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	template := []byte("<plist><string>__HOME__</string><string>__APP__</string></plist>")
	for _, name := range []string{hostLabel + ".plist", connectorLabel + ".plist"} {
		if err := os.WriteFile(filepath.Join(value.resources, name), template, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	var commands []string
	if err := repairWith(value, func(arguments ...string) error { commands = append(commands, strings.Join(arguments, " ")); return nil }); err != nil {
		t.Fatal(err)
	}
	if !regular(value.hostPlist) || !regular(value.connectorPlist) {
		t.Fatal("service definitions were not installed")
	}
	want := []string{"bootout", "bootout", "bootstrap", "enable", "bootstrap", "enable"}
	if len(commands) != len(want) {
		t.Fatalf("commands=%v", commands)
	}
	for index, prefix := range want {
		if !strings.HasPrefix(commands[index], prefix) {
			t.Fatalf("commands[%d]=%q", index, commands[index])
		}
	}
}

func TestUninstallKeepsDataUnlessPurgeWasConfirmed(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(map[bool]string{false: "keep", true: "purge"}[purge], func(t *testing.T) {
			value := testPaths(t.TempDir())
			for _, directory := range []string{value.app, filepath.Dir(value.hostPlist), value.applicationData, value.logs} {
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for _, plist := range []string{value.hostPlist, value.connectorPlist} {
				if err := os.WriteFile(plist, []byte("plist"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var commands []string
			if err := uninstallWith(value, purge, func(arguments ...string) error { commands = append(commands, strings.Join(arguments, " ")); return nil }); err != nil {
				t.Fatal(err)
			}
			if directory(value.app) || regular(value.hostPlist) || regular(value.connectorPlist) {
				t.Fatal("installed product files remain")
			}
			if directory(value.applicationData) == purge || directory(value.logs) == purge {
				t.Fatalf("purge=%v data existence is wrong", purge)
			}
			if len(commands) != 2 || !strings.HasPrefix(commands[0], "bootout") || !strings.HasPrefix(commands[1], "bootout") {
				t.Fatalf("commands=%v", commands)
			}
		})
	}
}

func TestDrainClosesConnectorAdmissionBeforeHost(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, plist := range []string{value.hostPlist, value.connectorPlist} {
		if err := os.WriteFile(plist, []byte("plist"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var commands []string
	if err := drainWith(value, func(arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || !strings.Contains(commands[0], connectorLabel) || !strings.Contains(commands[1], hostLabel) {
		t.Fatalf("drain order=%v", commands)
	}
}

func testPaths(root string) paths {
	home := filepath.Join(root, "home")
	return paths{
		home: home, app: filepath.Join(root, "Agent VMM.app"), resources: filepath.Join(root, "resources"),
		hostPlist: filepath.Join(home, "Library", "LaunchAgents", hostLabel+".plist"), connectorPlist: filepath.Join(home, "Library", "LaunchAgents", connectorLabel+".plist"),
		applicationData: filepath.Join(home, "Library", "Application Support", "AgentVMM"), logs: filepath.Join(home, "Library", "Logs", "AgentVMM"),
	}
}

func TestAtomicWriteReplacesDefinition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.plist")
	if err := atomicWrite(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "second" {
		t.Fatalf("definition=%q err=%v", data, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
}
