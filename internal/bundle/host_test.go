package bundle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateHostAppAcceptsFixedRuntimeLayout(t *testing.T) {
	directory := hostFixture(t)
	inventory, err := ValidateHostApp(directory)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Layout.MainExecutable != HostExecutableRelativePath {
		t.Fatalf("inventory=%+v", inventory)
	}
}

func TestHostBuildMetadataSourcesMatchContract(t *testing.T) {
	if err := validateInfoPlist(filepath.Join("..", "..", "build", "host", "Info.plist")); err != nil {
		t.Fatal(err)
	}
	if err := validateLaunchdTemplate(filepath.Join("..", "..", "build", "host", "com.afk-surf.agent-vmm-host.plist.template")); err != nil {
		t.Fatal(err)
	}
}

func TestValidateHostAppRejectsUIAndDebugPayload(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(string)
	}{
		{
			name: "debug executable",
			mutate: func(directory string) {
				writeHostFile(t, filepath.Join(directory, "Contents", "Helpers", "agent-vmm"), "debug", 0o755)
			},
		},
		{
			name: "window metadata",
			mutate: func(directory string) {
				writeHostFile(t, filepath.Join(directory, "Contents", "Info.plist"), stringsWithWindowMetadata, 0o644)
			},
		},
		{
			name: "missing release identity",
			mutate: func(directory string) {
				data, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(HostInventoryRelativePath)))
				if err != nil {
					t.Fatal(err)
				}
				var inventory ReleaseInventory
				if err := json.Unmarshal(data, &inventory); err != nil {
					t.Fatal(err)
				}
				inventory.ReleaseID = ""
				data, err = json.Marshal(inventory)
				if err != nil {
					t.Fatal(err)
				}
				writeHostFile(t, filepath.Join(directory, filepath.FromSlash(HostInventoryRelativePath)), string(data), 0o644)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := hostFixture(t)
			test.mutate(directory)
			if _, err := ValidateHostApp(directory); err == nil {
				t.Fatal("invalid host app was accepted")
			}
		})
	}
}

const stringsWithWindowMetadata = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>agent-vmm-host</string>
<key>CFBundleIdentifier</key><string>com.afk-surf.agent-vmm-host</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>LSBackgroundOnly</key><true/>
<key>NSMainNibFile</key><string>MainMenu</string>
</dict></plist>`

func hostFixture(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), HostAppName)
	for _, relative := range []string{
		"Contents/MacOS",
		"Contents/Helpers",
		HostGuestRelativePath,
	} {
		if err := os.MkdirAll(filepath.Join(directory, filepath.FromSlash(relative)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeHostFile(t, filepath.Join(directory, "Contents", "Info.plist"), hostInfoPlist, 0o644)
	for _, relative := range []string{HostExecutableRelativePath, HostDataHelperRelativePath, HostLifecycleHelperRelativePath} {
		writeHostFile(t, filepath.Join(directory, filepath.FromSlash(relative)), "executable", 0o755)
	}
	for _, relative := range []string{
		HostLaunchdTemplateRelativePath,
		HostBuildInfoRelativePath,
		HostLicenseRelativePath,
	} {
		value := "metadata"
		if relative == HostLaunchdTemplateRelativePath {
			value = hostLaunchdTemplate
		}
		writeHostFile(t, filepath.Join(directory, filepath.FromSlash(relative)), value, 0o644)
	}
	for _, name := range hostGuestArtifacts {
		writeHostFile(t, filepath.Join(directory, filepath.FromSlash(HostGuestRelativePath), name), name, 0o644)
	}
	inventory := ReleaseInventory{
		SchemaVersion: 1, Component: "agent-vmm-host-runtime", Product: "Agent VMM Host",
		BundleName: HostAppName, BundleIdentifier: HostBundleIdentifier, ReleaseID: "agent-vmm-host-test",
		Layout: HostLayout{
			MainExecutable: HostExecutableRelativePath,
			Helpers:        []string{HostDataHelperRelativePath, HostLifecycleHelperRelativePath},
			Guest:          HostGuestRelativePath, LaunchdTemplate: HostLaunchdTemplateRelativePath,
			BuildInfo: HostBuildInfoRelativePath, License: HostLicenseRelativePath, Inventory: HostInventoryRelativePath,
		},
	}
	data, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	writeHostFile(t, filepath.Join(directory, filepath.FromSlash(HostInventoryRelativePath)), string(data), 0o644)
	return directory
}

const hostInfoPlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>agent-vmm-host</string>
<key>CFBundleIdentifier</key><string>com.afk-surf.agent-vmm-host</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>LSBackgroundOnly</key><true/>
</dict></plist>`

const hostLaunchdTemplate = `<plist><dict>
<key>Label</key><string>com.afk-surf.agent-vmm-host</string>
<key>ProgramArguments</key><array>
<string>__APP__/Contents/MacOS/agent-vmm-host</string>
<string>__APP__/Contents/Helpers/agent-vmm-data</string>
</array>
</dict></plist>`

func writeHostFile(t *testing.T, path, value string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), mode); err != nil {
		t.Fatal(err)
	}
}
