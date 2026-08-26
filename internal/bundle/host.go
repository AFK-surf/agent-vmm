package bundle

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	HostAppName                     = "Agent VMM Host.app"
	HostBundleIdentifier            = "com.afk-surf.agent-vmm-host"
	HostExecutableRelativePath      = "Contents/MacOS/agent-vmm-host"
	HostDataHelperRelativePath      = "Contents/Helpers/agent-vmm-data"
	HostLifecycleHelperRelativePath = "Contents/Helpers/agent-vmm-lifecycle"
	HostGuestRelativePath           = "Contents/Resources/Guest"
	HostLaunchdTemplateRelativePath = "Contents/Resources/com.afk-surf.agent-vmm-host.plist.template"
	HostInventoryRelativePath       = "Contents/Resources/release-inventory.json"
	HostBuildInfoRelativePath       = "Contents/Resources/host-build-info.json"
	HostLicenseRelativePath         = "Contents/Resources/LICENSE"
	HostSBOMRelativePath            = "Contents/Resources/sbom.spdx.json"
	HostProvenanceRelativePath      = "Contents/Resources/provenance.json"
)

var hostGuestArtifacts = []string{
	"Image",
	"initramfs.xz",
	"rootfs.squashfs",
	ManifestName,
	SignatureName,
	"SHA256SUMS",
	"sbom.spdx.json",
	"provenance.json",
}

// ReleaseInventory is the non-secret, machine-readable inventory embedded in
// a Host App. It describes the fixed payload contract; the archive digest is
// intentionally owned by the outer signed release target.
type ReleaseInventory struct {
	SchemaVersion    int             `json:"schema_version"`
	Component        string          `json:"component"`
	Product          string          `json:"product"`
	BundleName       string          `json:"bundle_name"`
	BundleIdentifier string          `json:"bundle_identifier"`
	ReleaseID        string          `json:"release_id"`
	Identity         ReleaseIdentity `json:"identity"`
	Layout           HostLayout      `json:"layout"`
	UI               HostUI          `json:"ui"`
	MetadataScope    string          `json:"metadata_scope"`
	SourceRevision   string          `json:"source_revision,omitempty"`
}

type ReleaseIdentity struct {
	Profile     string `json:"profile"`
	CodeSigning string `json:"code_signing"`
	GuestSigner string `json:"guest_signer"`
}

type HostLayout struct {
	MainExecutable  string   `json:"main_executable"`
	Helpers         []string `json:"helpers"`
	Guest           string   `json:"guest"`
	LaunchdTemplate string   `json:"launchd_template"`
	BuildInfo       string   `json:"build_info"`
	License         string   `json:"license"`
	SBOM            string   `json:"sbom"`
	Provenance      string   `json:"provenance"`
	Inventory       string   `json:"inventory"`
}

type HostUI struct {
	Headless        bool `json:"headless"`
	SwiftUI         bool `json:"swiftui"`
	DebugExecutable bool `json:"debug_executable"`
	Window          bool `json:"window"`
	Menu            bool `json:"menu"`
	Dock            bool `json:"dock"`
}

// ValidateHostApp checks the on-disk shape that lifecycle code is allowed to
// execute. It deliberately does not verify macOS code signatures; codesign is
// a platform release gate, while this check remains runnable on every CI host.
func ValidateHostApp(directory string) (*ReleaseInventory, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if filepath.Base(directory) != HostAppName {
		return nil, fmt.Errorf("host app must be named %q", HostAppName)
	}
	if err := requireDirectory(filepath.Join(directory, "Contents")); err != nil {
		return nil, err
	}
	for _, relative := range []string{
		"Contents/Info.plist",
		HostExecutableRelativePath,
		HostDataHelperRelativePath,
		HostLifecycleHelperRelativePath,
		HostLaunchdTemplateRelativePath,
		HostInventoryRelativePath,
		HostBuildInfoRelativePath,
		HostLicenseRelativePath,
		HostSBOMRelativePath,
		HostProvenanceRelativePath,
	} {
		if err := requireRegular(filepath.Join(directory, filepath.FromSlash(relative)), relative, false); err != nil {
			return nil, err
		}
	}
	for _, relative := range []string{HostExecutableRelativePath, HostDataHelperRelativePath, HostLifecycleHelperRelativePath} {
		if err := requireRegular(filepath.Join(directory, filepath.FromSlash(relative)), relative, true); err != nil {
			return nil, err
		}
	}
	guest := filepath.Join(directory, filepath.FromSlash(HostGuestRelativePath))
	if err := requireDirectory(guest); err != nil {
		return nil, err
	}
	for _, name := range hostGuestArtifacts {
		if err := requireRegular(filepath.Join(guest, name), filepath.ToSlash(filepath.Join(HostGuestRelativePath, name)), false); err != nil {
			return nil, err
		}
	}
	if err := validateExecutableDirectory(filepath.Join(directory, "Contents", "MacOS"), []string{"agent-vmm-host"}); err != nil {
		return nil, err
	}
	if err := validateExecutableDirectory(filepath.Join(directory, "Contents", "Helpers"), []string{"agent-vmm-data", "agent-vmm-lifecycle"}); err != nil {
		return nil, err
	}
	if err := validateInfoPlist(filepath.Join(directory, "Contents", "Info.plist")); err != nil {
		return nil, err
	}
	if err := validateLaunchdTemplate(filepath.Join(directory, filepath.FromSlash(HostLaunchdTemplateRelativePath))); err != nil {
		return nil, err
	}

	inventoryData, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(HostInventoryRelativePath)))
	if err != nil {
		return nil, fmt.Errorf("read host release inventory: %w", err)
	}
	var inventory ReleaseInventory
	decoder := json.NewDecoder(bytes.NewReader(inventoryData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inventory); err != nil {
		return nil, fmt.Errorf("decode host release inventory: %w", err)
	}
	if err := validateInventory(inventory); err != nil {
		return nil, err
	}
	return &inventory, nil
}

func validateInventory(inventory ReleaseInventory) error {
	if inventory.SchemaVersion != 1 || inventory.Component != "agent-vmm-host-runtime" || inventory.Product != "Agent VMM Host" ||
		inventory.BundleName != HostAppName || inventory.BundleIdentifier != HostBundleIdentifier || inventory.ReleaseID == "" {
		return errors.New("host release inventory identity is invalid")
	}
	if inventory.Identity.Profile == "test" {
		if inventory.Identity.CodeSigning != "adhoc" || inventory.Identity.GuestSigner != "development" {
			return errors.New("test host release identity is invalid")
		}
		if inventory.MetadataScope != "provided" && inventory.MetadataScope != "test-fixture-guest" {
			return errors.New("test host metadata scope is invalid")
		}
	} else if inventory.Identity.Profile == "release" {
		if inventory.Identity.CodeSigning != "developer-id" || inventory.Identity.GuestSigner != "release" {
			return errors.New("release host identity is invalid")
		}
		if inventory.MetadataScope != "provided" {
			return errors.New("release host metadata must be provided by the release workflow")
		}
	} else {
		return errors.New("host release identity profile is invalid")
	}
	wantHelpers := []string{HostDataHelperRelativePath, HostLifecycleHelperRelativePath}
	if inventory.Layout.MainExecutable != HostExecutableRelativePath ||
		!slices.Equal(inventory.Layout.Helpers, wantHelpers) ||
		inventory.Layout.Guest != HostGuestRelativePath ||
		inventory.Layout.LaunchdTemplate != HostLaunchdTemplateRelativePath ||
		inventory.Layout.BuildInfo != HostBuildInfoRelativePath ||
		inventory.Layout.License != HostLicenseRelativePath ||
		inventory.Layout.SBOM != HostSBOMRelativePath ||
		inventory.Layout.Provenance != HostProvenanceRelativePath ||
		inventory.Layout.Inventory != HostInventoryRelativePath {
		return errors.New("host release inventory layout is invalid")
	}
	if inventory.UI != (HostUI{Headless: true}) {
		return errors.New("host release inventory claims a UI surface")
	}
	return nil
}

func validateExecutableDirectory(directory string, allowed []string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read executable directory %q: %w", directory, err)
	}
	for _, entry := range entries {
		if !slices.Contains(allowed, entry.Name()) {
			return fmt.Errorf("unexpected executable %q", filepath.Join(directory, entry.Name()))
		}
	}
	return nil
}

func requireDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("required host directory %q: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("required host path %q is not a directory", path)
	}
	return nil
}

func requireRegular(path, relative string, executable bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("required host file %q: %w", relative, err)
	}
	if !info.Mode().IsRegular() || (executable && info.Mode()&0o111 == 0) {
		return fmt.Errorf("required host file %q has an invalid mode", relative)
	}
	return nil
}

func validateInfoPlist(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read host Info.plist: %w", err)
	}
	values, err := plistValues(data)
	if err != nil {
		return fmt.Errorf("decode host Info.plist: %w", err)
	}
	if values["CFBundleExecutable"] != "agent-vmm-host" || values["CFBundleIdentifier"] != HostBundleIdentifier ||
		values["CFBundlePackageType"] != "APPL" || values["LSBackgroundOnly"] != "true" {
		return errors.New("host Info.plist is not a background-only agent-vmm-host app")
	}
	for _, key := range []string{"LSUIElement", "NSMainNibFile", "NSMainStoryboardFile", "NSPrincipalClass", "CFBundleIconFile", "CFBundleIconName", "CFBundleURLTypes"} {
		if _, ok := values[key]; ok {
			return fmt.Errorf("host Info.plist contains UI key %q", key)
		}
	}
	return nil
}

func validateLaunchdTemplate(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read host launchd template: %w", err)
	}
	template := string(data)
	for _, required := range []string{
		"<string>com.afk-surf.agent-vmm-host</string>",
		"__APP__/Contents/MacOS/agent-vmm-host",
		"__APP__/Contents/Helpers/agent-vmm-data",
	} {
		if !strings.Contains(template, required) {
			return fmt.Errorf("host launchd template is missing %q", required)
		}
	}
	for _, forbidden := range []string{"Agent VMM.app", "__APP__/Contents/Helpers/agent-vmm-host</string>", "__APP__/Contents/Helpers/agent-vmm</string>"} {
		if strings.Contains(template, forbidden) {
			return fmt.Errorf("host launchd template contains forbidden path %q", forbidden)
		}
	}
	return nil
}

func plistValues(data []byte) (map[string]string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	values := make(map[string]string)
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return values, nil
		}
		if err != nil {
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var key string
		if err := decoder.DecodeElement(&key, &start); err != nil {
			return nil, err
		}
		var valueStart xml.StartElement
		for {
			valueToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			var ok bool
			valueStart, ok = valueToken.(xml.StartElement)
			if ok {
				break
			}
			if whitespace, ok := valueToken.(xml.CharData); ok && strings.TrimSpace(string(whitespace)) == "" {
				continue
			}
			return nil, errors.New("plist key has no value")
		}
		var value string
		if valueStart.Name.Local == "true" {
			value = "true"
		} else if valueStart.Name.Local == "false" {
			value = "false"
		} else if valueStart.Name.Local == "string" {
			if err := decoder.DecodeElement(&value, &valueStart); err != nil {
				return nil, err
			}
		} else {
			if err := skipElement(decoder, valueStart); err != nil {
				return nil, err
			}
		}
		values[key] = value
	}
}

func skipElement(decoder *xml.Decoder, start xml.StartElement) error {
	depth := 1
	for depth > 0 {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return nil
}
