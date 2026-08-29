package bundle

import (
	"bytes"
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
	HostBuildInfoRelativePath       = "Contents/Resources/host-build-info.json"
	HostLicenseRelativePath         = "Contents/Resources/LICENSE"
)

var hostGuestArtifacts = []string{
	"Image",
	"initramfs.xz",
	"rootfs.squashfs",
	ManifestName,
}

// ValidateHostApp checks the on-disk shape that lifecycle code is allowed to
// execute. It deliberately does not verify macOS code signatures; codesign is
// a platform release gate, while this check remains runnable on every CI host.
func ValidateHostApp(directory string) error {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	if filepath.Base(directory) != HostAppName {
		return fmt.Errorf("host app must be named %q", HostAppName)
	}
	if err := requireDirectory(filepath.Join(directory, "Contents")); err != nil {
		return err
	}
	for _, relative := range []string{
		"Contents/Info.plist",
		HostExecutableRelativePath,
		HostDataHelperRelativePath,
		HostLifecycleHelperRelativePath,
		HostLaunchdTemplateRelativePath,
		HostBuildInfoRelativePath,
		HostLicenseRelativePath,
	} {
		if err := requireRegular(filepath.Join(directory, filepath.FromSlash(relative)), relative, false); err != nil {
			return err
		}
	}
	for _, relative := range []string{HostExecutableRelativePath, HostDataHelperRelativePath, HostLifecycleHelperRelativePath} {
		if err := requireRegular(filepath.Join(directory, filepath.FromSlash(relative)), relative, true); err != nil {
			return err
		}
	}
	guest := filepath.Join(directory, filepath.FromSlash(HostGuestRelativePath))
	if err := requireDirectory(guest); err != nil {
		return err
	}
	for _, name := range hostGuestArtifacts {
		if err := requireRegular(filepath.Join(guest, name), filepath.ToSlash(filepath.Join(HostGuestRelativePath, name)), false); err != nil {
			return err
		}
	}
	if err := validateExecutableDirectory(filepath.Join(directory, "Contents", "MacOS"), []string{"agent-vmm-host"}); err != nil {
		return err
	}
	if err := validateExecutableDirectory(filepath.Join(directory, "Contents", "Helpers"), []string{"agent-vmm-data", "agent-vmm-lifecycle"}); err != nil {
		return err
	}
	if err := validateInfoPlist(filepath.Join(directory, "Contents", "Info.plist")); err != nil {
		return err
	}
	if err := validateLaunchdTemplate(filepath.Join(directory, filepath.FromSlash(HostLaunchdTemplateRelativePath))); err != nil {
		return err
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
