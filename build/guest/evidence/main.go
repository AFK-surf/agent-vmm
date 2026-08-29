package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type spdxDocument struct {
	SPDXVersion string `json:"spdxVersion"`
	Packages    []struct {
		Name            string `json:"name"`
		VersionInfo     string `json:"versionInfo"`
		LicenseDeclared string `json:"licenseDeclared"`
	} `json:"packages"`
}

type apkPackage struct {
	Name    string
	Version string
	License string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "verify" {
		return errors.New("usage: evidence verify -bundle DIRECTORY -rootfs DIRECTORY")
	}
	flags := flag.NewFlagSet("evidence verify", flag.ContinueOnError)
	bundle := flags.String("bundle", "", "Guest bundle directory")
	rootfs := flags.String("rootfs", "", "unpacked Guest root filesystem")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *bundle == "" || *rootfs == "" {
		return errors.New("bundle and rootfs are required")
	}
	return verifySBOM(filepath.Join(*bundle, "sbom.spdx.json"), filepath.Join(*rootfs, "lib/apk/db/installed"))
}

func verifySBOM(sbomPath, installedPath string) error {
	data, err := os.ReadFile(sbomPath)
	if err != nil {
		return fmt.Errorf("SBOM: %w", err)
	}
	var doc spdxDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("SBOM: %w", err)
	}
	if doc.SPDXVersion != "SPDX-2.3" || len(doc.Packages) == 0 {
		return errors.New("SBOM is not a non-empty SPDX 2.3 package inventory")
	}
	installedData, err := os.ReadFile(installedPath)
	if err != nil {
		return fmt.Errorf("installed APK database: %w", err)
	}
	installed := parseAPKInstalled(string(installedData))
	if len(installed) == 0 {
		return errors.New("installed APK database contains no packages")
	}
	indexed := make(map[string]string, len(doc.Packages))
	for _, pkg := range doc.Packages {
		indexed[pkg.Name+"\x00"+pkg.VersionInfo] = strings.TrimSpace(pkg.LicenseDeclared)
	}
	for _, pkg := range installed {
		if strings.TrimSpace(pkg.License) == "" {
			return fmt.Errorf("installed package %s@%s has no license declaration", pkg.Name, pkg.Version)
		}
		license, ok := indexed[pkg.Name+"\x00"+pkg.Version]
		if !ok {
			return fmt.Errorf("installed package %s@%s is missing from the SBOM", pkg.Name, pkg.Version)
		}
		if license == "" || license == "NOASSERTION" {
			return fmt.Errorf("SBOM package %s@%s has no declared license", pkg.Name, pkg.Version)
		}
	}
	return nil
}

func parseAPKInstalled(data string) []apkPackage {
	var result []apkPackage
	for _, paragraph := range strings.Split(strings.TrimSpace(data), "\n\n") {
		var pkg apkPackage
		for _, line := range strings.Split(paragraph, "\n") {
			if len(line) < 2 || line[1] != ':' {
				continue
			}
			switch line[0] {
			case 'P':
				pkg.Name = line[2:]
			case 'V':
				pkg.Version = line[2:]
			case 'L':
				pkg.License = line[2:]
			}
		}
		if pkg.Name != "" && pkg.Version != "" {
			result = append(result, pkg)
		}
	}
	return result
}
