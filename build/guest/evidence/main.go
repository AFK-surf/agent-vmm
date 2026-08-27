package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	statementType = "https://in-toto.io/Statement/v1"
	predicateType = "https://slsa.dev/provenance/v1"
	buildType     = "https://github.com/AFK-surf/agent-vmm/guest/v1"
)

type subject struct {
	Name   string            `json:"name"`
	Digest map[string]string `json:"digest"`
}

type dependency struct {
	URI    string            `json:"uri"`
	Digest map[string]string `json:"digest"`
}

type provenance struct {
	Type          string    `json:"_type"`
	Subject       []subject `json:"subject"`
	PredicateType string    `json:"predicateType"`
	Predicate     struct {
		BuildDefinition struct {
			BuildType            string         `json:"buildType"`
			ResolvedDependencies []dependency   `json:"resolvedDependencies"`
			ExternalParameters   map[string]any `json:"externalParameters"`
		} `json:"buildDefinition"`
		RunDetails struct {
			Builder struct {
				ID string `json:"id"`
			} `json:"builder"`
		} `json:"runDetails"`
	} `json:"predicate"`
}

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
	if len(args) == 0 {
		return errors.New("usage: evidence generate|verify [flags]")
	}
	flags := flag.NewFlagSet("evidence "+args[0], flag.ContinueOnError)
	bundle := flags.String("bundle", "", "Guest bundle directory")
	rootfs := flags.String("rootfs", "", "unpacked Guest root filesystem")
	lock := flags.String("lock", "", "Guest dependency lock")
	revision := flags.String("source-revision", "", "source Git revision")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *bundle == "" || *lock == "" || !regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`).MatchString(*revision) {
		return errors.New("bundle, lock, and a full lowercase hexadecimal source-revision are required")
	}
	switch args[0] {
	case "generate":
		return generate(*bundle, *lock, *revision)
	case "verify":
		if *rootfs == "" {
			return errors.New("rootfs is required for verification")
		}
		return verify(*bundle, *rootfs, *lock, *revision)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func generate(bundle, lock, revision string) error {
	manifestDigest, err := fileSHA256(filepath.Join(bundle, "manifest.json"))
	if err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	lockDigest, err := fileSHA256(lock)
	if err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	sbomDigest, err := fileSHA256(filepath.Join(bundle, "sbom.spdx.json"))
	if err != nil {
		return fmt.Errorf("SBOM: %w", err)
	}

	p := provenance{Type: statementType, Subject: []subject{{Name: "manifest.json", Digest: map[string]string{"sha256": manifestDigest}}}, PredicateType: predicateType}
	p.Predicate.BuildDefinition.BuildType = buildType
	p.Predicate.BuildDefinition.ResolvedDependencies = []dependency{
		{URI: "git+https://github.com/AFK-surf/agent-vmm", Digest: map[string]string{"gitCommit": revision}},
		{URI: "build/guest/lock.json", Digest: map[string]string{"sha256": lockDigest}},
		{URI: "sbom.spdx.json", Digest: map[string]string{"sha256": sbomDigest}},
	}
	p.Predicate.BuildDefinition.ExternalParameters = map[string]any{"architecture": "arm64"}
	p.Predicate.RunDetails.Builder.ID = "https://github.com/AFK-surf/agent-vmm/build/guest"
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(bundle, "provenance.json"), append(data, '\n'), 0o644)
}

func verify(bundle, rootfs, lock, revision string) error {
	if err := verifySBOM(filepath.Join(bundle, "sbom.spdx.json"), filepath.Join(rootfs, "lib/apk/db/installed")); err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(bundle, "provenance.json"))
	if err != nil {
		return fmt.Errorf("provenance: %w", err)
	}
	var actual provenance
	if err := json.Unmarshal(data, &actual); err != nil {
		return fmt.Errorf("provenance: %w", err)
	}
	manifestDigest, err := fileSHA256(filepath.Join(bundle, "manifest.json"))
	if err != nil {
		return err
	}
	lockDigest, err := fileSHA256(lock)
	if err != nil {
		return err
	}
	sbomDigest, err := fileSHA256(filepath.Join(bundle, "sbom.spdx.json"))
	if err != nil {
		return err
	}
	if actual.Type != statementType || actual.PredicateType != predicateType || actual.Predicate.BuildDefinition.BuildType != buildType || actual.Predicate.RunDetails.Builder.ID == "" {
		return errors.New("provenance statement metadata is invalid")
	}
	if len(actual.Subject) != 1 || actual.Subject[0].Name != "manifest.json" || actual.Subject[0].Digest["sha256"] != manifestDigest {
		return errors.New("provenance does not bind the current Guest manifest")
	}
	want := map[string]map[string]string{
		"git+https://github.com/AFK-surf/agent-vmm": {"gitCommit": revision},
		"build/guest/lock.json":                     {"sha256": lockDigest},
		"sbom.spdx.json":                            {"sha256": sbomDigest},
	}
	if len(actual.Predicate.BuildDefinition.ResolvedDependencies) != len(want) {
		return errors.New("provenance dependency set is incomplete")
	}
	for _, item := range actual.Predicate.BuildDefinition.ResolvedDependencies {
		expected, ok := want[item.URI]
		if !ok || len(item.Digest) != 1 {
			return fmt.Errorf("provenance dependency %q is unexpected", item.URI)
		}
		for algorithm, value := range expected {
			if item.Digest[algorithm] != value {
				return fmt.Errorf("provenance dependency %q has the wrong digest", item.URI)
			}
		}
		delete(want, item.URI)
	}
	if len(want) != 0 {
		return errors.New("provenance dependency set is incomplete")
	}
	return nil
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

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
