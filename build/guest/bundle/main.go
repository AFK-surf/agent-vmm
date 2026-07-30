package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

func main() {
	directory := flag.String("directory", "", "bundle directory")
	lockPath := flag.String("lock", "", "guest lock")
	keyPath := flag.String("key", "", "Ed25519 seed")
	signer := flag.String("signer", "", "signer identity")
	logicalBytes := flag.Int64("logical-bytes", 0, "root tree logical bytes")
	flag.Parse()
	if *directory == "" || *lockPath == "" || *keyPath == "" || *signer == "" || *logicalBytes <= 0 {
		flag.Usage()
		os.Exit(2)
	}
	lockBytes := mustRead(*lockPath)
	var lock map[string]any
	check(json.Unmarshal(lockBytes, &lock))
	lockDigest := sha256.Sum256(lockBytes)

	sbom := map[string]any{
		"spdxVersion": "SPDX-2.3", "dataLicense": "CC0-1.0", "SPDXID": "SPDXRef-DOCUMENT",
		"name": "agent-vmm-guest", "documentNamespace": "https://github.com/AFK-surf/agent-vmm/guest/" + hex.EncodeToString(lockDigest[:]),
		"creationInfo": map[string]any{"creators": []string{"Tool: agent-vmm-bundle"}, "created": "1970-01-01T00:00:00Z"},
		"packages":     lockPackages(lock),
	}
	writeJSON(filepath.Join(*directory, "sbom.spdx.json"), sbom)
	provenance := map[string]any{
		"_type": "https://in-toto.io/Statement/v1", "predicateType": "https://slsa.dev/provenance/v1",
		"subject": []any{}, "predicate": map[string]any{"buildDefinition": map[string]any{"buildType": "https://github.com/AFK-surf/agent-vmm/guest/v1", "externalParameters": map[string]any{"lock_sha256": hex.EncodeToString(lockDigest[:])}}, "runDetails": map[string]any{"builder": map[string]any{"id": "https://github.com/AFK-surf/agent-vmm/build/guest"}}},
	}
	writeJSON(filepath.Join(*directory, "provenance.json"), provenance)

	checksumNames := []string{"Image", "initramfs.xz", "rootfs.squashfs", "sbom.spdx.json", "provenance.json"}
	lines := make([]string, 0, len(checksumNames))
	for _, name := range checksumNames {
		lines = append(lines, fileDigest(filepath.Join(*directory, name))+"  "+name)
	}
	check(os.WriteFile(filepath.Join(*directory, "SHA256SUMS"), []byte(strings.Join(lines, "\n")+"\n"), 0o644))

	artifacts := map[string]bundle.Artifact{}
	for name, path := range map[string]string{"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs", "sbom": "sbom.spdx.json", "provenance": "provenance.json", "checksums": "SHA256SUMS"} {
		info, err := os.Stat(filepath.Join(*directory, path))
		check(err)
		artifacts[name] = bundle.Artifact{Path: path, Size: info.Size(), SHA256: fileDigest(filepath.Join(*directory, path))}
	}
	manifest := bundle.Manifest{
		SchemaVersion: 1, Architecture: "arm64", MinimumMacOS: "14.0", Signer: *signer,
		Artifacts: artifacts, Components: components(lock),
		RootFS: bundle.RootFS{LogicalBytes: *logicalBytes, Filesystem: "squashfs", Compressor: "xz", BlockSize: 1 << 20, Dictionary: 1 << 20, Filter: "arm64-bcj"},
	}
	manifestBytes, err := json.Marshal(manifest)
	check(err)
	check(os.WriteFile(filepath.Join(*directory, bundle.ManifestName), manifestBytes, 0o644))
	seedText := strings.TrimSpace(string(mustRead(*keyPath)))
	seed, err := hex.DecodeString(seedText)
	check(err)
	if len(seed) != ed25519.SeedSize {
		panic("invalid Ed25519 seed")
	}
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(seed), manifestBytes)
	check(os.WriteFile(filepath.Join(*directory, bundle.SignatureName), []byte(base64.StdEncoding.EncodeToString(signature)+"\n"), 0o644))
}

func components(lock map[string]any) map[string]bundle.Component {
	result := map[string]bundle.Component{}
	for _, name := range []string{"containerd", "runsc", "runsc_shim", "buildkit", "kernel", "alpine"} {
		value, _ := lock[name].(map[string]any)
		version := stringValue(value, "version")
		if name == "runsc_shim" {
			version = stringValue(value, "version")
		}
		digest := firstString(value, "archive_sha256", "archive_sha512", "binary_sha512", "apk_sha256", "minirootfs_sha256", "image")
		result[name] = bundle.Component{Version: version, Digest: digest}
	}
	return result
}

func lockPackages(lock map[string]any) []map[string]string {
	packages := []map[string]string{}
	values, _ := lock["packages"].(map[string]any)
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		packages = append(packages, map[string]string{"SPDXID": "SPDXRef-Package-" + name, "name": name, "versionInfo": fmt.Sprint(values[name]), "downloadLocation": "NOASSERTION"})
	}
	return packages
}

func writeJSON(path string, value any) {
	data, err := json.Marshal(value)
	check(err)
	check(os.WriteFile(path, append(data, '\n'), 0o644))
}

func fileDigest(path string) string {
	data := mustRead(path)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func stringValue(values map[string]any, key string) string { return fmt.Sprint(values[key]) }

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			return fmt.Sprint(value)
		}
	}
	return ""
}

func mustRead(path string) []byte {
	data, err := os.ReadFile(path)
	check(err)
	return data
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
