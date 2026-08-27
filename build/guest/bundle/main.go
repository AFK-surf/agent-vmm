package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

func main() {
	directory := flag.String("directory", "", "bundle directory")
	lockPath := flag.String("lock", "", "guest lock")
	logicalBytes := flag.Int64("logical-bytes", 0, "root tree logical bytes")
	rootFSCompressor := flag.String("rootfs-compressor", "", "SquashFS compressor")
	rootFSDictionary := flag.Int64("rootfs-dictionary", 0, "SquashFS dictionary bytes")
	rootFSFilter := flag.String("rootfs-filter", "", "SquashFS executable filter")
	flag.Parse()
	if *directory == "" || *lockPath == "" || *logicalBytes <= 0 || !validRootFS(*rootFSCompressor, *rootFSDictionary, *rootFSFilter) {
		flag.Usage()
		os.Exit(2)
	}
	lockBytes := mustRead(*lockPath)
	var lock map[string]any
	check(json.Unmarshal(lockBytes, &lock))
	artifacts := map[string]bundle.Artifact{}
	for name, path := range map[string]string{"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs", "mcp_appliance_digest": "mcp-appliance.digest", "mcp_appliance_manifest": "mcp-appliance/manifest.pb"} {
		info, err := os.Stat(filepath.Join(*directory, path))
		check(err)
		artifacts[name] = bundle.Artifact{Path: path, Size: info.Size(), SHA256: fileDigest(filepath.Join(*directory, path))}
	}
	manifest := bundle.Manifest{
		SchemaVersion: 1, Architecture: "arm64", MinimumMacOS: "14.0",
		Artifacts: artifacts, Components: components(lock),
		RootFS: bundle.RootFS{LogicalBytes: *logicalBytes, Filesystem: "squashfs", Compressor: *rootFSCompressor, BlockSize: 1 << 20, Dictionary: *rootFSDictionary, Filter: *rootFSFilter},
	}
	manifestBytes, err := json.Marshal(manifest)
	check(err)
	check(os.WriteFile(filepath.Join(*directory, bundle.ManifestName), manifestBytes, 0o644))
}

func validRootFS(compressor string, dictionary int64, filter string) bool {
	return compressor == "xz" && dictionary == 1<<20 && filter == "arm64-bcj" ||
		compressor == "zstd" && dictionary == 0 && filter == "none"
}

func components(lock map[string]any) map[string]bundle.Component {
	result := map[string]bundle.Component{}
	for _, name := range []string{"containerd", "runsc", "runsc_shim", "buildkit", "kernel", "alpine"} {
		value, _ := lock[name].(map[string]any)
		version := stringValue(value, "version")
		digest := firstString(value, "archive_sha256", "archive_sha512", "binary_sha512", "apk_sha256", "minirootfs_sha256", "image")
		result[name] = bundle.Component{Version: version, Digest: digest}
	}
	return result
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
