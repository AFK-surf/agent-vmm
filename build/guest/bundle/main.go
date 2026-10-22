package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

func main() {
	directory := flag.String("directory", "", "bundle directory")
	rootFSCompressor := flag.String("rootfs-compressor", "", "SquashFS compressor")
	rootFSDictionary := flag.Int64("rootfs-dictionary", 0, "SquashFS dictionary bytes")
	rootFSFilter := flag.String("rootfs-filter", "", "SquashFS executable filter")
	bundledMCP := flag.Bool("bundled-mcp", false, "include the standalone MCP appliance artifacts")
	flag.Parse()
	if *directory == "" || !validRootFS(*rootFSCompressor, *rootFSDictionary, *rootFSFilter) {
		flag.Usage()
		os.Exit(2)
	}
	artifacts := map[string]bundle.Artifact{}
	paths := map[string]string{"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs"}
	if *bundledMCP {
		paths["mcp_appliance_digest"] = "mcp-appliance.digest"
		paths["mcp_appliance_manifest"] = "mcp-appliance/manifest.pb"
	}
	for name, path := range paths {
		info, err := os.Stat(filepath.Join(*directory, path))
		check(err)
		artifacts[name] = bundle.Artifact{Path: path, Size: info.Size(), SHA256: fileDigest(filepath.Join(*directory, path))}
	}
	manifest := bundle.Manifest{
		SchemaVersion: 1, Architecture: "arm64",
		Artifacts: artifacts,
		RootFS:    bundle.RootFS{Filesystem: "squashfs", Compressor: *rootFSCompressor, BlockSize: 1 << 20, Dictionary: *rootFSDictionary, Filter: *rootFSFilter},
	}
	manifestBytes, err := json.Marshal(manifest)
	check(err)
	check(os.WriteFile(filepath.Join(*directory, bundle.ManifestName), manifestBytes, 0o644))
}

func validRootFS(compressor string, dictionary int64, filter string) bool {
	return compressor == "xz" && dictionary == 1<<20 && filter == "arm64-bcj" ||
		compressor == "zstd" && dictionary == 0 && filter == "none"
}

func fileDigest(path string) string {
	data := mustRead(path)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
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
