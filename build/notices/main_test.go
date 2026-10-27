package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

func TestNoticeInputs(t *testing.T) {
	t.Run("Bazel buildinfo without main module", func(t *testing.T) {
		info := &debug.BuildInfo{Deps: []*debug.Module{{Path: "example.org/dependency", Version: "v1.0.0"}}}
		modules := binaryModules(info)
		if len(modules) != 1 || modules[0].Path != "example.org/dependency" {
			t.Fatalf("modules = %#v", modules)
		}
	})
	t.Run("unversioned APK Go main module", func(t *testing.T) {
		info := &debug.BuildInfo{Main: debug.Module{Path: "example.org/main", Version: "(devel)"}}
		if modules := binaryModules(info); len(modules) != 0 {
			t.Fatalf("unversioned main modules = %#v", modules)
		}
		info.Settings = []debug.BuildSetting{{Key: "vcs.revision", Value: "revision"}}
		if modules := binaryModules(info); len(modules) != 1 || modules[0].Version != "revision" {
			t.Fatalf("versioned main modules = %#v", modules)
		}
	})
	t.Run("Rust license fallback and normal dependency closure", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "MIT.txt"), []byte("MIT license text"), 0o600); err != nil {
			t.Fatal(err)
		}
		data := fmt.Appendf(nil, `{"packages":[{"id":"root","name":"root"},{"id":"crate","name":"crate","version":"1","source":"registry+example","license":"MIT","manifest_path":%q},{"id":"dev","name":"dev","source":"registry+example","manifest_path":%q}],"resolve":{"root":"root","nodes":[{"id":"root","deps":[{"pkg":"crate","dep_kinds":[{"kind":null}]},{"pkg":"dev","dep_kinds":[{"kind":"dev"}]}]}]}}`, filepath.Join(directory, "Cargo.toml"), filepath.Join(directory, "Cargo.toml"))
		notices, err := cargoNotices(data, directory)
		if err != nil || len(notices) != 1 || notices[0].texts["MIT"] != "MIT license text" {
			t.Fatalf("notices = %#v, error = %v", notices, err)
		}
		if err := os.Remove(filepath.Join(directory, "MIT.txt")); err != nil {
			t.Fatal(err)
		}
		if _, err := cargoNotices(data, directory); err == nil || !strings.Contains(err.Error(), "crate@1") || !strings.Contains(err.Error(), "MIT") {
			t.Fatalf("missing license error = %v", err)
		}
	})
	t.Run("APK without local license text", func(t *testing.T) {
		notices, err := apkNotices("P:busybox\nV:1.0-r0\nL:GPL-2.0-only\no:busybox\n", t.TempDir())
		if err != nil || len(notices) != 1 || len(notices[0].texts) != 0 {
			t.Fatalf("notices = %#v, error = %v", notices, err)
		}
	})
	t.Run("APK license text deduplication", func(t *testing.T) {
		licenses := t.TempDir()
		if err := os.WriteFile(filepath.Join(licenses, "GPL-2.0-only.txt"), []byte("shared license text"), 0600); err != nil {
			t.Fatal(err)
		}
		notices, err := apkNotices("P:busybox\nV:1.0-r0\nL:GPL-2.0-only\no:busybox\n\nP:another\nV:1\nL:GPL-2.0-only\n", licenses)
		if err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "THIRD_PARTY_NOTICES")
		if err := writeNotices(output, append(notices, notices...)); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(data), "shared license text") != 1 || !strings.Contains(string(data), "License texts\n") {
			t.Fatalf("duplicate or missing license text: %s", data)
		}
		if strings.Count(string(data), "busybox@1.0-r0") != 1 || !strings.Contains(string(data), "License: GPL-2.0-only\nOrigin: busybox\n") {
			t.Fatalf("notices = %s", data)
		}
	})
}
