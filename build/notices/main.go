package main

import (
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
)

type paths []string

func (p *paths) String() string         { return strings.Join(*p, ",") }
func (p *paths) Set(value string) error { *p = append(*p, value); return nil }

type notice struct {
	section, name, version, license, origin string
	texts                                   map[string]string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("notices", flag.ContinueOnError)
	output := flags.String("o", "", "output file")
	licenses := flags.String("licenses", "third_party/licenses", "fallback license texts")
	target := flags.String("cargo-target", "", "Cargo target triple")
	var binaries, databases, extras, manifests paths
	flags.Var(&manifests, "cargo-manifest", "Cargo package manifest (repeatable)")
	flags.Var(&binaries, "go-binary", "Go executable or directory (repeatable)")
	flags.Var(&databases, "apk-db", "APK installed database (repeatable)")
	flags.Var(&extras, "extra", "additional NAME=SPDX-ID component (repeatable)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *output == "" || flags.NArg() != 0 || (len(manifests) != 0 && *target == "") {
		return fmt.Errorf("usage: notices -o FILE [-licenses DIRECTORY] [-go-binary PATH] [-cargo-manifest FILE -cargo-target TRIPLE] [-apk-db FILE] [-extra NAME=SPDX-ID]")
	}
	var notices []notice
	for _, extra := range extras {
		name, license, ok := strings.Cut(extra, "=")
		if !ok || name == "" || license == "" {
			return fmt.Errorf("invalid extra component %q", extra)
		}
		texts, err := fallbackTexts(license, *licenses)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		notices = append(notices, notice{section: "Go", name: name, license: license, texts: texts})
	}
	modules := map[string]debug.Module{}
	for _, path := range binaries {
		if err := filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.Type().IsRegular() {
				return nil
			}
			info, err := buildinfo.ReadFile(path)
			if err != nil {
				return nil
			}
			for _, module := range binaryModules(info) {
				modules[module.Path+"@"+module.Version] = module
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if len(modules) > 0 {
		directory, err := os.MkdirTemp("", "agent-vmm-notices-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(directory)
		if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module notices\n"), 0o600); err != nil {
			return err
		}
		for _, key := range sortedKeys(modules) {
			module := modules[key]
			cmd := exec.Command("go", "mod", "download", "-json", key)
			cmd.Dir = directory
			data, err := cmd.Output()
			if err != nil {
				return fmt.Errorf("download %s: %w: %s", key, err, bytes.TrimSpace(data))
			}
			var download struct{ Dir, Error string }
			if err := json.Unmarshal(data, &download); err != nil {
				return err
			}
			if download.Error != "" {
				return fmt.Errorf("download %s: %s", key, download.Error)
			}
			texts, err := licenseFiles(download.Dir)
			if err != nil {
				return err
			}
			if len(texts) == 0 {
				return fmt.Errorf("Go module %s has no license files", key)
			}
			notices = append(notices, notice{section: "Go", name: module.Path, version: module.Version, texts: texts})
		}
	}
	for _, manifest := range manifests {
		cmd := exec.Command("cargo", "metadata", "--locked", "--format-version", "1", "--manifest-path", manifest, "--filter-platform", *target)
		data, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("cargo metadata for %s: %w", manifest, err)
		}
		rust, err := cargoNotices(data, *licenses)
		if err != nil {
			return err
		}
		notices = append(notices, rust...)
	}
	for _, path := range databases {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		packages, err := apkNotices(string(data), *licenses)
		if err != nil {
			return err
		}
		notices = append(notices, packages...)
	}

	return writeNotices(*output, notices)
}

func binaryModules(info *debug.BuildInfo) []debug.Module {
	var result []debug.Module
	modules := append([]*debug.Module{&info.Main}, info.Deps...)
	for _, module := range modules {
		if module.Path == "" || module.Path == "github.com/AFK-surf/agent-vmm" {
			continue
		}
		if module.Replace != nil {
			module = module.Replace
		}
		copy := *module
		if copy.Version == "(devel)" || copy.Version == "" {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" {
					copy.Version = setting.Value
				}
			}
		}
		if module == &info.Main && (copy.Version == "" || copy.Version == "(devel)") {
			continue
		}
		result = append(result, copy)
	}
	return result
}

var licenseName = regexp.MustCompile(`(?i)^(licen[cs]e|copying|notice|copyright|unlicense)`)
var licenseID = regexp.MustCompile(`[A-Za-z0-9][A-Za-z0-9.+-]*`)

func licenseFiles(directory string) (map[string]string, error) {
	texts := map[string]string{}
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "testdata" {
			return filepath.SkipDir
		}
		if !entry.Type().IsRegular() || !licenseName.MatchString(entry.Name()) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		texts[relative] = string(data)
		return nil
	})
	return texts, err
}

func fallbackTexts(expression, directory string) (map[string]string, error) {
	texts := map[string]string{}
	for _, id := range licenseID.FindAllString(expression, -1) {
		if id == "AND" || id == "OR" || id == "WITH" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, id+".txt"))
		if err != nil {
			return nil, fmt.Errorf("missing license text %s: %w", id, err)
		}
		texts[id] = string(data)
	}
	if len(texts) == 0 {
		return nil, fmt.Errorf("missing license expression")
	}
	return texts, nil
}

func cargoNotices(data []byte, licenses string) ([]notice, error) {
	var metadata struct {
		Packages []struct {
			ID, Name, Version, Source, License string
			ManifestPath                       string `json:"manifest_path"`
		}
		Resolve struct {
			Root  string
			Nodes []struct {
				ID   string
				Deps []struct {
					Pkg   string
					Kinds []struct{ Kind *string } `json:"dep_kinds"`
				}
			}
		}
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}
	if metadata.Resolve.Root == "" {
		return nil, fmt.Errorf("cargo metadata has no root package")
	}
	graph := map[string][]string{}
	for _, node := range metadata.Resolve.Nodes {
		for _, dep := range node.Deps {
			for _, kind := range dep.Kinds {
				if kind.Kind == nil {
					graph[node.ID] = append(graph[node.ID], dep.Pkg)
					break
				}
			}
		}
	}
	visited := map[string]bool{}
	var visit func(string)
	visit = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true
		for _, dep := range graph[id] {
			visit(dep)
		}
	}
	visit(metadata.Resolve.Root)
	var result []notice
	for _, pkg := range metadata.Packages {
		if !visited[pkg.ID] || pkg.Source == "" {
			continue
		}
		texts, err := licenseFiles(filepath.Dir(pkg.ManifestPath))
		if err != nil {
			return nil, err
		}
		if len(texts) == 0 {
			texts, err = fallbackTexts(pkg.License, licenses)
			if err != nil {
				return nil, fmt.Errorf("Rust crate %s@%s: %w", pkg.Name, pkg.Version, err)
			}
		}
		result = append(result, notice{section: "Rust", name: pkg.Name, version: pkg.Version, license: pkg.License, texts: texts})
	}
	return result, nil
}

func apkNotices(data, licenses string) ([]notice, error) {
	var result []notice
	for _, paragraph := range strings.Split(strings.TrimSpace(data), "\n\n") {
		pkg := notice{section: "Alpine"}
		for _, line := range strings.Split(paragraph, "\n") {
			if len(line) < 2 || line[1] != ':' {
				continue
			}
			switch line[0] {
			case 'P':
				pkg.name = line[2:]
			case 'V':
				pkg.version = line[2:]
			case 'L':
				pkg.license = line[2:]
			case 'o':
				pkg.origin = line[2:]
			}
		}
		if pkg.name == "" {
			continue
		}
		pkg.texts = map[string]string{}
		for _, id := range licenseID.FindAllString(pkg.license, -1) {
			if id == "AND" || id == "OR" || id == "WITH" {
				continue
			}
			data, err := os.ReadFile(filepath.Join(licenses, id+".txt"))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			pkg.texts[id] = string(data)
		}
		result = append(result, pkg)
	}
	return result, nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func writeNotices(output string, notices []notice) error {
	sort.Slice(notices, func(i, j int) bool {
		a, b := notices[i], notices[j]
		return a.section+"\x00"+a.name+"\x00"+a.version < b.section+"\x00"+b.name+"\x00"+b.version
	})
	var result bytes.Buffer
	licenseTexts := map[string]string{}
	result.WriteString("THIRD-PARTY NOTICES\n\n")
	for _, section := range []string{"Go", "Rust", "Alpine"} {
		fmt.Fprintf(&result, "%s\n%s\n", section, strings.Repeat("=", len(section)))
		seen := map[string]bool{}
		for _, pkg := range notices {
			if pkg.section != section {
				continue
			}
			key := pkg.name + "@" + pkg.version
			if seen[key] {
				continue
			}
			seen[key] = true
			fmt.Fprintf(&result, "\n%s\n", key)
			if pkg.license != "" {
				fmt.Fprintf(&result, "License: %s\n", pkg.license)
			}
			if pkg.origin != "" {
				fmt.Fprintf(&result, "Origin: %s\n", pkg.origin)
			}
			for _, name := range sortedKeys(pkg.texts) {
				if slices.Contains(licenseID.FindAllString(pkg.license, -1), name) {
					licenseTexts[name] = pkg.texts[name]
					continue
				}
				fmt.Fprintf(&result, "\n--- %s ---\n%s\n", name, strings.TrimSpace(pkg.texts[name]))
			}
		}
		result.WriteByte('\n')
	}
	if len(licenseTexts) != 0 {
		result.WriteString("License texts\n=============\n")
		for _, id := range sortedKeys(licenseTexts) {
			fmt.Fprintf(&result, "\n--- %s ---\n%s\n", id, strings.TrimSpace(licenseTexts[id]))
		}
	}
	return os.WriteFile(output, result.Bytes(), 0o644)
}
