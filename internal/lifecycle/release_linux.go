package lifecycle

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ReleaseManifestName names the release inventory of a Linux release
// directory. Its SHA-256 is the content ID and the installed directory name.
const ReleaseManifestName = "release.json"

var contentIDPattern = regexp.MustCompile(`\A[0-9a-f]{64}\z`)

type releaseFile struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Executable bool   `json:"executable"`
}

type releaseManifest struct {
	Version   int           `json:"version"`
	ReleaseID string        `json:"release_id"`
	Files     []releaseFile `json:"files"`
}

// ReleaseID returns the release identity of a Linux release directory. It
// does not hash the release files.
func ReleaseID(directory string) (string, error) {
	manifest, _, err := readReleaseManifest(directory)
	return manifest.ReleaseID, err
}

func readReleaseManifest(directory string) (releaseManifest, string, error) {
	file, err := os.Open(filepath.Join(directory, ReleaseManifestName))
	if err != nil {
		return releaseManifest{}, "", fmt.Errorf("read release inventory: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1<<20))
	if err != nil {
		return releaseManifest{}, "", fmt.Errorf("read release inventory: %w", err)
	}
	var manifest releaseManifest
	if json.Unmarshal(data, &manifest) != nil || manifest.Version != 1 || !operationIDPattern.MatchString(manifest.ReleaseID) || len(manifest.Files) == 0 {
		return releaseManifest{}, "", errors.New("release inventory is invalid")
	}
	seen := make(map[string]bool, len(manifest.Files))
	for _, file := range manifest.Files {
		if file.Path != filepath.Clean(file.Path) || filepath.IsAbs(file.Path) || file.Path == ReleaseManifestName ||
			strings.HasPrefix(file.Path, "..") || seen[file.Path] || file.Size < 0 || len(file.SHA256) != 64 {
			return releaseManifest{}, "", errors.New("release inventory entry is invalid")
		}
		seen[file.Path] = true
	}
	for _, required := range []string{"agent-vmm", "agent-vmm-data", "guest/manifest.json"} {
		if !seen[required] {
			return releaseManifest{}, "", fmt.Errorf("release inventory does not list %s", required)
		}
	}
	digest := sha256.Sum256(data)
	return manifest, hex.EncodeToString(digest[:]), nil
}

// copyRelease copies every listed file and rejects a source tree that has a
// different file set or different file content. It syncs every copied
// directory so a published release survives a power loss.
func copyRelease(source, target string, manifest releaseManifest) error {
	files := make(map[string]releaseFile, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	var directories []string
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(source, path)
		switch {
		case entry.IsDir():
			directories = append(directories, filepath.Join(target, relative))
			if relative == "." {
				return os.MkdirAll(target, 0o755)
			}
			return os.Mkdir(filepath.Join(target, relative), 0o755)
		case !entry.Type().IsRegular():
			return fmt.Errorf("release entry %s is not a regular file", relative)
		case relative == ReleaseManifestName:
			return copyReleaseFile(path, filepath.Join(target, relative), releaseFile{}, false)
		}
		file, listed := files[relative]
		if !listed {
			return fmt.Errorf("release entry %s is not listed in the release inventory", relative)
		}
		delete(files, relative)
		return copyReleaseFile(path, filepath.Join(target, relative), file, true)
	})
	if err != nil {
		return err
	}
	for path := range files {
		return fmt.Errorf("release file %s is missing", path)
	}
	for _, directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	return errors.Join(syncErr, directory.Close())
}

func copyReleaseFile(source, target string, expected releaseFile, verify bool) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	mode := os.FileMode(0o644)
	if expected.Executable {
		mode = 0o755
	}
	output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	syncErr := output.Sync()
	closeErr := output.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		return err
	}
	if verify && (written != expected.Size || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256) {
		return fmt.Errorf("release file %s does not match the release inventory", expected.Path)
	}
	return nil
}

// installRelease publishes source as installRoot/<content-id>. A complete
// directory is published only by rename, so an existing one is complete.
func installRelease(value paths, source string) (string, error) {
	manifest, contentID, err := readReleaseManifest(source)
	if err != nil {
		return "", err
	}
	target := filepath.Join(value.installRoot, contentID)
	if directory(target) {
		return contentID, nil
	}
	if err := os.MkdirAll(value.installRoot, 0o755); err != nil {
		return "", err
	}
	staging := filepath.Join(value.installRoot, ".install-"+contentID+"-"+randomSuffix())
	defer os.RemoveAll(staging)
	if err := copyRelease(source, staging, manifest); err != nil {
		return "", fmt.Errorf("install release %s: %w", manifest.ReleaseID, err)
	}
	if err := os.Rename(staging, target); err != nil {
		return "", fmt.Errorf("publish release %s: %w", manifest.ReleaseID, err)
	}
	return contentID, syncDirectory(value.installRoot)
}

func currentContentID(value paths) string {
	target, err := os.Readlink(value.app)
	if err != nil || !contentIDPattern.MatchString(target) {
		return ""
	}
	return target
}

// switchCurrent replaces the current link in one rename.
func switchCurrent(value paths, contentID string) error {
	if currentContentID(value) == contentID {
		return nil
	}
	link := filepath.Join(value.installRoot, ".current-"+randomSuffix())
	if err := os.Symlink(contentID, link); err != nil {
		return err
	}
	if err := os.Rename(link, value.app); err != nil {
		os.Remove(link)
		return fmt.Errorf("switch current release: %w", err)
	}
	return syncDirectory(value.installRoot)
}

// pruneReleases removes installed releases other than the current release.
func pruneReleases(value paths) error {
	current := currentContentID(value)
	entries, err := os.ReadDir(value.installRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && contentIDPattern.MatchString(entry.Name()) && entry.Name() != current {
			if err := os.RemoveAll(filepath.Join(value.installRoot, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func randomSuffix() string {
	var value [8]byte
	_, _ = rand.Read(value[:])
	return hex.EncodeToString(value[:])
}
