package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/lifecycle"
)

const managedServiceAppName = "current"

func managedPreparedUpdatePath(targetApp, requestID string) string {
	return filepath.Join(filepath.Dir(targetApp), "."+requestID)
}

// extractManagedUpdate unpacks the release tar into a hidden staging
// directory. Lifecycle update verifies every file against release.json.
func extractManagedUpdate(_ context.Context, archivePath, stagedApp, targetRelease string) error {
	extracted, err := os.MkdirTemp(filepath.Dir(stagedApp), ".agent-vmm-update-extract-")
	if err != nil {
		return managedUpdateFailure("preparation", err)
	}
	defer os.RemoveAll(extracted)
	if err := extractManagedReleaseArchive(archivePath, extracted); err != nil {
		return managedUpdateFailure("preparation", fmt.Errorf("Host artifact extraction failed: %w", err))
	}
	if release, releaseErr := lifecycle.ReleaseID(extracted); releaseErr != nil || release != targetRelease {
		return managedUpdateFailure("preparation", errors.New("Host artifact release inventory does not match the selected target"))
	}
	if err := os.Rename(extracted, stagedApp); err != nil {
		return managedUpdateFailure("preparation", err)
	}
	return nil
}

// extractManagedReleaseArchive accepts only directories and regular files
// under one top-level directory, and writes them without that directory.
func extractManagedReleaseArchive(archivePath, target string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	decompressed, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	reader := tar.NewReader(decompressed)
	top := ""
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := path.Clean(header.Name)
		first, relative, _ := strings.Cut(name, "/")
		if top == "" {
			top = first
		}
		if first != top || path.IsAbs(name) || first == ".." || strings.HasPrefix(relative, "..") {
			return fmt.Errorf("archive entry %q is outside the release directory", header.Name)
		}
		destination := filepath.Join(target, filepath.FromSlash(relative))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(destination, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if relative == "" {
				return errors.New("archive top-level entry must be a directory")
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
				return err
			}
			output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(output, reader)
			if err := errors.Join(copyErr, output.Close()); err != nil {
				return err
			}
		default:
			return fmt.Errorf("archive entry %q is not a regular file or directory", header.Name)
		}
	}
}

func managedHostReleaseID(app string, _ bool) (string, error) {
	return lifecycle.ReleaseID(app)
}
