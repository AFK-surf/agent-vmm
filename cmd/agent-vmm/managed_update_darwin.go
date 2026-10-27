package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

const managedServiceAppName = bundle.HostAppName

func managedPreparedUpdatePath(targetApp, requestID string) string {
	return filepath.Join(filepath.Dir(targetApp), ".Agent VMM Host."+requestID+".app")
}

func extractManagedUpdate(ctx context.Context, archivePath, stagedApp, targetRelease string) error {
	extracted, err := os.MkdirTemp(filepath.Dir(stagedApp), ".agent-vmm-update-extract-")
	if err != nil {
		return managedUpdateFailure("preparation", err)
	}
	defer os.RemoveAll(extracted)
	command := exec.CommandContext(ctx, "/usr/bin/ditto", "-x", "-k", archivePath, extracted)
	if output, extractErr := command.CombinedOutput(); extractErr != nil {
		return managedUpdateFailure("preparation", fmt.Errorf("Host artifact extraction failed: %s", boundedManagedText(string(output), 256)))
	}
	prepared := filepath.Join(extracted, bundle.HostAppName)
	if release, releaseErr := managedHostReleaseID(prepared); releaseErr != nil || release != targetRelease {
		return managedUpdateFailure("preparation", errors.New("prepared Host app does not match the selected target"))
	}
	if err := os.Rename(prepared, stagedApp); err != nil {
		return managedUpdateFailure("preparation", err)
	}
	return nil
}

func managedHostReleaseID(app string) (string, error) {
	return bundle.HostReleaseID(app)
}
