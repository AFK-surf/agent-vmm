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
	extractedApp := filepath.Join(extracted, bundle.HostAppName)
	if release, releaseErr := managedHostReleaseID(extractedApp, false); releaseErr != nil || release != targetRelease {
		return managedUpdateFailure("preparation", errors.New("Host artifact release inventory does not match the selected target"))
	}
	verify := exec.CommandContext(ctx, "/usr/bin/codesign", "--verify", "--deep", "--strict", extractedApp)
	if output, verifyErr := verify.CombinedOutput(); verifyErr != nil {
		return managedUpdateFailure("preparation", fmt.Errorf("Host artifact signature verification failed: %s", boundedManagedText(string(output), 256)))
	}
	copyCommand := exec.CommandContext(ctx, "/usr/bin/ditto", extractedApp, stagedApp)
	if output, copyErr := copyCommand.CombinedOutput(); copyErr != nil {
		_ = os.RemoveAll(stagedApp)
		return managedUpdateFailure("preparation", fmt.Errorf("Host artifact preparation failed: %s", boundedManagedText(string(output), 256)))
	}
	if release, releaseErr := managedHostReleaseID(stagedApp, true); releaseErr != nil || release != targetRelease {
		_ = os.RemoveAll(stagedApp)
		return managedUpdateFailure("preparation", errors.New("prepared Host app does not match the selected target"))
	}
	return nil
}

func managedHostReleaseID(app string, staged bool) (string, error) {
	return bundle.HostReleaseID(app, staged)
}
