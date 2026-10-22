package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"golang.org/x/sys/unix"
)

func updateApplication(value paths, sourceApp, targetReleaseID, requestID string) error {
	return updateApplicationWith(value, sourceApp, targetReleaseID, requestID, updateApplicationActions{
		releaseID: bundle.HostReleaseID,
		drain:     drain,
		replace:   atomicReplaceApp,
		repair:    repair,
		startVM:   ensureVMStarted,
		remove:    os.RemoveAll,
	})
}

func validatePreparedUpdate(value paths, sourceApp string) error {
	if filepath.Dir(sourceApp) != filepath.Dir(value.app) || sourceApp == value.app {
		return errors.New("prepared update must be a hidden sibling of the installed Host app")
	}
	return nil
}

func atomicReplaceApp(source, target string) error {
	if !directory(source) {
		return errors.New("staged application bundle is unavailable")
	}
	if directory(target) {
		if err := unix.RenameatxNp(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_SWAP); err != nil {
			return fmt.Errorf("application bundle could not be atomically replaced: %w", err)
		}
		return nil
	}
	if err := os.Rename(source, target); err != nil {
		return fmt.Errorf("application bundle could not be installed: %w", err)
	}
	return nil
}
