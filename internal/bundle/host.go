package bundle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
)

var hostReleaseIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

const (
	HostAppName                     = "Agent VMM Host.app"
	HostBundleIdentifier            = "com.afk-surf.agent-vmm-host"
	HostExecutableRelativePath      = "Contents/MacOS/agent-vmm-host"
	HostDataHelperRelativePath      = "Contents/Helpers/agent-vmm-data"
	HostLifecycleHelperRelativePath = "Contents/Helpers/agent-vmm-lifecycle"
	HostManagedCLIRelativePath      = "Contents/Helpers/agent-vmm"
	HostServiceExecutorRelativePath = "Contents/Helpers/agent-vmm-service-executor"
	HostGuestRelativePath           = "Contents/Resources/Guest"
	HostInventoryRelativePath       = "Contents/Resources/release-inventory.json"
)

// HostReleaseID reads the release identity from the Host inventory.
func HostReleaseID(directory string) (string, error) {
	data, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(HostInventoryRelativePath)))
	if err != nil {
		return "", err
	}
	var inventory struct {
		ReleaseID string `json:"release_id"`
	}
	if json.Unmarshal(data, &inventory) != nil || !hostReleaseIDPattern.MatchString(inventory.ReleaseID) {
		return "", errors.New("host release inventory is invalid")
	}
	return inventory.ReleaseID, nil
}
