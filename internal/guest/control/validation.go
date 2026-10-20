package control

import (
	"errors"
	"path"
	"regexp"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

var opaqueID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)
var environmentKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateID(kind, value string) error {
	if !opaqueID.MatchString(value) {
		return errors.New(kind + " must be a 1-63 character opaque identifier")
	}
	if strings.Contains(kind, "namespace") && (isSystemNamespace(value) || value == runtimeContainerdNamespace) {
		return errors.New(kind + " does not name an ordinary environment")
	}
	return nil
}

func isSystemNamespace(value string) bool { return strings.HasPrefix(value, "system.") }

func validateSystemNamespace(value string) error {
	if !opaqueID.MatchString(value) || !isSystemNamespace(value) {
		return errors.New("system namespace must use the reserved system. prefix")
	}
	return nil
}

func validateRequestID(value string) error {
	if value == "" {
		return errors.New("request_id is required")
	}
	return nil
}

func validateDestination(value string) error {
	if !path.IsAbs(value) || value == "/" || path.Clean(value) != value || value == "/proc" || value == "/sys" || value == "/dev" || strings.HasPrefix(value, "/proc/") || strings.HasPrefix(value, "/sys/") || strings.HasPrefix(value, "/dev/") {
		return errors.New("volume destination must be a clean absolute non-system path")
	}
	return nil
}

func validateResourceLimits(namespaceID string, limits *vmmdv1.ResourceLimits, namespace *vmmdv1.NamespaceLimits) error {
	if limits == nil {
		return nil
	}
	if limits.Pids != nil && (limits.GetPids() == 0 || limits.GetPids() > namespace.GetPids()) {
		return errors.New("container PID limit must be positive and no greater than its namespace")
	}
	return nil
}
