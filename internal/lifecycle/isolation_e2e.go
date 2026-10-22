//go:build darwin && e2e

package lifecycle

import (
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

// The installation-chain E2E runs the real helper against launchd. This seam
// moves the launchd label and the service home of that helper away from the
// production Host. It exists only in builds with the e2e tag.
var hostLabel, e2eHome = e2eIsolation()

var e2eLabelPattern = regexp.MustCompile(`\A[A-Za-z0-9.-]{1,128}\z`)

func e2eIsolation() (string, string) {
	label, home := os.Getenv("AGENT_VMM_E2E_HOST_LABEL"), os.Getenv("AGENT_VMM_E2E_HOME")
	if len(os.Args) > 1 && os.Args[1] == "version" {
		return label, home
	}
	if !e2eLabelPattern.MatchString(label) || label == productionHostLabel {
		fatal("e2e lifecycle requires AGENT_VMM_E2E_HOST_LABEL other than the production label")
	}
	current, err := user.Current()
	canonical, canonicalErr := filepath.EvalSymlinks(home)
	if err != nil || canonicalErr != nil || !filepath.IsAbs(home) || canonical != home || home == current.HomeDir {
		fatal("e2e lifecycle requires a canonical AGENT_VMM_E2E_HOME other than the user's home")
	}
	return label, home
}

func serviceAccount(account *user.User) *user.User {
	isolated := *account
	isolated.HomeDir = e2eHome
	return &isolated
}

func supportedSchema() store.SchemaRange {
	schema := store.SupportedSchema()
	if value := os.Getenv("AGENT_VMM_E2E_SUPPORTED_SCHEMA_MAX"); value != "" {
		maximum, err := strconv.Atoi(value)
		if err != nil {
			fatal("AGENT_VMM_E2E_SUPPORTED_SCHEMA_MAX must be an integer")
		}
		schema.Max = maximum
	}
	return schema
}
