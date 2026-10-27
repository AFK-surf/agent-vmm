package launchdservice

import (
	_ "embed"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

//go:embed com.afk-surf.agent-vmm-host.plist.template
var hostTemplate string

type HostParams struct {
	Home, App, Label, User, Group string
	Daemon, DisablePairing        bool
}

func RenderHostPlist(params HostParams) []byte {
	label := params.Label
	if label == "" {
		label = bundle.HostBundleIdentifier
	}
	logs := filepath.Join(params.Home, "Library", "Logs", "AgentVMM")
	if params.Daemon {
		logs = "/Library/Logs/AgentVMM"
	}
	rendered := strings.NewReplacer("__LABEL__", escape(label), "__HOME__", escape(params.Home), "__APP__", escape(params.App), "__LOGS__", escape(logs)).Replace(hostTemplate)
	if params.DisablePairing {
		rendered = strings.Replace(rendered, "  </array>", "    <string>--disable-personal-mesh-pairing</string>\n  </array>", 1)
	}
	if params.Daemon {
		rendered = strings.Replace(rendered, "<string>Interactive</string>", "<string>Background</string>", 1)
		identity := fmt.Sprintf("  <key>UserName</key>\n  <string>%s</string>\n  <key>GroupName</key>\n  <string>%s</string>\n", escape(params.User), escape(params.Group))
		rendered = strings.Replace(rendered, "</dict>\n</plist>", identity+"</dict>\n</plist>", 1)
	}
	return []byte(rendered)
}

func escape(value string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;").Replace(value)
}
