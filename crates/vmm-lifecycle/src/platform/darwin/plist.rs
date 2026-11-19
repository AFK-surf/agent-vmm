//! The launchd definitions of the Host and of the carrier.

use std::path::Path;

const HOST_TEMPLATE: &str = include_str!("../../../templates/host.plist");
const CARRIER_TEMPLATE: &str = include_str!("../../../templates/carrier.plist");

pub struct Params<'a> {
    pub label: &'a str,
    pub home: &'a Path,
    pub app: &'a Path,
    pub user: &'a str,
    pub group: &'a str,
    pub daemon: bool,
    pub disable_pairing: bool,
}

fn escape(value: &str) -> String {
    value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&apos;")
}

fn render(template: &str, params: &Params<'_>) -> String {
    let logs = if params.daemon { "/Library/Logs/AgentVMM".to_string() } else { params.home.join("Library/Logs/AgentVMM").display().to_string() };
    let mut rendered = template
        .replace("__LABEL__", &escape(params.label))
        .replace("__HOME__", &escape(&params.home.display().to_string()))
        .replace("__APP__", &escape(&params.app.display().to_string()))
        .replace("__LOGS__", &escape(&logs));
    if params.daemon {
        // A system job has no login session. It runs as the service user, never as root.
        rendered = rendered.replacen("<string>Interactive</string>", "<string>Background</string>", 1);
        let identity = format!("  <key>UserName</key>\n  <string>{}</string>\n  <key>GroupName</key>\n  <string>{}</string>\n", escape(params.user), escape(params.group));
        rendered = rendered.replacen("</dict>\n</plist>", &format!("{identity}</dict>\n</plist>"), 1);
    }
    rendered
}

pub fn host(params: &Params<'_>) -> String {
    let mut rendered = render(HOST_TEMPLATE, params);
    if params.label != crate::HOST_BUNDLE_IDENTIFIER {
        rendered = rendered.replacen("  </array>", "    <string>--mcp-port</string>\n    <string>0</string>\n  </array>", 1);
    }
    if params.disable_pairing {
        return rendered.replacen("  </array>", "    <string>--disable-personal-mesh-pairing</string>\n  </array>", 1);
    }
    rendered
}

pub fn carrier(params: &Params<'_>) -> String {
    render(CARRIER_TEMPLATE, params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_daemon_definition_names_the_service_user_and_escapes_values() {
        let params = Params { label: "com.afk-surf.agent-vmm-host", home: Path::new("/Users/a&b"), app: Path::new("/Users/a&b/Agent VMM Host.app"), user: "a<b", group: "staff", daemon: true, disable_pairing: true };
        let rendered = host(&params);
        assert!(rendered.contains("<string>/Users/a&amp;b/Agent VMM Host.app/Contents/MacOS/agent-vmm-host</string>"));
        assert!(rendered.contains("<string>--disable-personal-mesh-pairing</string>\n  </array>"));
        assert!(rendered.contains("<key>UserName</key>\n  <string>a&lt;b</string>\n  <key>GroupName</key>\n  <string>staff</string>\n</dict>\n</plist>"));
        assert!(rendered.contains("<string>Background</string>") && rendered.contains("<string>/Library/Logs/AgentVMM/host.stderr.log</string>"));
        assert!(!rendered.contains("__"));

        assert!(!rendered.contains("--mcp-port"));
        let isolated = host(&Params { label: "test.agent-vmm-host", ..params });
        let args = isolated.split_once("<key>ProgramArguments</key>").unwrap().1.split_once("</array>").unwrap().0;
        assert!(args.contains("<string>--mcp-port</string>\n    <string>0</string>"));
        assert!(args.contains("<string>--disable-personal-mesh-pairing</string>"));
        let agent = carrier(&Params { daemon: false, disable_pairing: false, ..params });
        assert!(agent.contains("/Contents/Helpers/agent-vmm-carrier</string>") && agent.contains("<string>Interactive</string>"));
        assert!(agent.contains("<string>/Users/a&amp;b/Library/Logs/AgentVMM/carrier.stdout.log</string>") && !agent.contains("UserName"));
    }
}
