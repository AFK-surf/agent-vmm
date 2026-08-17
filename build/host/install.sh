#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
payload=${OUTPUT_DIRECTORY:-"$repo/dist"}
app="$HOME/Applications/Agent VMM.app"
launch_agents="$HOME/Library/LaunchAgents"
plist="$launch_agents/com.afk-surf.agent-vmm.plist"
domain="gui/$(id -u)"

test -d "$payload/Agent VMM.app"
test -f "$payload/com.afk-surf.agent-vmm.plist"
mkdir -p "$HOME/Applications" "$launch_agents" "$HOME/Library/Application Support/AgentVMM" "$HOME/Library/Logs/AgentVMM"
launchctl bootout "$domain/com.afk-surf.agent-vmm" 2>/dev/null || true
launchctl bootout "$domain/com.afk-surf.agent-vmm-connect" 2>/dev/null || true
rm -f "$launch_agents/com.afk-surf.agent-vmm-connect.plist"
attempt=0
while launchctl print "$domain/com.afk-surf.agent-vmm" >/dev/null 2>&1; do
	attempt=$((attempt + 1))
	if [ "$attempt" -ge 50 ]; then
		echo "timed out waiting for the previous LaunchAgent to stop" >&2
		exit 1
	fi
	sleep 0.1
done
rm -rf "$app"
ditto "$payload/Agent VMM.app" "$app"
cp "$payload/com.afk-surf.agent-vmm.plist" "$plist"
launchctl bootstrap "$domain" "$plist"
launchctl enable "$domain/com.afk-surf.agent-vmm"
