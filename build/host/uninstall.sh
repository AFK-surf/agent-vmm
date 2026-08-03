#!/bin/sh
set -eu

user_home=$(dscl . -read "/Users/$(id -un)" NFSHomeDirectory | awk '{print $2}')
case "$user_home" in
  /*) ;;
  *) echo "cannot resolve the user home directory" >&2; exit 1 ;;
esac
test "$user_home" != "/"
app="$user_home/Applications/Agent VMM.app"
plist="$user_home/Library/LaunchAgents/com.afk-surf.agent-vmm.plist"
domain="gui/$(id -u)"

launchctl bootout "$domain/com.afk-surf.agent-vmm" 2>/dev/null || true
rm -f "$plist"
rm -rf "$app"

if [ "${1:-}" = "--purge" ]; then
	rm -rf "$user_home/Library/Application Support/AgentVMM" "$user_home/Library/Logs/AgentVMM"
fi
