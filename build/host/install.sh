#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
payload=${OUTPUT_DIRECTORY:-"$repo/dist"}
service_type=auto
service_user=$(id -un)
while [ "$#" -gt 0 ]; do
	case "$1" in
		--service-type) service_type=${2:-}; shift 2 ;;
		--service-user) service_user=${2:-}; shift 2 ;;
		*) echo "usage: install.sh [--service-type agent|daemon|auto] [--service-user USER]" >&2; exit 1 ;;
	esac
done
case "$service_type" in agent|daemon|auto) ;; *) echo "invalid service type" >&2; exit 1 ;; esac
service_uid=$(id -u "$service_user")
service_gid=$(id -g "$service_user")
test "$service_uid" -ne 0
if [ "$(id -u)" -ne 0 ] && [ "$(id -u)" -ne "$service_uid" ]; then
	echo "a non-root installer can only install for its current user" >&2
	exit 1
fi
service_home=$(dscl . -read "/Users/$service_user" NFSHomeDirectory | awk '{print $2}')
case "$service_home" in /*) ;; *) echo "cannot resolve service user home" >&2; exit 1 ;; esac
app="$service_home/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app"
operation_suffix="$$-$(date +%s)"
drain_request_id="local-host-update-drain-$operation_suffix"
install_request_id="local-host-install-$operation_suffix"

test -d "$payload/Agent VMM Host.app"
mkdir -p "$(dirname "$app")"
if [ -d "$app" ]; then
	if [ "$(id -u)" -eq 0 ]; then
		"$payload/Agent VMM Host.app/Contents/Helpers/agent-vmm-lifecycle" drain \
			--service-app "$app" --service-type "$service_type" --service-user "$service_user" \
			--request-id "$drain_request_id"
	else
		"$payload/Agent VMM Host.app/Contents/Helpers/agent-vmm-lifecycle" drain \
			--service-type "$service_type" --service-user "$service_user" \
			--request-id "$drain_request_id"
	fi
fi
staged_app="$(dirname "$app")/.Agent VMM Host.$$.app"
trap 'rm -rf "$staged_app"' EXIT HUP INT TERM
ditto "$payload/Agent VMM Host.app" "$staged_app"
"$payload/Agent VMM Host.app/Contents/Helpers/agent-vmm-lifecycle" atomic-replace-app \
	--source-app "$staged_app" --target-app "$app"
# After the swap, staged_app is the complete previous release. Retain it if
# bootstrap or health verification fails so an operator can recover it.
trap - EXIT HUP INT TERM
if [ "$(id -u)" -eq 0 ]; then
	"$payload/Agent VMM Host.app/Contents/Helpers/agent-vmm-lifecycle" install \
		--service-app "$app" --service-type "$service_type" --service-user "$service_user" \
		--request-id "$install_request_id"
	for path in "$service_home/Library/Application Support/AgentVMM" "$service_home/Library/Logs/AgentVMM"; do
		if [ -e "$path" ]; then chown -R "$service_uid:$service_gid" "$path"; fi
	done
	chown -R "$service_uid:$service_gid" "$(dirname "$(dirname "$app")")"
else
	"$app/Contents/Helpers/agent-vmm-lifecycle" install \
		--service-type "$service_type" --service-user "$service_user" \
		--request-id "$install_request_id"
fi
rm -rf "$staged_app"
