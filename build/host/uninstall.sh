#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
payload=${OUTPUT_DIRECTORY:-"$repo/dist"}
service_type=auto
service_user=$(id -un)
purge=
while [ "$#" -gt 0 ]; do
	case "$1" in
		--service-type) service_type=${2:-}; shift 2 ;;
		--service-user) service_user=${2:-}; shift 2 ;;
		--purge) purge=--purge; shift ;;
		*) echo "usage: uninstall.sh [--service-type agent|daemon|auto] [--service-user USER] [--purge]" >&2; exit 1 ;;
	esac
done
user_home=$(dscl . -read "/Users/$service_user" NFSHomeDirectory | awk '{print $2}')
case "$user_home" in
  /*) ;;
  *) echo "cannot resolve the user home directory" >&2; exit 1 ;;
esac
test "$user_home" != "/"
app="$user_home/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app"
helper="$app/Contents/Helpers/agent-vmm-lifecycle"
test -x "$helper"
if [ "$(id -u)" -eq 0 ]; then
	trusted_helper="$payload/Agent VMM Host.app/Contents/Helpers/agent-vmm-lifecycle"
	test -x "$trusted_helper"
	"$trusted_helper" uninstall --service-app "$app" --service-type "$service_type" --service-user "$service_user" $purge --request-id local-host-uninstall
else
	"$helper" uninstall --service-type "$service_type" --service-user "$service_user" $purge --request-id local-host-uninstall
fi
