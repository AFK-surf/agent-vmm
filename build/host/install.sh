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
installer_uid=$(id -u)
if [ "$installer_uid" -ne 0 ] && [ "$installer_uid" -ne "$service_uid" ]; then
	echo "a non-root installer can only install for its current user" >&2
	exit 1
fi
service_home=$(dscl . -read "/Users/$service_user" NFSHomeDirectory | awk '{print $2}')
case "$service_home" in /*) ;; *) echo "cannot resolve service user home" >&2; exit 1 ;; esac

effective_service_type=$service_type
if [ "$effective_service_type" = auto ]; then
	if [ "$installer_uid" -eq 0 ]; then effective_service_type=daemon; else effective_service_type=agent; fi
fi
if [ "$effective_service_type" = daemon ] && [ "$installer_uid" -ne 0 ]; then
	echo "daemon installation requires administrator access" >&2
	exit 1
fi

source_app="$payload/Agent VMM Host.app"
test -d "$source_app"
test -x "$source_app/Contents/Helpers/agent-vmm"
test -x "$source_app/Contents/Helpers/agent-vmm-lifecycle"
test -x "$source_app/Contents/Helpers/agent-vmm-service-executor"

app="$service_home/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app"
app_parent=$(dirname "$app")
executor=/Library/PrivilegedHelperTools/agent-vmm-service-executor
trusted_stage=
trusted_app="$source_app"

run_as_service_user() {
	if [ "$installer_uid" -eq 0 ]; then
		/usr/bin/sudo -u "$service_user" -- "$@"
	else
		"$@"
	fi
}

if [ "$installer_uid" -eq 0 ]; then
	install -d -o "$service_user" -g "$service_gid" -m 0755 "$app_parent"
	install -d -o "$service_user" -g "$service_gid" -m 0700 \
		"$service_home/Library/Application Support/AgentVMM" \
		"$service_home/Library/Logs/AgentVMM"
else
	mkdir -p "$app_parent"
fi

if [ "$installer_uid" -eq 0 ]; then
	trusted_stage=$(mktemp -d /var/tmp/agent-vmm-install.XXXXXX)
	chmod 0700 "$trusted_stage"
	chown root:wheel "$trusted_stage"
	trusted_app="$trusted_stage/Agent VMM Host.app"
	ditto "$source_app" "$trusted_app"
	chown -R root:wheel "$trusted_app"
	codesign --verify --deep --strict "$trusted_app"
	target_release_id=$("$trusted_app/Contents/Helpers/agent-vmm-lifecycle" version --json | \
		python3 -c 'import json,sys; value=json.load(sys.stdin); release=value.get("release_id", ""); assert release; print(release)')
else
	codesign --verify --deep --strict "$trusted_app"
	target_release_id=$("$trusted_app/Contents/Helpers/agent-vmm-lifecycle" version --json | \
		python3 -c 'import json,sys; value=json.load(sys.stdin); release=value.get("release_id", ""); assert release; print(release)')
fi
release_key=$(printf '%s' "$target_release_id" | shasum -a 256 | awk '{print $1}')
install_request_id="local-host-install-$release_key"
update_request_id="local-host-update-$release_key"
staged_app="$app_parent/.Agent VMM Host.$release_key.app"
created_staged=0
trap 'if [ "$created_staged" -eq 1 ]; then rm -rf "$staged_app"; fi; if [ -n "$trusted_stage" ]; then rm -rf "$trusted_stage"; fi' EXIT HUP INT TERM
already_current=0
if [ -d "$app" ] && [ ! -e "$staged_app" ]; then
	codesign --verify --deep --strict "$app"
	current_release_id=$(run_as_service_user "$app/Contents/Helpers/agent-vmm-lifecycle" version --json | \
		python3 -c 'import json,sys; value=json.load(sys.stdin); release=value.get("release_id", ""); assert release; print(release)')
	if [ "$current_release_id" = "$target_release_id" ]; then
		already_current=1
	fi
fi
if [ "$already_current" -eq 1 ]; then
	:
elif [ -e "$staged_app" ]; then
	codesign --verify --deep --strict "$staged_app"
	staged_release_id=$("$staged_app/Contents/Helpers/agent-vmm-lifecycle" version --json | \
		python3 -c 'import json,sys; value=json.load(sys.stdin); release=value.get("release_id", ""); assert release; print(release)')
	test "$staged_release_id" = "$target_release_id"
else
	ditto "$trusted_app" "$staged_app"
	created_staged=1
	if [ "$installer_uid" -eq 0 ]; then
		chown -R "$service_uid:$service_gid" "$staged_app"
	fi
	codesign --verify --deep --strict "$staged_app"
fi

if [ "$effective_service_type" = daemon ]; then
	install -d -o root -g wheel -m 0755 /Library/PrivilegedHelperTools
	install -o root -g wheel -m 0755 "$trusted_app/Contents/Helpers/agent-vmm-service-executor" "$executor"
	cmp -s "$trusted_app/Contents/Helpers/agent-vmm-service-executor" "$executor"
	codesign --verify --strict "$executor"
	"$executor" configure --service-user "$service_user"
fi
if [ -n "$trusted_stage" ]; then
	rm -rf "$trusted_stage"
	trusted_stage=
fi

if [ "$already_current" -eq 1 ]; then
	run_as_service_user "$app/Contents/Helpers/agent-vmm-lifecycle" status \
		--service-type "$effective_service_type" --service-user "$service_user"
	trap - EXIT HUP INT TERM
	exit 0
fi

if [ ! -d "$app" ]; then
	mv "$staged_app" "$app"
	created_staged=0
	trap - EXIT HUP INT TERM
	if [ "$effective_service_type" = daemon ]; then
		"$executor" install --job host
	fi
	run_as_service_user "$app/Contents/Helpers/agent-vmm-lifecycle" install \
		--service-type "$effective_service_type" --service-user "$service_user" \
		--request-id "$install_request_id"
	exit 0
fi

lifecycle="$app/Contents/Helpers/agent-vmm-lifecycle"
# From this point the prepared path is owner recovery material. Lifecycle
# removes it only after the selected release completes.
trap - EXIT HUP INT TERM
if [ "$effective_service_type" = agent ]; then
	run_as_service_user "$lifecycle" update --source-app "$staged_app" \
		--target-release-id "$target_release_id" --request-id "$update_request_id" \
		--service-type agent --service-user "$service_user"
	exit 0
fi

# A daemon update is a bounded administrator ceremony. Lifecycle reports the
# exact stage that needs one fixed executor action. The same request then
# resumes from that stage. No old-binary rollback occurs after cutover.
attempt_log=$(mktemp -t agent-vmm-update.XXXXXX)
trap 'rm -f "$attempt_log"' EXIT HUP INT TERM
administrator_actions=0
while [ "$administrator_actions" -lt 3 ]; do
	if run_as_service_user "$lifecycle" update --source-app "$staged_app" \
		--target-release-id "$target_release_id" --request-id "$update_request_id" \
		--service-type daemon --service-user "$service_user" >"$attempt_log" 2>&1; then
		rm -f "$attempt_log"
		trap - EXIT HUP INT TERM
		exit 0
	fi
	grep -q 'update_forward_repair_required' "$attempt_log" || { sed -n '1,20p' "$attempt_log" >&2; exit 1; }
	stage=$(python3 - "$attempt_log" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    for line in handle:
        try:
            value = json.loads(line)
        except Exception:
            continue
        print(value.get("stage", ""))
        break
PY
	)
	case "$stage" in
	stop) "$executor" stop --job host ;;
	recovery) "$executor" install --job host ;;
	*) sed -n '1,20p' "$attempt_log" >&2; exit 1 ;;
	esac
	administrator_actions=$((administrator_actions + 1))
done
sed -n '1,20p' "$attempt_log" >&2
exit 1
