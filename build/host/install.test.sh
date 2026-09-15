#!/usr/bin/env bash
set -euo pipefail

repo="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/agent-vmm-install-test.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
service_user="$(id -un)"
service_home="$scratch/home"
payload="$scratch/payload"
source_app="$payload/Agent VMM Host.app"
mkdir -p "$scratch/bin" "$source_app/Contents/Helpers" "$source_app/Contents/MacOS"

cat > "$scratch/bin/dscl" <<'EOF'
#!/bin/sh
printf 'NFSHomeDirectory: %s\n' "$TEST_SERVICE_HOME"
EOF
cat > "$scratch/bin/codesign" <<'EOF'
#!/bin/sh
exit 0
EOF
cat > "$scratch/bin/ditto" <<'EOF'
#!/bin/sh
cp -R "$1" "$2"
EOF
chmod +x "$scratch/bin/dscl" "$scratch/bin/codesign" "$scratch/bin/ditto"

for helper in agent-vmm agent-vmm-service-executor; do
	printf '#!/bin/sh\nexit 0\n' > "$source_app/Contents/Helpers/$helper"
	chmod +x "$source_app/Contents/Helpers/$helper"
done
cat > "$source_app/Contents/Helpers/agent-vmm-lifecycle" <<'EOF'
#!/bin/sh
if [ "${1:-}" = version ]; then
	case "$0" in
	*'/current/Agent VMM Host.app/'*)
		if [ -e "$TEST_OLD_CURRENT" ]; then
			printf '%s\n' '{"release_id":"release-old"}'
			exit 0
		fi
		;;
	esac
	printf '%s\n' '{"release_id":"release-stable"}'
	exit 0
fi
printf '%s\n' "$*" >> "$TEST_LIFECYCLE_LOG"
if [ "${1:-}" = install ] || [ "${1:-}" = status ]; then
	exit 0
fi
count=$(cat "$TEST_ATTEMPT_COUNT" 2>/dev/null || printf 0)
count=$((count + 1))
printf '%s' "$count" > "$TEST_ATTEMPT_COUNT"
test "$count" -gt 1
EOF
printf '#!/bin/sh\nexit 0\n' > "$source_app/Contents/MacOS/agent-vmm-host"
chmod +x "$source_app/Contents/Helpers/agent-vmm-lifecycle" "$source_app/Contents/MacOS/agent-vmm-host"

run_installer() {
	TEST_SERVICE_HOME="$service_home" TEST_LIFECYCLE_LOG="$scratch/lifecycle.log" TEST_ATTEMPT_COUNT="$scratch/attempt-count" \
		TEST_OLD_CURRENT="$scratch/current-is-old" \
		OUTPUT_DIRECTORY="$payload" PATH="$scratch/bin:$PATH" \
		"$repo/build/host/install.sh" --service-type agent --service-user "$service_user"
}

run_installer
run_installer
if find "$service_home/Library/Application Support/Agent VMM Host/current" -maxdepth 1 -name '.Agent VMM Host.*.app' | grep -q .; then
	echo "same-release rerun created false recovery material" >&2
	exit 1
fi
touch "$scratch/current-is-old"
if run_installer >/dev/null 2>&1; then
	echo "first interrupted update unexpectedly succeeded" >&2
	exit 1
fi
run_installer

test "$(wc -l < "$scratch/lifecycle.log" | tr -d '[:space:]')" -eq 4
install_call=$(sed -n '1p' "$scratch/lifecycle.log")
status_call=$(sed -n '2p' "$scratch/lifecycle.log")
first_update=$(sed -n '3p' "$scratch/lifecycle.log")
second_update=$(sed -n '4p' "$scratch/lifecycle.log")
test "$first_update" = "$second_update"
release_key=$(printf '%s' release-stable | shasum -a 256 | awk '{print $1}')
test "$install_call" = "install --service-type agent --service-user $service_user --request-id local-host-install-$release_key"
test "$status_call" = "status --service-type agent --service-user $service_user"
case "$first_update" in
"update --source-app $service_home/Library/Application Support/Agent VMM Host/current/.Agent VMM Host.$release_key.app --target-release-id release-stable --request-id local-host-update-$release_key --service-type agent --service-user $service_user") ;;
*) echo "update did not resume the release-derived request: $first_update" >&2; exit 1 ;;
esac

echo "Agent VMM installer resume contract passed"
