#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
output=${OUTPUT_DIRECTORY:-"$repo/dist"}
identity=${CODE_SIGN_IDENTITY:--}
guest=${AGENT_VMM_GUEST_BUNDLE:-}
mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
product="$output/Agent VMM Host.app"
release_id=${AGENT_VMM_RELEASE_ID:-agent-vmm-host}
phase=${AGENT_VMM_PACKAGE_PHASE:-all}
case "$phase" in
	all|assemble|sign) ;;
	*) echo "AGENT_VMM_PACKAGE_PHASE must be all, assemble, or sign" >&2; exit 1 ;;
esac
case "$release_id" in
	''|latest|*[!A-Za-z0-9._+-]*) echo "AGENT_VMM_RELEASE_ID must be a stable non-latest release id" >&2; exit 1 ;;
esac

host="$product/Contents/MacOS/agent-vmm-host"
helpers="$product/Contents/Helpers"
build_profile=release
if [ "$phase" = all ] && [ "$identity" = "-" ]; then
	build_profile=test
elif [ "$phase" != assemble ]; then
	test "$identity" != "-"
	test -n "${NOTARYTOOL_PROFILE:-}"
fi

if [ "${AGENT_VMM_PACKAGE_E2E:-}" = 1 ]; then
	test "$build_profile" = test
fi

if [ "$phase" != sign ]; then
	test -n "$guest"
	guest=$(CDPATH= cd -- "$guest" && pwd)
	for artifact in Image initramfs.xz rootfs.squashfs manifest.json THIRD_PARTY_NOTICES; do
		test -s "$guest/$artifact"
	done

	export AGENT_VMM_RELEASE_ID="$release_id"
	AGENT_VMM_GUEST_MANIFEST_DIGEST=$(shasum -a 256 "$guest/manifest.json" | awk '{print $1}')
	export AGENT_VMM_GUEST_MANIFEST_DIGEST
	rust_target=${CARGO_TARGET_DIR:-"$repo/target/host-rust"}
	mkdir -p "$rust_target"
	CARGO_TARGET_DIR=$(CDPATH= cd -- "$rust_target" && pwd)
	export CARGO_TARGET_DIR
	PROTOC=$(sh "$repo/build/protoc.sh" "$CARGO_TARGET_DIR")
	export PROTOC
	target=aarch64-apple-darwin
	(
		cd "$repo"
		cargo build --locked --release --target "$target" \
			-p agent-vmm-host -p agent-vmm-carrier -p agent-vmm-lifecycle \
			-p agent-vmm -p agent-vmm-data
		cargo rustc --locked --release --target "$target" -p agent-vmm-service-executor -- \
			-C link-arg=-Wl,-dead_strip_dylibs
		if [ "${AGENT_VMM_PACKAGE_E2E:-}" = 1 ]; then
			cargo build --locked --release --target "$target" -p agent-vmm-lifecycle --features e2e-isolation
		fi
	)

	rm -rf "$product"
	resources="$product/Contents/Resources"
	mkdir -p "$product/Contents/MacOS" "$helpers" "$resources/Guest"
	cp "$repo/build/host/Info.plist" "$product/Contents/Info.plist"
	for artifact in Image initramfs.xz rootfs.squashfs manifest.json THIRD_PARTY_NOTICES; do
		cp "$guest/$artifact" "$resources/Guest/$artifact"
	done
	cp "$CARGO_TARGET_DIR/$target/release/agent-vmm-host" "$host"
	for helper in agent-vmm-carrier agent-vmm-lifecycle agent-vmm agent-vmm-service-executor agent-vmm-data; do
		cp "$CARGO_TARGET_DIR/$target/release/$helper" "$helpers/$helper"
	done
	for binary in "$host" "$helpers/agent-vmm-lifecycle"; do
		report=$("$binary" version --json)
		printf '%s\n' "$report" |
			jq -e --arg release_id "$release_id" '.version == $release_id and .release_id == $release_id' >/dev/null
	done
	for command in inspect diagnose logs operation repair update vm cleanup; do
		"$helpers/agent-vmm" "$command" --help >/dev/null
	done
	appliance_help=$("$helpers/agent-vmm" appliance --help)
	for command in install update status recover stop start activate restart revoke remove; do
		printf '%s\n' "$appliance_help" | grep -qw "$command"
	done
	if "$helpers/agent-vmm" appliance build --help >/dev/null 2>&1; then
		echo "managed Agent VMM CLI exposed development appliance build" >&2
		exit 1
	else
		test "$?" -eq 2
	fi
	cp "$repo/LICENSE" "$repo/NOTICE" "$resources/"
	set -- -o "$resources/THIRD_PARTY_NOTICES" -licenses "$repo/third_party/licenses" -cargo-target "$target"
	for crate in bins/agent-vmm-host bins/agent-vmm-carrier bins/agent-vmm-lifecycle bins/agent-vmm bins/agent-vmm-service-executor data/agent-vmm-data; do
		set -- "$@" -cargo-manifest "$repo/$crate/Cargo.toml"
	done
	(cd "$repo" && cargo run --locked -p xtask -- notices "$@")
	jq -n --arg release_id "$release_id" \
		'{schema_version: 1, component: "agent-vmm-host-runtime", release_id: $release_id}' \
		> "$resources/release-inventory.json"
fi

test -x "$host"
for helper in agent-vmm-carrier agent-vmm-lifecycle agent-vmm agent-vmm-service-executor agent-vmm-data; do
	test -x "$helpers/$helper"
done
executor_links=$(otool -L "$helpers/agent-vmm-service-executor")
if [ "$(printf '%s\n' "$executor_links" | awk 'NR > 1 {print $1}')" != /usr/lib/libSystem.B.dylib ]; then
	printf 'Unexpected service executor dependencies:\n%s\n' "$executor_links" >&2
	exit 1
fi

/usr/bin/plutil -lint "$product/Contents/Info.plist"
test "$(/usr/bin/plutil -extract CFBundleExecutable raw -o - "$product/Contents/Info.plist")" = agent-vmm-host
test "$(/usr/bin/plutil -extract CFBundleIdentifier raw -o - "$product/Contents/Info.plist")" = com.afk-surf.agent-vmm-host
test "$(/usr/bin/plutil -extract LSBackgroundOnly raw -o - "$product/Contents/Info.plist")" = true
test ! -e "$product/Contents/MacOS/AgentVMM"
test ! -e "$product/Contents/MacOS/agent-vmm"
test ! -e "$product/Contents/Helpers/agent-vmm-host"
test ! -e "$product/Contents/Resources/SwiftUI"
if [ "$phase" = assemble ]; then
	exit 0
fi

profile=${AGENT_VMM_PROVISIONING_PROFILE:-}
rm -f "$product/Contents/embedded.provisionprofile"
set --
if [ "$build_profile" = release ] && [ -n "$profile" ]; then
	test -s "$profile"
	cp "$profile" "$product/Contents/embedded.provisionprofile"
	set -- --entitlements "$repo/build/host/keychain.entitlements.plist"
fi
codesign --force --sign "$identity" --options runtime --entitlements "$repo/build/host/vz.entitlements.plist" "$helpers/agent-vmm-carrier"
for helper in agent-vmm-lifecycle agent-vmm-data agent-vmm agent-vmm-service-executor; do
	codesign --force --sign "$identity" --options runtime "$helpers/$helper"
done
codesign --force --sign "$identity" --options runtime "$@" "$product"
codesign --verify --deep --strict --verbose=2 "$product"

carrier_entitlements="$output/agent-vmm-carrier-entitlements.plist"
codesign -d --xml --entitlements "$carrier_entitlements" "$helpers/agent-vmm-carrier"
plutil -convert json -o - "$carrier_entitlements" |
	jq -e '.["com.apple.security.virtualization"] == true and (has("keychain-access-groups") | not)' >/dev/null

host_entitlements="$output/agent-vmm-host-entitlements.plist"
rm -f "$host_entitlements"
codesign -d --xml --entitlements "$host_entitlements" "$host"
if [ "$#" -ne 0 ]; then
	plutil -convert json -o - "$host_entitlements" |
		jq -e '(has("com.apple.security.virtualization") | not) and
			.["keychain-access-groups"] == ["D9AAN3VJK8.com.afk-surf.agent-vmm-host"] and
			.["com.apple.application-identifier"] == "D9AAN3VJK8.com.afk-surf.agent-vmm-host" and
			.["com.apple.developer.team-identifier"] == "D9AAN3VJK8"' >/dev/null
elif [ -s "$host_entitlements" ]; then
	plutil -convert json -o - "$host_entitlements" | jq -e '. == {}' >/dev/null
fi
if [ "$build_profile" = test ]; then
	codesign -dv --verbose=4 "$host" 2>&1 | grep -q 'Signature=adhoc'
else
	codesign -dv --verbose=4 "$host" 2>&1 | grep -q 'Authority=Developer ID Application'
fi

if [ -n "${NOTARYTOOL_PROFILE:-}" ]; then
  ditto -c -k --norsrc --noextattr --keepParent "$product" "$output/Agent-VMM.zip"
  xcrun notarytool submit "$output/Agent-VMM.zip" --keychain-profile "$NOTARYTOOL_PROFILE" --wait
  xcrun stapler staple "$product"
fi

ditto -c -k --norsrc --noextattr --keepParent "$product" "$output/Agent-VMM-Host.zip"
