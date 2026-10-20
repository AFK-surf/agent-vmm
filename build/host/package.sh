#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
output=${OUTPUT_DIRECTORY:-"$repo/dist"}
identity=${CODE_SIGN_IDENTITY:--}
guest=${AGENT_VMM_GUEST_BUNDLE:-/tmp/agent-vmm-e2e-bundle}
rust_target=${CARGO_TARGET_DIR:-"$repo/target/host-rust"}
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

go_tags=
build_profile=release
if [ "$phase" = sign ]; then
	test "$identity" != "-"
	test -n "${NOTARYTOOL_PROFILE:-}"
	test -x "$product/Contents/MacOS/agent-vmm-host"
	test -x "$product/Contents/Helpers/agent-vmm"
	test -x "$product/Contents/Helpers/agent-vmm-lifecycle"
	test -x "$product/Contents/Helpers/agent-vmm-data"
	test -x "$product/Contents/Helpers/agent-vmm-service-executor"
	host="$product/Contents/MacOS/agent-vmm-host"
else
test -d "$guest"
for artifact in Image initramfs.xz rootfs.squashfs manifest.json; do
	test -s "$guest/$artifact"
done
has_mcp_digest=$(jq -r 'has("artifacts") and (.artifacts | has("mcp_appliance_digest"))' "$guest/manifest.json")
has_mcp_manifest=$(jq -r 'has("artifacts") and (.artifacts | has("mcp_appliance_manifest"))' "$guest/manifest.json")
test "$has_mcp_digest" = "$has_mcp_manifest"
if [ "$has_mcp_digest" = true ]; then
	test -s "$guest/mcp-appliance.digest"
	test -s "$guest/mcp-appliance/manifest.pb"
fi
guest_manifest_digest=$(shasum -a 256 "$guest/manifest.json" | awk '{print $1}')
if [ "$phase" = all ] && [ "$identity" = "-" ]; then
	build_profile=test
	go_tags=development
elif [ "$phase" != assemble ]; then
	test "$identity" != "-"
	test -n "${NOTARYTOOL_PROFILE:-}"
fi
mkdir -p "$output"
rm -rf "$product"
mkdir -p "$product/Contents/MacOS" "$product/Contents/Helpers" "$product/Contents/Resources/Guest"
cp "$repo/build/host/Info.plist" "$product/Contents/Info.plist"
for artifact in Image initramfs.xz rootfs.squashfs manifest.json; do
	cp "$guest/$artifact" "$product/Contents/Resources/Guest/$artifact"
done
if [ "$has_mcp_digest" = true ]; then
	cp "$guest/mcp-appliance.digest" "$product/Contents/Resources/Guest/mcp-appliance.digest"
	mkdir -p "$product/Contents/Resources/Guest/mcp-appliance"
	cp "$guest/mcp-appliance/manifest.pb" "$product/Contents/Resources/Guest/mcp-appliance/manifest.pb"
fi
if [ -n "$go_tags" ]; then
	GOOS=darwin GOARCH=arm64 go build -tags="$go_tags" -trimpath -ldflags "-X main.guestManifestDigest=$guest_manifest_digest" -o "$product/Contents/MacOS/agent-vmm-host" "$repo/cmd/agent-vmm-host"
else
	GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "-X main.guestManifestDigest=$guest_manifest_digest" -o "$product/Contents/MacOS/agent-vmm-host" "$repo/cmd/agent-vmm-host"
fi
host="$product/Contents/MacOS/agent-vmm-host"
host_info="$product/Contents/Resources/host-build-info.json"
host_symbols="$output/agent-vmm-host-symbols.txt"
host_undefined_symbols="$output/agent-vmm-host-undefined-symbols.txt"
go version -m -json "$host" > "$host_info"
go tool nm "$host" > "$host_symbols"
nm -u "$host" > "$host_undefined_symbols"
jq -e '
  .Path == "github.com/AFK-surf/agent-vmm/cmd/agent-vmm-host" and
  ((.Settings | map({key:.Key, value:.Value}) | from_entries) as $settings |
    $settings.GOOS == "darwin" and $settings.GOARCH == "arm64" and $settings.CGO_ENABLED == "1")
' "$host_info" >/dev/null
case "$build_profile" in
	test)
		jq -e '(.Settings | map({key:.Key, value:.Value}) | from_entries) as $settings |
			(($settings["-tags"] // "") | test("(^|[, ])development([, ]|$)")) and
			(($settings["-tags"] // "") | test("(^|[, ])e2e([, ]|$)") | not)' "$host_info" >/dev/null
		grep -q 'fileApplianceSecrets' "$host_symbols"
		if grep -Eq '_Sec(Item|Key)' "$host_undefined_symbols"; then
			echo "development host links a login-context Security API" >&2
			exit 1
		fi
		;;
	release)
		jq -e '(.Settings | map({key:.Key, value:.Value}) | from_entries) as $settings |
			(($settings["-tags"] // "") | test("(^|[, ])(development|e2e)([, ]|$)") | not) and
			$settings["vcs.modified"] == "false" and ($settings["vcs.revision"] // "") != ""' "$host_info" >/dev/null
		grep -q 'fileApplianceSecrets' "$host_symbols"
		if grep -Eq '_Sec(Item|Key)|NewDevelopmentFileProvider|developmentFileProvider' "$host_undefined_symbols" "$host_symbols"; then
			echo "release host contains a login-context Security provider or development identity provider" >&2
			exit 1
		fi
		;;
esac
GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "-X main.releaseID=$release_id" -o "$product/Contents/Helpers/agent-vmm-lifecycle" "$repo/cmd/agent-vmm-lifecycle"
printf '%s\n' "$("$product/Contents/Helpers/agent-vmm-lifecycle" version --json)" |
	jq -e --arg release_id "$release_id" '.version == $release_id and .release_id == $release_id' >/dev/null
GOOS=darwin GOARCH=arm64 go build -tags=managedonly -trimpath -o "$product/Contents/Helpers/agent-vmm" "$repo/cmd/agent-vmm"
for command in inspect diagnose logs operation repair update vm cleanup; do
	"$product/Contents/Helpers/agent-vmm" "$command" --help >/dev/null
done
for command in install update status recover stop start activate restart revoke remove; do
 "$product/Contents/Helpers/agent-vmm" appliance "$command" --help >/dev/null 2>&1
done
if "$product/Contents/Helpers/agent-vmm" appliance build --help >/dev/null 2>&1; then
	echo "managed Agent VMM CLI exposed development appliance build" >&2
	exit 1
else
	test "$?" -eq 2
fi
GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/Helpers/agent-vmm-service-executor" "$repo/cmd/agent-vmm-service-executor"
CARGO_TARGET_DIR="$rust_target" cargo build --locked --release --target aarch64-apple-darwin --manifest-path "$repo/data/agent-vmm-data/Cargo.toml"
cp "$rust_target/aarch64-apple-darwin/release/agent-vmm-data" "$product/Contents/Helpers/agent-vmm-data"
cp "$repo/LICENSE" "$product/Contents/Resources/LICENSE"
cp "$repo/build/host/com.afk-surf.agent-vmm-host.plist.template" "$product/Contents/Resources/com.afk-surf.agent-vmm-host.plist.template"
source_revision=$(jq -r '(.Settings | map({key:.Key, value:.Value}) | from_entries) | (."vcs.revision" // "")' "$host_info")
jq -n \
	--arg component agent-vmm-host-runtime \
	--arg product "Agent VMM Host" \
	--arg bundle_name "Agent VMM Host.app" \
	--arg bundle_identifier com.afk-surf.agent-vmm-host \
	--arg source_revision "$source_revision" \
	--arg release_id "$release_id" \
	'{
		schema_version: 1,
		component: $component,
		product: $product,
		bundle_name: $bundle_name,
		bundle_identifier: $bundle_identifier,
		layout: {
			main_executable: "Contents/MacOS/agent-vmm-host",
			helpers: ["Contents/Helpers/agent-vmm", "Contents/Helpers/agent-vmm-data", "Contents/Helpers/agent-vmm-lifecycle", "Contents/Helpers/agent-vmm-service-executor"],
			guest: "Contents/Resources/Guest",
			launchd_template: "Contents/Resources/com.afk-surf.agent-vmm-host.plist.template",
			build_info: "Contents/Resources/host-build-info.json",
			license: "Contents/Resources/LICENSE",
			inventory: "Contents/Resources/release-inventory.json"
		},
		release_id: $release_id,
		source_revision: $source_revision
	}' > "$product/Contents/Resources/release-inventory.json"

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
codesign --force --sign "$identity" --options runtime --entitlements "$repo/build/host/vz.entitlements.plist" "$product/Contents/MacOS/agent-vmm-host"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-lifecycle"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-data"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-service-executor"
codesign --force --sign "$identity" --options runtime --preserve-metadata=entitlements "$product"
codesign --verify --deep --strict --verbose=2 "$product"
host_entitlements="$output/agent-vmm-host-entitlements.plist"
codesign -d --xml --entitlements "$host_entitlements" "$host" 2>/dev/null
plutil -convert json -o - "$host_entitlements" |
	jq -e '.["com.apple.security.virtualization"] == true and has("keychain-access-groups") == false' >/dev/null
if [ "$build_profile" = test ]; then
	codesign -dv --verbose=4 "$host" 2>&1 | grep -q 'Signature=adhoc'
else
	codesign -dv --verbose=4 "$host" 2>&1 | grep -q 'Authority=Developer ID Application'
fi

app="$HOME/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app"
sed -e "s|__HOME__|$HOME|g" -e "s|__APP__|$app|g" "$repo/build/host/com.afk-surf.agent-vmm-host.plist.template" > "$output/com.afk-surf.agent-vmm-host.plist"
plutil -lint "$output/com.afk-surf.agent-vmm-host.plist"

if [ -n "${NOTARYTOOL_PROFILE:-}" ]; then
  ditto -c -k --keepParent "$product" "$output/Agent-VMM.zip"
  xcrun notarytool submit "$output/Agent-VMM.zip" --keychain-profile "$NOTARYTOOL_PROFILE" --wait
  xcrun stapler staple "$product"
fi

ditto -c -k --keepParent "$product" "$output/Agent-VMM-Host.zip"
