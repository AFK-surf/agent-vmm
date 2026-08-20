#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
output=${OUTPUT_DIRECTORY:-"$repo/dist"}
identity=${CODE_SIGN_IDENTITY:--}
guest=${AGENT_VMM_GUEST_BUNDLE:-/tmp/agent-vmm-e2e-bundle}
derived="$output/DerivedData"
product="$output/Agent VMM Host.app"
target_revision=${AGENT_VMM_TARGET_REVISION:-1}
release_id=${AGENT_VMM_RELEASE_ID:-agent-vmm-host-r${target_revision}}

case "$target_revision" in
	''|*[!0-9]*) echo "AGENT_VMM_TARGET_REVISION must be a positive integer" >&2; exit 1 ;;
esac
test "$target_revision" -gt 0
case "$release_id" in
	''|latest|*[!A-Za-z0-9._+-]*) echo "AGENT_VMM_RELEASE_ID must be a stable non-latest release id" >&2; exit 1 ;;
esac

test -d "$guest"
for artifact in Image initramfs.xz rootfs.squashfs manifest.json manifest.json.sig SHA256SUMS sbom.spdx.json provenance.json; do
	test -s "$guest/$artifact"
done
for artifact in mcp-appliance.digest mcp-appliance/manifest.pb mcp-appliance/manifest.sig mcp-appliance/publisher.pub; do
	test -s "$guest/$artifact"
done
(cd "$guest" && shasum -a 256 -c SHA256SUMS)
guest_signer=$(/usr/bin/plutil -extract signer raw -o - "$guest/manifest.json")
go_tags=
case "$guest_signer" in
	development)
		if [ "$identity" != "-" ]; then
			echo "development guest bundles require ad-hoc signing" >&2
			exit 1
		fi
		go_tags=development
		;;
	release) ;;
	*)
		echo "unsupported guest bundle signer: $guest_signer" >&2
		exit 1
		;;
esac
if [ "$guest_signer" = release ]; then
	test "$identity" != "-"
	test -n "${NOTARYTOOL_PROFILE:-}"
fi
host_sbom=${AGENT_VMM_HOST_SBOM:-"$guest/sbom.spdx.json"}
host_provenance=${AGENT_VMM_HOST_PROVENANCE:-"$guest/provenance.json"}
metadata_scope=provided
if [ -z "${AGENT_VMM_HOST_SBOM:-}" ] || [ -z "${AGENT_VMM_HOST_PROVENANCE:-}" ]; then
	if [ "$guest_signer" = release ]; then
		echo "release Host App builds require AGENT_VMM_HOST_SBOM and AGENT_VMM_HOST_PROVENANCE" >&2
		exit 1
	fi
	metadata_scope=test-fixture-guest
fi
test -s "$host_sbom"
test -s "$host_provenance"
mkdir -p "$output"
rm -rf "$product"
mkdir -p "$product/Contents/MacOS" "$product/Contents/Helpers" "$product/Contents/Resources/Guest"
cp "$repo/build/host/Info.plist" "$product/Contents/Info.plist"
ditto "$guest" "$product/Contents/Resources/Guest"

if [ -n "$go_tags" ]; then
	GOOS=darwin GOARCH=arm64 go build -tags="$go_tags" -trimpath -o "$product/Contents/MacOS/agent-vmm-host" "$repo/cmd/agent-vmm-host"
else
	GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/MacOS/agent-vmm-host" "$repo/cmd/agent-vmm-host"
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
case "$guest_signer" in
	development)
		jq -e '(.Settings | map({key:.Key, value:.Value}) | from_entries) as $settings |
			(($settings["-tags"] // "") | test("(^|[, ])development([, ]|$)")) and
			(($settings["-tags"] // "") | test("(^|[, ])e2e([, ]|$)") | not)' "$host_info" >/dev/null
		grep -q 'fileApplianceSecrets' "$host_symbols"
		if grep -Eq '_Sec(Item|Key)' "$host_undefined_symbols"; then
			echo "development host links a Keychain or Secure Enclave API" >&2
			exit 1
		fi
		;;
	release)
		jq -e '(.Settings | map({key:.Key, value:.Value}) | from_entries) as $settings |
			(($settings["-tags"] // "") | test("(^|[, ])(development|e2e)([, ]|$)") | not) and
			$settings["vcs.modified"] == "false" and ($settings["vcs.revision"] // "") != ""' "$host_info" >/dev/null
		grep -q 'keychainApplianceSecrets' "$host_symbols"
		grep -q '_SecItemCopyMatching' "$host_undefined_symbols"
		grep -q '_SecKeyCreateRandomKey' "$host_undefined_symbols"
		if grep -Eq 'fileApplianceSecrets|NewDevelopmentFileProvider|developmentFileProvider' "$host_symbols"; then
			echo "release host contains a development secret provider" >&2
			exit 1
		fi
		;;
esac
GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "-X main.releaseID=$release_id" -o "$product/Contents/Helpers/agent-vmm-lifecycle" "$repo/cmd/agent-vmm-lifecycle"
printf '%s\n' "$("$product/Contents/Helpers/agent-vmm-lifecycle" version --json)" |
	jq -e --arg release_id "$release_id" '.version == $release_id and .release_id == $release_id' >/dev/null
CARGO_TARGET_DIR="$derived/rust" cargo build --locked --release --target aarch64-apple-darwin --manifest-path "$repo/data/agent-vmm-data/Cargo.toml"
cp "$derived/rust/aarch64-apple-darwin/release/agent-vmm-data" "$product/Contents/Helpers/agent-vmm-data"
cp "$repo/LICENSE" "$product/Contents/Resources/LICENSE"
cp "$host_sbom" "$product/Contents/Resources/sbom.spdx.json"
cp "$host_provenance" "$product/Contents/Resources/provenance.json"
cp "$repo/build/host/com.afk-surf.agent-vmm-host.plist.template" "$product/Contents/Resources/com.afk-surf.agent-vmm-host.plist.template"

case "$guest_signer" in
	development)
		identity_profile=test
		code_signing=adhoc
		;;
	release)
		identity_profile=release
		code_signing=developer-id
		;;
esac
source_revision=$(jq -r '(.Settings | map({key:.Key, value:.Value}) | from_entries) | (."vcs.revision" // "")' "$host_info")
jq -n \
	--arg component agent-vmm-host-runtime \
	--arg product "Agent VMM Host" \
	--arg bundle_name "Agent VMM Host.app" \
	--arg bundle_identifier com.afk-surf.agent-vmm-host \
	--arg profile "$identity_profile" \
	--arg code_signing "$code_signing" \
	--arg guest_signer "$guest_signer" \
	--arg metadata_scope "$metadata_scope" \
	--arg source_revision "$source_revision" \
	--arg release_id "$release_id" \
	'{
		schema_version: 1,
		component: $component,
		product: $product,
		bundle_name: $bundle_name,
		bundle_identifier: $bundle_identifier,
		identity: {profile: $profile, code_signing: $code_signing, guest_signer: $guest_signer},
		metadata_scope: $metadata_scope,
		layout: {
			main_executable: "Contents/MacOS/agent-vmm-host",
			helpers: ["Contents/Helpers/agent-vmm-data", "Contents/Helpers/agent-vmm-lifecycle"],
			guest: "Contents/Resources/Guest",
			launchd_template: "Contents/Resources/com.afk-surf.agent-vmm-host.plist.template",
			build_info: "Contents/Resources/host-build-info.json",
			license: "Contents/Resources/LICENSE",
			sbom: "Contents/Resources/sbom.spdx.json",
			provenance: "Contents/Resources/provenance.json",
			inventory: "Contents/Resources/release-inventory.json"
		},
		ui: {headless: true, swiftui: false, debug_executable: false, window: false, menu: false, dock: false},
		release_id: $release_id,
		source_revision: $source_revision
	}' > "$product/Contents/Resources/release-inventory.json"

/usr/bin/plutil -lint "$product/Contents/Info.plist"
test "$(/usr/bin/plutil -extract CFBundleExecutable raw -o - "$product/Contents/Info.plist")" = agent-vmm-host
test "$(/usr/bin/plutil -extract CFBundleIdentifier raw -o - "$product/Contents/Info.plist")" = com.afk-surf.agent-vmm-host
test "$(/usr/bin/plutil -extract LSBackgroundOnly raw -o - "$product/Contents/Info.plist")" = true
test ! -e "$product/Contents/MacOS/AgentVMM"
test ! -e "$product/Contents/MacOS/agent-vmm"
test ! -e "$product/Contents/Helpers/agent-vmm-host"
test ! -e "$product/Contents/Helpers/agent-vmm"
test ! -e "$product/Contents/Resources/SwiftUI"
jq -e --arg release_id "$release_id" '
	.schema_version == 1 and
	.component == "agent-vmm-host-runtime" and
	.release_id == $release_id and
	.bundle_name == "Agent VMM Host.app" and
	.bundle_identifier == "com.afk-surf.agent-vmm-host" and
	.layout.main_executable == "Contents/MacOS/agent-vmm-host" and
	.layout.helpers == ["Contents/Helpers/agent-vmm-data", "Contents/Helpers/agent-vmm-lifecycle"] and
	.layout.sbom == "Contents/Resources/sbom.spdx.json" and
	.layout.provenance == "Contents/Resources/provenance.json" and
	.ui == {headless: true, swiftui: false, debug_executable: false, window: false, menu: false, dock: false}
' "$product/Contents/Resources/release-inventory.json" >/dev/null
codesign --force --sign "$identity" --options runtime --entitlements "$repo/build/host/vz.entitlements.plist" "$product/Contents/MacOS/agent-vmm-host"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-lifecycle"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-data"
codesign --force --sign "$identity" --options runtime --preserve-metadata=entitlements "$product"
codesign --verify --deep --strict --verbose=2 "$product"
host_entitlements="$output/agent-vmm-host-entitlements.plist"
codesign -d --xml --entitlements "$host_entitlements" "$host" 2>/dev/null
plutil -convert json -o - "$host_entitlements" |
	jq -e '.["com.apple.security.virtualization"] == true and has("keychain-access-groups") == false' >/dev/null
if [ "$guest_signer" = development ]; then
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

target_archive="$output/Agent-VMM-Host.zip"
ditto -c -k --keepParent "$product" "$target_archive"
artifact_digest=$(shasum -a 256 "$target_archive" | awk '{print $1}')
jq -n \
	--arg component agent-vmm-host-runtime \
	--arg target_revision "$target_revision" \
	--arg artifact_digest "sha256:$artifact_digest" \
	--arg archive "$target_archive" \
	--arg release_id "$release_id" \
	'{component: $component, release_id: $release_id, target_revision: ($target_revision | tonumber), artifact_digest: $artifact_digest, archive: $archive, production_catalog_activated: false}' \
	> "$output/agent-vmm-host-target.json"
