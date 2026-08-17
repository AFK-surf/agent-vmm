#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
configuration=${CONFIGURATION:-Release}
output=${OUTPUT_DIRECTORY:-"$repo/dist"}
identity=${CODE_SIGN_IDENTITY:--}
guest=${AGENT_VMM_GUEST_BUNDLE:-/tmp/agent-vmm-e2e-bundle}
derived="$output/DerivedData"
product="$output/Agent VMM.app"

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
mkdir -p "$output"

xcodebuild -quiet -project "$repo/apps/macos/AgentVMM/AgentVMM.xcodeproj" -scheme AgentVMM -configuration "$configuration" \
	-derivedDataPath "$derived" ARCHS=arm64 ONLY_ACTIVE_ARCH=YES build
rm -rf "$product"
ditto "$derived/Build/Products/$configuration/AgentVMM.app" "$product"
mkdir -p "$product/Contents/Helpers" "$product/Contents/Resources/Guest"
ditto "$guest" "$product/Contents/Resources/Guest"

if [ -n "$go_tags" ]; then
	GOOS=darwin GOARCH=arm64 go build -tags="$go_tags" -trimpath -o "$product/Contents/Helpers/agent-vmm-host" "$repo/cmd/agent-vmm-host"
else
	GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/Helpers/agent-vmm-host" "$repo/cmd/agent-vmm-host"
fi
host="$product/Contents/Helpers/agent-vmm-host"
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
GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/Helpers/agent-vmm-lifecycle" "$repo/cmd/agent-vmm-lifecycle"
CARGO_TARGET_DIR="$derived/rust" cargo build --locked --release --target aarch64-apple-darwin --manifest-path "$repo/data/agent-vmm-data/Cargo.toml"
cp "$derived/rust/aarch64-apple-darwin/release/agent-vmm-data" "$product/Contents/Helpers/agent-vmm-data"
cp "$repo/LICENSE" "$product/Contents/Resources/LICENSE"
cp "$repo/build/host/com.afk-surf.agent-vmm.plist" "$product/Contents/Resources/com.afk-surf.agent-vmm.plist"
codesign --force --sign "$identity" --options runtime --entitlements "$repo/build/host/vz.entitlements.plist" "$product/Contents/Helpers/agent-vmm-host"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-lifecycle"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-data"
codesign --force --sign "$identity" --options runtime "$product"
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

app="$HOME/Applications/Agent VMM.app"
sed -e "s|__HOME__|$HOME|g" -e "s|__APP__|$app|g" "$repo/build/host/com.afk-surf.agent-vmm.plist" > "$output/com.afk-surf.agent-vmm.plist"
plutil -lint "$output/com.afk-surf.agent-vmm.plist"

if [ -n "${NOTARYTOOL_PROFILE:-}" ]; then
  ditto -c -k --keepParent "$product" "$output/Agent-VMM.zip"
  xcrun notarytool submit "$output/Agent-VMM.zip" --keychain-profile "$NOTARYTOOL_PROFILE" --wait
  xcrun stapler staple "$product"
fi
