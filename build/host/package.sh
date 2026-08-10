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
GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/Helpers/agent-vmm-mcp" "$repo/cmd/agent-vmm-mcp"
GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/Helpers/agent-vmm-connect" "$repo/cmd/agent-vmm-connect"
GOOS=darwin GOARCH=arm64 go build -trimpath -o "$product/Contents/Helpers/agent-vmm-lifecycle" "$repo/cmd/agent-vmm-lifecycle"
go version -m "$product/Contents/Helpers/agent-vmm-mcp" > "$product/Contents/Resources/mcp-build-info.txt"
go version -m "$product/Contents/Helpers/agent-vmm-connect" > "$product/Contents/Resources/connect-build-info.txt"
cp "$repo/LICENSE" "$product/Contents/Resources/LICENSE"
cp "$repo/build/host/com.afk-surf.agent-vmm.plist" "$product/Contents/Resources/com.afk-surf.agent-vmm.plist"
cp "$repo/build/host/com.afk-surf.agent-vmm-connect.plist" "$product/Contents/Resources/com.afk-surf.agent-vmm-connect.plist"
codesign --force --sign "$identity" --options runtime --entitlements "$repo/build/host/vz.entitlements.plist" "$product/Contents/Helpers/agent-vmm-host"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-mcp"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-connect"
codesign --force --sign "$identity" --options runtime "$product/Contents/Helpers/agent-vmm-lifecycle"
codesign --force --sign "$identity" --options runtime "$product"
codesign --verify --deep --strict --verbose=2 "$product"

app="$HOME/Applications/Agent VMM.app"
sed -e "s|__HOME__|$HOME|g" -e "s|__APP__|$app|g" "$repo/build/host/com.afk-surf.agent-vmm.plist" > "$output/com.afk-surf.agent-vmm.plist"
plutil -lint "$output/com.afk-surf.agent-vmm.plist"
sed -e "s|__HOME__|$HOME|g" -e "s|__APP__|$app|g" "$repo/build/host/com.afk-surf.agent-vmm-connect.plist" > "$output/com.afk-surf.agent-vmm-connect.plist"
plutil -lint "$output/com.afk-surf.agent-vmm-connect.plist"

if [ -n "${NOTARYTOOL_PROFILE:-}" ]; then
  ditto -c -k --keepParent "$product" "$output/Agent-VMM.zip"
  xcrun notarytool submit "$output/Agent-VMM.zip" --keychain-profile "$NOTARYTOOL_PROFILE" --wait
  xcrun stapler staple "$product"
fi
