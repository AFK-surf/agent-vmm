#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
output=${OUTPUT_DIRECTORY:?set OUTPUT_DIRECTORY to the signed test output directory}
identity=${CODE_SIGN_IDENTITY:?set CODE_SIGN_IDENTITY to the release signing identity}
profile=${AGENT_VMM_PROVISIONING_PROFILE:-}
test "$identity" != "-"
product="$output/Agent VMM Keychain Tests.app"
mkdir -p "$product/Contents/MacOS"
cp "$repo/build/host/Info.plist" "$product/Contents/Info.plist"
plutil -replace CFBundleExecutable -string keychain-tests "$product/Contents/Info.plist"
rm -f "$product/Contents/embedded.provisionprofile"
entitlements="$repo/build/host/vz.entitlements.plist"
expect_keychain=false
if [ -n "$profile" ]; then
	test -s "$profile"
	cp "$profile" "$product/Contents/embedded.provisionprofile"
	entitlements="$repo/build/host/keychain.entitlements.plist"
	expect_keychain=true
fi
cd "$repo"
GOOS=darwin GOARCH=arm64 go test -c -tags keychain,keychain_integration -o "$product/Contents/MacOS/keychain-tests" ./internal/keychain
codesign --force --sign "$identity" --options runtime --entitlements "$entitlements" "$product"
codesign --verify --deep --strict "$product"
"$product/Contents/MacOS/keychain-tests" -test.v -expect-keychain="$expect_keychain"
