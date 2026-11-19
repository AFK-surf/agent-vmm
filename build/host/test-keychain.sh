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
set --
expect_keychain=false
if [ -n "$profile" ]; then
	test -s "$profile"
	cp "$profile" "$product/Contents/embedded.provisionprofile"
	set -- --entitlements "$repo/build/host/keychain.entitlements.plist"
	expect_keychain=true
fi
xcrun swiftc -o "$product/Contents/MacOS/keychain-tests" "$repo/build/host/keychain-probe.swift"
codesign --force --sign "$identity" --options runtime "$@" "$product"
codesign --verify --deep --strict "$product"
"$product/Contents/MacOS/keychain-tests" "$expect_keychain"
