#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
guest="$scratch/guest"
mkdir "$guest"
artifact() {
	file="$guest/$1"
	printf 'package fixture: %s\n' "$1" > "$file"
	jq -cn --arg path "$1" --arg sha256 "$(shasum -a 256 "$file" | awk '{print $1}')" \
		--argjson size "$(wc -c < "$file" | tr -d ' ')" '{path: $path, sha256: $sha256, size: $size}'
}
kernel=$(artifact Image)
initramfs=$(artifact initramfs.xz)
rootfs=$(artifact rootfs.squashfs)
jq -n --argjson kernel "$kernel" --argjson initramfs "$initramfs" --argjson rootfs "$rootfs" \
	'{schema_version: 1, architecture: "arm64", artifacts: {kernel: $kernel, initramfs: $initramfs, rootfs: $rootfs}}' > "$guest/manifest.json"
printf 'Fixture notices\n' > "$guest/THIRD_PARTY_NOTICES"

export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$scratch/target"}
output="$scratch/output"
unset NOTARYTOOL_PROFILE AGENT_VMM_PROVISIONING_PROFILE AGENT_VMM_PACKAGE_E2E
AGENT_VMM_PACKAGE_PHASE=all CODE_SIGN_IDENTITY=- AGENT_VMM_RELEASE_ID=agent-vmm-package-test \
	AGENT_VMM_GUEST_BUNDLE="$guest" OUTPUT_DIRECTORY="$output" sh "$repo/build/host/package.sh"
app="$output/Agent VMM Host.app"
for executable in "$app/Contents/MacOS/agent-vmm-host" "$app/Contents/Helpers/"*; do
	test -f "$executable" && test ! -L "$executable" && test -x "$executable"
done
(
	cd "$repo"
	export VMM_CONTRACT_REQUIRE_LIVE=1 AGENT_VMM_HOST_APP="$app"
	export AGENT_VMM_LIFECYCLE_BIN="$app/Contents/Helpers/agent-vmm-lifecycle"
	cargo test --locked -p vmm-comma-contract-tests --test live macos_bundle_has_the_executables_cue_checks -- --exact
	cargo test --locked -p vmm-comma-contract-tests --test live lifecycle_helper_reports_a_readable_version -- --exact
)
