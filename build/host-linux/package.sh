#!/bin/sh
set -eu

# Packages the Linux arm64 Host release: agent-vmm (Host, CLI, VMM child, and
# lifecycle), an existing Linux agent-vmm-data, and an existing Guest bundle.
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
output=${OUTPUT_DIRECTORY:-"$repo/dist/linux-arm64"}
guest=${AGENT_VMM_GUEST_BUNDLE:?set AGENT_VMM_GUEST_BUNDLE to the Guest bundle directory}
data_worker=${AGENT_VMM_DATA_WORKER:?set AGENT_VMM_DATA_WORKER to the Linux arm64 agent-vmm-data executable}
release_id=${AGENT_VMM_RELEASE_ID:-agent-vmm-host}
case "$release_id" in
	''|latest|*[!A-Za-z0-9._-]*) echo "AGENT_VMM_RELEASE_ID must be a stable non-latest release id" >&2; exit 1 ;;
esac
test -x "$data_worker"
for artifact in Image initramfs.xz rootfs.squashfs manifest.json; do
	test -s "$guest/$artifact"
done

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'
}

name="agent-vmm-$release_id"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
release="$work/$name"
mkdir -p "$release/guest"
for artifact in Image initramfs.xz rootfs.squashfs manifest.json THIRD_PARTY_NOTICES; do
	cp "$guest/$artifact" "$release/guest/$artifact"
done
if [ -s "$guest/mcp-appliance.digest" ]; then
	cp "$guest/mcp-appliance.digest" "$release/guest/mcp-appliance.digest"
	mkdir -p "$release/guest/mcp-appliance"
	cp "$guest/mcp-appliance/manifest.pb" "$release/guest/mcp-appliance/manifest.pb"
fi
guest_manifest_digest=$(sha256 "$guest/manifest.json")
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags=managedonly -trimpath \
	-ldflags "-X main.guestManifestDigest=$guest_manifest_digest -X github.com/AFK-surf/agent-vmm/internal/lifecycle.releaseID=$release_id" \
	-o "$release/agent-vmm" "$repo/cmd/agent-vmm"
cp "$data_worker" "$release/agent-vmm-data"
chmod 0755 "$release/agent-vmm" "$release/agent-vmm-data"
cp "$repo/LICENSE" "$repo/NOTICE" "$release/"
go run "$repo/build/notices" -o "$release/THIRD_PARTY_NOTICES" \
  -licenses "$repo/third_party/licenses" -go-binary "$release/agent-vmm" \
  -cargo-manifest "$repo/data/agent-vmm-data/Cargo.toml" -cargo-target aarch64-unknown-linux-gnu

# release.json lists every file. Its SHA-256 is the content ID and the
# installed directory name under /opt/agent-vmm.
(cd "$release" && find . -type f | sed 's|^\./||' | LC_ALL=C sort) | while read -r path; do
	executable=false
	case "$path" in agent-vmm|agent-vmm-data) executable=true ;; esac
	jq -cn --arg path "$path" --argjson size "$(wc -c < "$release/$path" | tr -d ' ')" \
		--arg sha256 "$(sha256 "$release/$path")" --argjson executable "$executable" \
		'{path: $path, size: $size, sha256: $sha256, executable: $executable}'
done | jq -cs --arg release_id "$release_id" '{version: 1, release_id: $release_id, files: .}' > "$work/release.json"
mv "$work/release.json" "$release/release.json"
content_id=$(sha256 "$release/release.json")

mkdir -p "$output"
archive="$output/$name-linux-arm64.tar.gz"
COPYFILE_DISABLE=1 tar --no-xattrs -C "$work" -czf "$archive" "$name"
jq -n --arg release_id "$release_id" --arg content_id "$content_id" --arg guest_manifest_sha256 "$guest_manifest_digest" \
	--arg artifact "$(basename "$archive")" --arg artifact_sha256 "$(sha256 "$archive")" \
	--argjson artifact_size "$(wc -c < "$archive" | tr -d ' ')" \
	'{schema_version: 1, platform: "linux/arm64", release_id: $release_id, content_id: $content_id,
	  guest_manifest_sha256: $guest_manifest_sha256, artifact: $artifact, artifact_sha256: $artifact_sha256, artifact_size: $artifact_size}' \
	> "$output/$name-linux-arm64.json"
cat "$output/$name-linux-arm64.json"
