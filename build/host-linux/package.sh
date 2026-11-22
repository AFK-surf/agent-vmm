#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
output=${OUTPUT_DIRECTORY:-"$repo/dist/linux-arm64"}
guest=${AGENT_VMM_GUEST_BUNDLE:?set AGENT_VMM_GUEST_BUNDLE to the Guest bundle directory}
data_worker=${AGENT_VMM_DATA_WORKER:-}
host=${AGENT_VMM_HOST_BIN:-}
release_id=${AGENT_VMM_RELEASE_ID:-agent-vmm-host}
case "$release_id" in
	latest|[!A-Za-z0-9]*|*[!A-Za-z0-9_-]*) echo "Invalid Linux release ID: $release_id" >&2; exit 1 ;;
esac
test "${#release_id}" -le 128
for artifact in Image initramfs.xz rootfs.squashfs manifest.json THIRD_PARTY_NOTICES; do
	test -s "$guest/$artifact"
done

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'
}

name="agent-vmm-$release_id"
mkdir -p "$output"
work=$(mktemp -d "$output/.package.XXXXXXXX")
work=$(CDPATH= cd -- "$work" && pwd)
trap 'rm -rf "$work"' EXIT
release="$work/$name"
mkdir -p "$release/guest"
for artifact in Image initramfs.xz rootfs.squashfs manifest.json THIRD_PARTY_NOTICES; do
	cp "$guest/$artifact" "$release/guest/$artifact"
done
guest_manifest_digest=$(sha256 "$release/guest/manifest.json")
target=aarch64-unknown-linux-gnu
if [ -z "$host" ]; then
	rust_target=${CARGO_TARGET_DIR:-"$repo/target"}
	mkdir -p "$rust_target"
	CARGO_TARGET_DIR=$(CDPATH= cd -- "$rust_target" && pwd)
	export CARGO_TARGET_DIR
	PROTOC=$(sh "$repo/build/protoc.sh" "$work")
	export PROTOC
	(cd "$repo" && AGENT_VMM_RELEASE_ID="$release_id" AGENT_VMM_GUEST_MANIFEST_DIGEST="$guest_manifest_digest" \
		cargo build --locked --release --target "$target" -p agent-vmm)
	host="$CARGO_TARGET_DIR/$target/release/agent-vmm"
fi
install -m 0755 "$host" "$release/agent-vmm"
report=$("$release/agent-vmm" lifecycle version --json)
printf '%s\n' "$report" | jq -e --arg release_id "$release_id" '.release_id == $release_id' >/dev/null

set -- -cargo-manifest "$repo/bins/agent-vmm/Cargo.toml"
if [ -n "$data_worker" ]; then
	test -x "$data_worker"
	install -m 0755 "$data_worker" "$release/agent-vmm-data"
	set -- "$@" -cargo-manifest "$repo/bins/agent-vmm-data/Cargo.toml"
fi
cp "$repo/LICENSE" "$repo/NOTICE" "$release/"
(cd "$repo" && cargo run --locked -p xtask -- notices -o "$release/THIRD_PARTY_NOTICES" \
	-licenses "$repo/third_party/licenses" -cargo-target "$target" "$@")

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

archive="$output/$name-linux-arm64.tar.gz"
COPYFILE_DISABLE=1 tar --no-xattrs -C "$work" -czf "$archive" "$name"
jq -n --arg release_id "$release_id" --arg content_id "$content_id" --arg guest_manifest_sha256 "$guest_manifest_digest" \
	--arg artifact "$(basename "$archive")" --arg artifact_sha256 "$(sha256 "$archive")" \
	--argjson artifact_size "$(wc -c < "$archive" | tr -d ' ')" \
	'{schema_version: 1, platform: "linux/arm64", release_id: $release_id, content_id: $content_id,
	  guest_manifest_sha256: $guest_manifest_sha256, artifact: $artifact, artifact_sha256: $artifact_sha256, artifact_size: $artifact_size}' \
	> "$output/$name-linux-arm64.json"
cat "$output/$name-linux-arm64.json"
