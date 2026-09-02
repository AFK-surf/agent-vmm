#!/usr/bin/env bash
set -euo pipefail

repo="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
script="$repo/build/host/ci-intermediate.sh"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/agent-vmm-ci-intermediate-test.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
revision="${AGENT_VMM_TEST_REVISION:-$(git -C "$repo" rev-parse HEAD)}"
app="$scratch/input/Agent VMM Host.app"
resources="$app/Contents/Resources"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Helpers" "$resources"
for executable in "$app/Contents/MacOS/agent-vmm-host" "$app/Contents/Helpers/agent-vmm-lifecycle" "$app/Contents/Helpers/agent-vmm-data"; do
  printf '#!/bin/sh\nexit 0\n' > "$executable"
  chmod +x "$executable"
done
jq -n --arg revision "$revision" --arg release_id "agent-vmm-$revision" \
  '{source_revision:$revision,release_id:$release_id}' > "$resources/release-inventory.json"
jq -n --arg revision "$revision" \
  '{Settings:[{Key:"vcs.revision",Value:$revision}]}' > "$resources/host-build-info.json"

first="$scratch/first.tar.gz"
second="$scratch/second.tar.gz"
"$script" pack agent-vmm-host "$revision" "$scratch/input" "Agent VMM Host.app" "$first" >/dev/null
touch "$app/Contents/MacOS/agent-vmm-host" "$resources/release-inventory.json"
"$script" pack agent-vmm-host "$revision" "$scratch/input" "Agent VMM Host.app" "$second" >/dev/null
cmp "$first" "$second"

if command -v sha256sum >/dev/null 2>&1; then
  first_sha="$(sha256sum "$first" | awk '{print $1}')"
else
  first_sha="$(shasum -a 256 "$first" | awk '{print $1}')"
fi

if "$script" download agent-vmm-host "$revision" "$first_sha" \
    "$(wc -c < "$first" | tr -d '[:space:]')" "$scratch/no-server.tar.gz" >/dev/null 2>&1; then
  echo "download unexpectedly succeeded without a server" >&2
  exit 1
fi

mkdir -p "$scratch/bin"
cat > "$scratch/bin/aws" <<'EOF'
#!/usr/bin/env bash
set -eu
echo "$*" >> "$AWS_LOG"
case "$*" in
  *" head-object "*) exit 1 ;;
  *" put-object "*) exit 0 ;;
  *) exit 1 ;;
esac
EOF
chmod +x "$scratch/bin/aws"
output="$scratch/output"
AWS_LOG="$scratch/aws.log" GITHUB_OUTPUT="$output" \
  CLOUDFLARE_R2_ACCOUNT_ID=account CLOUDFLARE_R2_BUCKET=bucket \
  AWS_ACCESS_KEY_ID=key AWS_SECRET_ACCESS_KEY=secret \
  PATH="$scratch/bin:$PATH" \
  "$script" publish agent-vmm-host "$revision" "$first"
grep -q "key=ci-intermediates/agent-vmm-host/commit/$revision.tar.gz" "$output"
grep -q -- "--if-none-match \*" "$scratch/aws.log"

wrong_revision="0000000000000000000000000000000000000000"
if "$script" publish agent-vmm-host "$wrong_revision" "$first" >/dev/null 2>&1; then
  echo "archive with the wrong source revision unexpectedly published" >&2
  exit 1
fi

echo "Agent VMM CI intermediate contract passed"
