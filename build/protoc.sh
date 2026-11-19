#!/bin/sh
set -eu

if [ -n "${PROTOC:-}" ]; then
	printf '%s\n' "$PROTOC"
	exit 0
fi
if command -v protoc; then
	exit 0
fi

version=33.5
case "$(uname -s)/$(uname -m)" in
	Darwin/arm64)
		platform=osx-aarch_64
		digest=7084c6482e3bb416a33fe2162ba566711773b842e6953bf6cb181647b9ef57c0
		;;
	Linux/aarch64|Linux/arm64)
		platform=linux-aarch_64
		digest=2b0fcf9b2c32cbadccc0eb7a88b841fffecd4a06fc80acdba2b5be45e815c38a
		;;
	Linux/x86_64)
		platform=linux-x86_64
		digest=24e58fb231d50306ee28491f33a170301e99540f7e29ca461e0e80fd1239f8d1
		;;
	*) echo "Install protoc for this build platform" >&2; exit 1 ;;
esac
work=$(mktemp -d "$1/protoc.XXXXXXXX")
archive="$work/protoc.zip"
curl -fLsS --connect-timeout 15 --max-time 120 \
	"https://github.com/protocolbuffers/protobuf/releases/download/v$version/protoc-$version-$platform.zip" -o "$archive"
actual=$(shasum -a 256 "$archive" | awk '{print $1}')
if [ "$actual" != "$digest" ]; then
	echo "The Protobuf compiler archive hash does not match" >&2
	exit 1
fi
unzip -q "$archive" -d "$work"
rm "$archive"
printf '%s\n' "$work/bin/protoc"
