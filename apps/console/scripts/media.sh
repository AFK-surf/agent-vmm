#!/bin/sh
# Draw the console without a screen: `media.sh picture <dir>` or `media.sh film <dir>`.
set -eu
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
mode=${1:?usage: media.sh picture|film <directory>}
out=${2:?usage: media.sh picture|film <directory>}
# The harness is not shipped: build it without the link-time optimization of a release.
harness() {
  CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
    cargo run --locked --release --features harness --bin console-harness -- "$@"
}
case "$mode" in
  picture)
    harness shots "$out"
    ;;
  film)
    harness film "$out/frames"
    ffmpeg -loglevel error -y -framerate 60 -i "$out/frames/frame-%05d.png" \
      -c:v libx264 -preset slow -crf 16 -pix_fmt yuv420p -movflags +faststart "$out/console.mp4"
    ;;
  *)
    echo "usage: media.sh picture|film <directory>" >&2
    exit 2
    ;;
esac
