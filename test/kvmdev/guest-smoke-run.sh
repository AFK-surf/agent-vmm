#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then echo 'usage: guest-smoke-run.sh GUEST_BUNDLE TEST_DIRECTORY' >&2; exit 2; fi
repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
work=$(CDPATH= cd -- "$2" && pwd)
date +%s >"$work/guest-smoke-host-unix-seconds"
exec "$repository/test/kvmdev/run.sh" "$1" "$work"
