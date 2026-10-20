#!/bin/sh
set -eu
export PATH=/usr/bin:/usr/sbin:/bin:/sbin
mount -t cgroup2 cgroup2 /sys/fs/cgroup
modprobe virtiofs
mkdir -p /tmp/shares /tmp/runsc
mount -t virtiofs agent.hostshares /tmp/shares

prepare() {
  name=$1
  mkdir -p /tmp/$name/rootfs/bin /tmp/$name/rootfs/lib /tmp/$name/rootfs/proc /tmp/$name/rootfs/dev /tmp/$name/rootfs/tmp /tmp/$name/rootfs/share /tmp/$name/rootfs/state
  cp /bin/busybox /tmp/$name/rootfs/bin/busybox
  cp /lib/ld-musl-aarch64.so.1 /tmp/$name/rootfs/lib/
  ln -s busybox /tmp/$name/rootfs/bin/sh
  mount=
  command='i=0; while :; do i=$((i+1)); echo $i > /state/counter; /bin/busybox sleep .05; done'
  if [ "$name" = one ]; then
    mount=', {"destination":"/share","type":"bind","source":"/tmp/shares/authorized","options":["rbind","rw"]}'
    command='exec 3<>/share/data; while :; do echo writer >&3; /bin/busybox sleep .05; done'
  fi
  cat >/tmp/$name/config.json <<EOF
{"ociVersion":"1.0.2","root":{"path":"rootfs","readonly":false},
"process":{"terminal":false,"user":{"uid":0,"gid":0},"args":["/bin/sh","-c","$command"],"env":["PATH=/bin"],"cwd":"/","capabilities":{"bounding":[],"effective":[],"inheritable":[],"permitted":[],"ambient":[]}},
"mounts":[{"destination":"/proc","type":"proc","source":"proc","options":["nosuid","noexec","nodev"]}$mount],
"linux":{"namespaces":[{"type":"pid"},{"type":"mount"},{"type":"uts"},{"type":"ipc"},{"type":"network"}]}}
EOF
}

runtime() { runsc --root=/tmp/runsc --platform=systrap --directfs=false --overlay2=none --host-uds=create --network=none --ignore-cgroups --debug --debug-log=/tmp/runsc.log "$@"; }
run() {
  runtime "$@" || {
    grep -E 'FATAL|error|failed|not supported|permission' /tmp/runsc.log | tail -40
    tail -80 /tmp/runsc.log
    return 1
  }
}
running() {
  for attempt in $(seq 1 100); do
    if runtime state "$1" >/tmp/$1.state 2>/dev/null && grep -Eq '"status"[[:space:]]*:[[:space:]]*"running"' /tmp/$1.state; then return 0; fi
    sleep .05
  done
  cat /tmp/$1.stderr
  return 1
}
settle() {
  run kill "$1" KILL
  if wait "$2"; then return 1; else code=$?; fi
  if [ "$code" -ne 137 ]; then cat /tmp/$1.stderr; return 1; fi
}
prepare one
prepare peer
prepare probe
probe() {
  share=$1
  uid=1000
  mkdir -p /tmp/probe/rootfs/share
  cat >/tmp/probe/config.json <<EOF
{"ociVersion":"1.0.2","root":{"path":"rootfs","readonly":false},"process":{"terminal":false,"user":{"uid":$uid,"gid":$uid},"args":["/bin/sh","-c","/bin/busybox id; /bin/busybox stat -c '%u:%g:%a' /share; if /bin/busybox cat /share/token; then echo READ_OK; else echo READ_DENIED; fi; if echo nonroot >/share/nonroot; then echo WRITE_OK; else echo WRITE_DENIED; fi; for link in absolute relative; do if /bin/busybox cat /share/\$link/secret; then echo ESCAPE_FAILED; else echo ESCAPE_DENIED; fi; done"],"env":["PATH=/bin"],"cwd":"/","capabilities":{"bounding":[],"effective":[],"inheritable":[],"permitted":[],"ambient":[]}},"mounts":[{"destination":"/proc","type":"proc","source":"proc","options":["nosuid","noexec","nodev"]},{"destination":"/share","type":"bind","source":"/tmp/shares/$share","options":["rbind","rw"]}],"linux":{"namespaces":[{"type":"pid"},{"type":"mount"},{"type":"uts"},{"type":"ipc"},{"type":"network"}]}}
EOF
  runtime run --bundle=/tmp/probe probe >/tmp/probe.stdout 2>/tmp/probe.stderr
  cat /tmp/probe.stdout /tmp/probe.stderr
}
echo 'SPIKE READY'
while read -r number action expected; do
  case "$action" in
    READ)
      value=
      for attempt in $(seq 1 100); do
        value=$(cat /tmp/shares/authorized/token 2>/dev/null) || value=
        [ "$value" = "$expected" ] && break
        sleep .05
      done
      echo "SPIKE $number READ $value" ;;
    RO) if echo forbidden >/tmp/shares/readonly/write; then echo "SPIKE $number RO FAILED"; else echo "SPIKE $number RO denied"; fi ;;
    FSINFO)
      runsc --version
      uname -r
      ready=failed
      for attempt in $(seq 1 100); do
        if stat -f /tmp/shares/authorized >/tmp/statfs.log 2>&1; then ready=ready; break; fi
        sleep .05
      done
      cat /tmp/statfs.log
      echo "SPIKE $number FSINFO $ready" ;;
    START)
      runtime run --bundle=/tmp/peer peer >/tmp/peer.stdout 2>/tmp/peer.stderr & peer_runner=$!
      running peer
      runtime run --bundle=/tmp/one one >/tmp/one.stdout 2>/tmp/one.stderr & one_runner=$!
      running one
      echo "SPIKE $number START complete" ;;
    PEER) value=$(run exec peer /bin/busybox cat /state/counter); echo "SPIKE $number PEER $value" ;;
    ISOLATION)
      if runtime exec peer /bin/busybox cat /share/token >/dev/null 2>&1; then echo "SPIKE $number ISOLATION FAILED"; else echo "SPIKE $number ISOLATION denied"; fi ;;
    STOP)
      settle one "$one_runner"
      echo "SPIKE $number STOP complete" ;;
    PROBE)
      probe "$expected"
      echo "SPIKE $number PROBE complete" ;;
    DONE) settle peer "$peer_runner"; echo "SPIKE $number DONE complete" ;;
  esac
done
