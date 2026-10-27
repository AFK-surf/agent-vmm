#!/bin/sh
while sleep 5; do
  base=/sys/fs/cgroup/agent-vmm
  test -r "$base/workload/memory.current" || continue
  if test ! -e /run/pressure-fixture-child-done; then
    # This 8GiB fixture leaves memory outside the workload parent for the OOM probe.
    echo 6442450944 > "$base/workload/memory.max"
    child="$base/workload/pressure-fixture-child"
    before=$(awk '$1=="oom" {print $2}' "$base/workload/memory.events.local")
    mkdir "$child"
    echo 33554432 > "$child/memory.max"
    sh -c 'echo $$ > "$1/cgroup.procs"; exec awk '\''BEGIN {s="x"; for(i=0;i<27;i++)s=s s; print length(s)}'\''' sh "$child" >/dev/null 2>&1
    child_oom=$(awk '$1=="oom" {print $2}' "$child/memory.events.local")
    after=$(awk '$1=="oom" {print $2}' "$base/workload/memory.events.local")
    echo "PRESSURE_CHILD_OOM child=$child_oom parent_before=$before parent_after=$after" > /dev/hvc0
    rmdir "$child"
    touch /run/pressure-fixture-child-done
  fi
  current=$(cat "$base/workload/memory.current")
  limit=$(cat "$base/workload/memory.max")
  # gVisor's file-backed allocations can spend the request budget reclaiming
  # at memory.max. Once the all-busy fixture reaches 99%, add a small anonymous
  # allocation under the same parent to exercise the kernel's parent-OOM path.
  # The normal reclaim phase targets 91% and never starts this probe.
  if test "$current" -gt "$((limit * 99 / 100))" && test ! -e /run/pressure-fixture-parent-done; then
    touch /run/pressure-fixture-parent-done
    child="$base/workload/pressure-fixture-parent"
    mkdir "$child"
    sh -c 'echo $$ > "$1/cgroup.procs"; exec awk '\''BEGIN {s="x"; for(i=0;i<26;i++)s=s s; print length(s)}'\''' sh "$child" >/dev/null 2>&1
    rmdir "$child"
    current=$(cat "$base/workload/memory.current")
  fi
  system=$(cat "$base/system/memory.current" 2>/dev/null || echo 0)
  total=$(awk '/^MemTotal:/ {print $2*1024}' /proc/meminfo)
  available=$(awk '/^MemAvailable:/ {print $2*1024}' /proc/meminfo)
  oom=$(awk '$1=="oom" {print $2}' "$base/workload/memory.events.local")
  killed=$(awk '$1=="oom_kill" {print $2}' "$base/workload/memory.events.local")
  full=$(awk '$1=="full" {print $2}' "$base/workload/memory.pressure")
  echo "PRESSURE_FIXTURE current=$current limit=$limit system=$system available=$available total=$total oom=$oom killed=$killed $full" > /dev/hvc0
done
