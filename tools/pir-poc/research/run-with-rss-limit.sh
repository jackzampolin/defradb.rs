#!/usr/bin/env bash
set -euo pipefail

limit_bytes=5368709120
metrics=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --limit-bytes)
      limit_bytes=$2
      shift 2
      ;;
    --metrics)
      metrics=$2
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "usage: $0 [--limit-bytes N] [--metrics FILE] -- COMMAND..." >&2
      exit 2
      ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "missing command" >&2
  exit 2
fi
if ! command -v setsid >/dev/null 2>&1; then
  echo "setsid is required for process-tree enforcement" >&2
  exit 1
fi

limit_kib=$((limit_bytes / 1024))
peak_rss_kib=0
peak_swap_kib=0
aborted=none

setsid "$@" &
leader=$!

terminate_group() {
  kill -TERM -- "-$leader" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 "$leader" 2>/dev/null; then
      return
    fi
    sleep 1
  done
  kill -KILL -- "-$leader" 2>/dev/null || true
}

while kill -0 "$leader" 2>/dev/null; do
  mapfile -t pids < <(ps -o pid= --sid "$leader" 2>/dev/null | awk '{print $1}')
  rss_kib=0
  swap_kib=0
  for pid in "${pids[@]}"; do
    [[ -r "/proc/$pid/status" ]] || continue
    process_rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status")
    process_swap=$(awk '/^VmSwap:/ {print $2}' "/proc/$pid/status")
    rss_kib=$((rss_kib + ${process_rss:-0}))
    swap_kib=$((swap_kib + ${process_swap:-0}))
  done
  (( rss_kib > peak_rss_kib )) && peak_rss_kib=$rss_kib
  (( swap_kib > peak_swap_kib )) && peak_swap_kib=$swap_kib

  if (( rss_kib > limit_kib )); then
    aborted=rss_limit
    echo "RSS watchdog: ${rss_kib} KiB exceeds ${limit_kib} KiB; terminating process group" >&2
    terminate_group
    break
  fi
  if (( swap_kib > 0 )); then
    aborted=swap_pressure
    echo "RSS watchdog: process tree is using ${swap_kib} KiB swap; terminating process group" >&2
    terminate_group
    break
  fi
  sleep 1
done

set +e
wait "$leader"
status=$?
set -e

if [[ -n "$metrics" ]]; then
  printf '{\n  "limit_bytes": %s,\n  "peak_aggregate_rss_bytes": %s,\n  "peak_aggregate_swap_bytes": %s,\n  "aborted": "%s",\n  "exit_status": %s\n}\n' \
    "$limit_bytes" "$((peak_rss_kib * 1024))" "$((peak_swap_kib * 1024))" \
    "$aborted" "$status" > "$metrics"
fi

if [[ $aborted != none ]]; then
  exit 75
fi
exit "$status"
