#!/usr/bin/env bash

set -u

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
result_dir="$repo_root/target/pir-poc-results"
binary="$repo_root/target/release/pir-poc"
json="$result_dir/production-scale-10m-32b-quick.json"
timing="$result_dir/production-scale-10m-32b-quick.time.txt"
stderr_log="$result_dir/production-scale-10m-32b-quick.stderr.txt"
peak_file="$result_dir/production-scale-10m-32b-quick.peak-rss-kib.txt"
status_file="$result_dir/production-scale-10m-32b-quick.status.txt"
rss_limit_kib=$((6 * 1024 * 1024))
time_limit_seconds=600

mkdir -p "$result_dir"

/usr/bin/time -v -o "$timing" \
    "$binary" bench-production-scale quick execute 10000000 32 \
    >"$json" 2>"$stderr_log" &
timer_pid=$!
started=$(date +%s)
peak_rss_kib=0
current_rss_kib=0
last_report=0
aborted="none"
child_pid=""

while kill -0 "$timer_pid" 2>/dev/null; do
    child_pid=$(pgrep -P "$timer_pid" -f "target/release/pir-poc" | head -n 1 || true)
    if [[ -n "$child_pid" && -r "/proc/$child_pid/status" ]]; then
        current_rss_kib=$(awk '/VmRSS:/ { print $2 }' "/proc/$child_pid/status")
        current_rss_kib=${current_rss_kib:-0}
        if (( current_rss_kib > peak_rss_kib )); then
            peak_rss_kib=$current_rss_kib
        fi
        if (( current_rss_kib >= rss_limit_kib )); then
            aborted="rss_limit"
            kill -TERM "$child_pid" 2>/dev/null || true
            kill -TERM "$timer_pid" 2>/dev/null || true
        fi
    fi

    now=$(date +%s)
    elapsed=$((now - started))
    if (( elapsed >= time_limit_seconds )); then
        aborted="time_limit"
        if [[ -n "$child_pid" ]]; then
            kill -TERM "$child_pid" 2>/dev/null || true
        fi
        kill -TERM "$timer_pid" 2>/dev/null || true
    fi
    if (( elapsed - last_report >= 10 )); then
        echo "PRODUCTION_SCALE_PROGRESS elapsed_s=$elapsed rss_kib=$current_rss_kib peak_rss_kib=$peak_rss_kib"
        last_report=$elapsed
    fi
    sleep 0.25
done

wait "$timer_pid"
exit_code=$?
printf '%s\n' "$peak_rss_kib" >"$peak_file"
printf 'exit_code=%s aborted=%s watchdog_peak_rss_kib=%s\n' \
    "$exit_code" "$aborted" "$peak_rss_kib" >"$status_file"
echo "PRODUCTION_SCALE_DONE exit_code=$exit_code aborted=$aborted peak_rss_kib=$peak_rss_kib"
exit "$exit_code"
