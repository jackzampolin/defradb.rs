#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Measure one dense-batch server-evaluation phase, excluding corpus/client work.

Usage:
  bench-perf.sh --cpus LIST --servers N --batch K --kernel NAME [options]

Required:
  --cpus LIST       taskset CPU list for the benchmark (for example 2-4)
  --servers N       2 or 3 replicas
  --batch K         dense batch size
  --kernel NAME     exact bench-dense-batch kernel name

Options:
  --profile P       quick (default) or full
  --sample N        measured sample index, default 0
  --result-dir DIR  output directory
  --energy MODE     auto (default) or off; RAPL is aggregate-only
  --dram-event E@B  aggregate uncore event E with B bytes/count (repeatable)
  --timeout SEC     gate timeout, default 120
  --no-build        use the existing target/release/pir-poc

Examples:
  scripts/bench-perf.sh --cpus 2-4 --servers 3 --batch 64 \
    --kernel grouped-four-russians-g6
  scripts/bench-perf.sh --cpus 2-3 --servers 2 --batch 1 \
    --kernel independent-query-major --energy off

The script deliberately does not support the former whole-process mode. It
starts one perf collector per server TID with counters disabled; the benchmark
worker enables its collector immediately around BatchEvaluator::evaluate.
EOF
}

profile=quick
sample_index=0
result_dir=
cpu_list=
server_count=
batch_size=
kernel=
energy_mode=auto
timeout_seconds=120
build=true
declare -a dram_specs=()

while (($#)); do
  case "$1" in
    --profile) profile="${2:?missing --profile value}"; shift 2 ;;
    --sample) sample_index="${2:?missing --sample value}"; shift 2 ;;
    --result-dir) result_dir="${2:?missing --result-dir value}"; shift 2 ;;
    --cpus) cpu_list="${2:?missing --cpus value}"; shift 2 ;;
    --servers) server_count="${2:?missing --servers value}"; shift 2 ;;
    --batch) batch_size="${2:?missing --batch value}"; shift 2 ;;
    --kernel) kernel="${2:?missing --kernel value}"; shift 2 ;;
    --energy) energy_mode="${2:?missing --energy value}"; shift 2 ;;
    --dram-event) dram_specs+=("${2:?missing --dram-event value}"); shift 2 ;;
    --timeout) timeout_seconds="${2:?missing --timeout value}"; shift 2 ;;
    --no-build) build=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$profile" == quick || "$profile" == full ]] || {
  echo "--profile must be quick or full" >&2
  exit 2
}
[[ "$energy_mode" == auto || "$energy_mode" == off ]] || {
  echo "--energy must be auto or off" >&2
  exit 2
}
[[ "$server_count" =~ ^[23]$ ]] || { echo "--servers must be 2 or 3" >&2; exit 2; }
[[ "$batch_size" =~ ^[1-9][0-9]*$ ]] || { echo "--batch must be positive" >&2; exit 2; }
[[ "$sample_index" =~ ^[0-9]+$ ]] || { echo "--sample must be non-negative" >&2; exit 2; }
[[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "--timeout must be positive" >&2; exit 2; }
[[ -n "$cpu_list" ]] || { echo "--cpus is required" >&2; exit 2; }
[[ -n "$kernel" ]] || { echo "--kernel is required" >&2; exit 2; }

for command in git taskset perf python3 mkfifo; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "$command is required" >&2
    exit 3
  }
done

workspace_root="$(git rev-parse --show-toplevel)"
cd "$workspace_root"
if [[ -z "$result_dir" ]]; then
  result_dir="target/pir-poc-results/perf-server-${profile}-n${server_count}-k${batch_size}-${kernel}-s${sample_index}"
fi
mkdir -p "$result_dir"
result_dir="$(realpath "$result_dir")"
if find "$result_dir" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  echo "result directory is not empty: $result_dir" >&2
  exit 2
fi
# Named FIFOs do not work reliably on WSL's DrvFS (/mnt/c). Keep the live gate
# on the Linux filesystem and copy only immutable evidence into the result.
gate_dir="$(mktemp -d /tmp/defradb-pir-perf.XXXXXX)"
binary="$workspace_root/target/release/pir-poc"

if $build; then
  cargo build --release -p pir-poc
fi
[[ -x "$binary" ]] || { echo "release binary is missing: $binary" >&2; exit 3; }

core_events="cycles,instructions,cache-references,cache-misses,task-clock,context-switches,page-faults"
printf '%s\n' ${core_events//,/ } >"$result_dir/core-events.txt"
: >"$result_dir/unavailable.txt"
: >"$result_dir/aggregate-events.tsv"

declare -a child_pids=()
benchmark_pid=
aggregate_pid=
cleanup() {
  local pid
  for pid in "${child_pids[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  case "$gate_dir" in
    /tmp/defradb-pir-perf.*) rm -rf -- "$gate_dir" ;;
    *) echo "refusing to remove unexpected gate directory: $gate_dir" >&2 ;;
  esac
}
trap cleanup EXIT INT TERM

PIR_POC_PERF_GATE_DIR="$gate_dir" \
PIR_POC_PERF_PROFILE="$profile" \
PIR_POC_PERF_BATCH_SIZE="$batch_size" \
PIR_POC_PERF_SERVER_COUNT="$server_count" \
PIR_POC_PERF_KERNEL="$kernel" \
PIR_POC_PERF_SAMPLE_INDEX="$sample_index" \
PIR_POC_PERF_GATE_TIMEOUT_SECONDS="$timeout_seconds" \
  taskset -c "$cpu_list" "$binary" bench-dense-batch "$profile" \
  >"$result_dir/benchmark.json" 2>"$result_dir/benchmark.stderr" &
benchmark_pid=$!
child_pids+=("$benchmark_pid")

wait_for_file() {
  local path="$1"
  local started=$SECONDS
  while [[ ! -e "$path" ]]; do
    if ! kill -0 "$benchmark_pid" 2>/dev/null; then
      echo "benchmark exited before publishing $path" >&2
      return 1
    fi
    if ((SECONDS - started >= timeout_seconds)); then
      echo "timed out waiting for $path" >&2
      return 1
    fi
    sleep 0.01
  done
}

wait_for_ack() {
  local collector_pid="$1"
  local acknowledgement_fd="$2"
  local started=$SECONDS
  local line
  while true; do
    if IFS= read -r -t 0.1 -u "$acknowledgement_fd" line; then
      [[ "$line" == ack ]]
      return
    fi
    if ! kill -0 "$collector_pid" 2>/dev/null; then
      return 1
    fi
    if ((SECONDS - started >= timeout_seconds)); then
      return 1
    fi
  done
}

wait_for_file "$gate_dir/phase.json"
for ((server_index = 0; server_index < server_count; server_index++)); do
  wait_for_file "$gate_dir/server-${server_index}.tid"
done

declare -a perf_pids=()
declare -a control_fds=()
declare -a acknowledgement_fds=()
for ((server_index = 0; server_index < server_count; server_index++)); do
  control="$gate_dir/server-${server_index}.ctl"
  acknowledgement="$gate_dir/server-${server_index}.ack"
  mkfifo "$control" "$acknowledgement"
  exec {control_fd}<>"$control"
  exec {acknowledgement_fd}<>"$acknowledgement"
  control_fds+=("$control_fd")
  acknowledgement_fds+=("$acknowledgement_fd")
  tid="$(tr -d '[:space:]' <"$gate_dir/server-${server_index}.tid")"
  perf stat --all-user --delay=-1 \
    --control "fifo:$control,$acknowledgement" \
    --no-big-num -x ';' -e "$core_events" \
    -o "$result_dir/server-${server_index}.perf.csv" -t "$tid" &
  perf_pid=$!
  perf_pids+=("$perf_pid")
  child_pids+=("$perf_pid")

  printf 'disable\n' >&"$control_fd"
  if ! wait_for_ack "$perf_pid" "$acknowledgement_fd"; then
    echo "server $server_index perf collector did not acknowledge readiness" >&2
    exit 4
  fi
done

declare -a aggregate_events=()
if [[ "$energy_mode" == auto ]]; then
  if [[ -e /sys/bus/event_source/devices/power/events/energy-pkg ]]; then
    aggregate_events+=("power/energy-pkg/")
    printf 'cpu_energy\tpower/energy-pkg/\t\n' >>"$result_dir/aggregate-events.tsv"
  else
    printf 'cpu_energy: power/energy-pkg/ is not exposed by this kernel\n' >>"$result_dir/unavailable.txt"
  fi
  if [[ -e /sys/bus/event_source/devices/power/events/energy-ram ]]; then
    aggregate_events+=("power/energy-ram/")
    printf 'dram_energy\tpower/energy-ram/\t\n' >>"$result_dir/aggregate-events.tsv"
  else
    printf 'dram_energy: power/energy-ram/ is not exposed by this kernel\n' >>"$result_dir/unavailable.txt"
  fi
else
  printf 'cpu_energy: disabled by --energy off\n' >>"$result_dir/unavailable.txt"
  printf 'dram_energy: disabled by --energy off\n' >>"$result_dir/unavailable.txt"
fi

for spec in "${dram_specs[@]}"; do
  if [[ "$spec" != *@* ]]; then
    echo "--dram-event must be EVENT@BYTES_PER_COUNT: $spec" >&2
    exit 2
  fi
  event="${spec%@*}"
  bytes_per_count="${spec##*@}"
  [[ "$bytes_per_count" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
    echo "invalid DRAM bytes/count in $spec" >&2
    exit 2
  }
  aggregate_events+=("$event")
  printf 'dram_traffic\t%s\t%s\n' "$event" "$bytes_per_count" >>"$result_dir/aggregate-events.tsv"
done
if ((${#dram_specs[@]} == 0)); then
  printf 'dram_traffic: no platform-specific uncore event and bytes/count mapping was supplied\n' >>"$result_dir/unavailable.txt"
fi

if ((${#aggregate_events[@]})); then
  aggregate_control="$gate_dir/aggregate.ctl"
  aggregate_acknowledgement="$gate_dir/aggregate.ack"
  mkfifo "$aggregate_control" "$aggregate_acknowledgement"
  exec {aggregate_control_fd}<>"$aggregate_control"
  exec {aggregate_acknowledgement_fd}<>"$aggregate_acknowledgement"
  declare -a aggregate_event_arguments=()
  for event in "${aggregate_events[@]}"; do
    aggregate_event_arguments+=(-e "$event")
  done
  perf stat -a --delay=-1 \
    --control "fifo:$aggregate_control,$aggregate_acknowledgement" \
    --no-big-num -x ';' "${aggregate_event_arguments[@]}" \
    -o "$result_dir/aggregate.perf.csv" &
  aggregate_pid=$!
  child_pids+=("$aggregate_pid")
  printf 'disable\n' >&"$aggregate_control_fd"
  if wait_for_ack "$aggregate_pid" "$aggregate_acknowledgement_fd"; then
    touch "$gate_dir/aggregate.enabled"
  else
    printf 'aggregate counters: perf collector failed its readiness handshake\n' >>"$result_dir/unavailable.txt"
    kill "$aggregate_pid" 2>/dev/null || true
    aggregate_pid=
  fi
fi

touch "$gate_dir/collectors.ready"
wait_for_file "$gate_dir/phase.done"
if [[ -n "$aggregate_pid" ]] && kill -0 "$aggregate_pid" 2>/dev/null; then
  kill -INT "$aggregate_pid" 2>/dev/null || true
fi

for perf_pid in "${perf_pids[@]}"; do
  wait "$perf_pid" || true
done
if [[ -n "$aggregate_pid" ]]; then
  wait "$aggregate_pid" || true
fi
if ! wait "$benchmark_pid"; then
  echo "benchmark failed; see $result_dir/benchmark.stderr" >&2
  exit 5
fi

mkdir "$result_dir/gate"
cp "$gate_dir/phase.json" "$gate_dir/phase.done" "$result_dir/gate/"
for ((server_index = 0; server_index < server_count; server_index++)); do
  cp "$gate_dir/server-${server_index}.tid" "$result_dir/gate/"
done

{
  printf 'profile=%s\n' "$profile"
  printf 'server_count=%s\n' "$server_count"
  printf 'batch_size=%s\n' "$batch_size"
  printf 'kernel=%s\n' "$kernel"
  printf 'sample_index=%s\n' "$sample_index"
  printf 'cpus=%s\n' "$cpu_list"
  printf 'git_revision=%s\n' "$(git rev-parse HEAD)"
  printf 'git_dirty=%s\n' "$(test -n "$(git status --short)" && printf true || printf false)"
  printf 'rustc=%s\n' "$(rustc --version)"
  printf 'cargo=%s\n' "$(cargo --version)"
  printf 'perf=%s\n' "$(perf --version)"
  printf 'kernel_release=%s\n' "$(uname -srmo)"
  printf 'perf_event_paranoid=%s\n' "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || printf unavailable)"
  printf 'cpu=%s\n' "$(lscpu | awk -F: '/Model name/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')"
} >"$result_dir/environment.txt"

python3 "$workspace_root/tools/pir-poc/scripts/parse-server-perf.py" "$result_dir" \
  >"$result_dir/hardware-counters.json"
cleanup
trap - EXIT INT TERM
echo "wrote phase-scoped hardware results to $result_dir"
