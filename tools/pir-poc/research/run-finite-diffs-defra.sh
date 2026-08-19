#!/usr/bin/env bash
set -euo pipefail

revision=4574a4f8c52eeda165e110cbb64f834397d7c049
expected_origin=https://github.com/ahenzinger/finite-diffs-pir.git
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"$repo_root/target/pir-artifacts"}
checkout="$scratch_root/finite-diffs-pir"
corpus_dir=${DEFRA_PIR_CORPUS_DIR:-"$repo_root/target/pir-research-corpus"}
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/finite-diffs-$revision"}
samples=${DEFRA_PIR_SAMPLES:-7}
correctness_limit_kib=${DEFRA_PIR_CORRECTNESS_RSS_KIB:-2097152}
adapter_limit_kib=${DEFRA_PIR_ADAPTER_RSS_KIB:-4194304}
minimum_free_kib=${DEFRA_PIR_MINIMUM_FREE_KIB:-5242880}
run_correctness=1
run_adapter=1
check_adapter=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --analysis-only) run_correctness=0; run_adapter=0 ;;
    --check-adapter) run_correctness=0; run_adapter=0; check_adapter=1 ;;
    --skip-correctness) run_correctness=0 ;;
    --skip-adapter) run_adapter=0 ;;
    *)
      echo "usage: $0 [--analysis-only] [--check-adapter] [--skip-correctness] [--skip-adapter]" >&2
      exit 2
      ;;
  esac
  shift
done

mkdir -p "$scratch_root" "$result_dir"
python3 "$script_dir/finite_diffs_artifact/analyze.py" \
  --manifest "$corpus_dir/manifest.json" \
  --output "$result_dir/analysis.json"

if [[ $run_correctness -eq 0 && $run_adapter -eq 0 && $check_adapter -eq 0 ]]; then
  exit 0
fi

if [[ ! -f "$corpus_dir/pages.bin" || ! -f "$corpus_dir/manifest.json" ]]; then
  echo "missing common corpus; export it before entering the isolated artifact window" >&2
  exit 1
fi
if ! command -v go >/dev/null 2>&1 || ! command -v cc >/dev/null 2>&1; then
  echo "Go and a C compiler are required" >&2
  exit 1
fi

available_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
if (( available_kib < minimum_free_kib )); then
  echo "memory guard: ${available_kib} KiB available, require ${minimum_free_kib} KiB" >&2
  exit 75
fi

# A new session gives the monitor an exact process group to terminate.  This is
# intentionally independent of systemd/cgroups, which are unstable in some WSL
# hosts.  The monitor sums resident memory across the compiler, test binary,
# and their descendants and leaves 2+ GiB to the rest of the workspace.
run_guarded() {
  local limit_kib=$1
  shift
  local leader rss status swap_kib
  setsid --wait "$@" &
  leader=$!
  while kill -0 "$leader" 2>/dev/null; do
    rss=$(ps -o rss= -g "$leader" 2>/dev/null | awk '{sum += $1} END {print sum + 0}')
    if (( rss > limit_kib )); then
      echo "memory guard: killing process group $leader at ${rss} KiB (limit ${limit_kib} KiB)" >&2
      kill -TERM -- "-$leader" 2>/dev/null || true
      sleep 1
      kill -KILL -- "-$leader" 2>/dev/null || true
      wait "$leader" || true
      return 75
    fi
    swap_kib=$(awk '/^SwapTotal:/ {total = $2} /^SwapFree:/ {free = $2} END {print total - free}' /proc/meminfo)
    if (( swap_kib > 0 )); then
      echo "memory guard: killing process group $leader after ${swap_kib} KiB of swap use" >&2
      kill -TERM -- "-$leader" 2>/dev/null || true
      sleep 1
      kill -KILL -- "-$leader" 2>/dev/null || true
      wait "$leader" || true
      return 76
    fi
    sleep 0.05
  done
  set +e
  wait "$leader"
  status=$?
  set -e
  return "$status"
}

if [[ ! -d "$checkout/.git" ]]; then
  git clone "$expected_origin" "$checkout"
fi
if [[ $(git -C "$checkout" remote get-url origin) != "$expected_origin" ]]; then
  echo "refusing checkout with unexpected origin" >&2
  exit 1
fi
git -C "$checkout" fetch --quiet origin "$revision"
git -C "$checkout" checkout --quiet --detach "$revision"
if [[ $(git -C "$checkout" rev-parse HEAD) != "$revision" ]]; then
  echo "failed to pin finite-differences artifact revision" >&2
  exit 1
fi

if [[ $run_correctness -eq 1 ]]; then
  # Deliberately bounded. TestPIRMed10240 disables GC around an allocation-heavy
  # encoder and exceeded 7 GiB RSS on this host. TestFakePIRBig* intentionally
  # allocate multi-gigabyte random tables. They are resource/cost tests, not
  # additional correctness coverage.
  correctness_regex='^(TestEncoding.*|TestPIRSmall1|TestPIRSmall10|TestPIRMed1|TestPIRMed10|TestPIRMed100|TestPIRMed1024)$'
  run_guarded "$correctness_limit_kib" bash -lc \
    "cd '$checkout' && go test -count=1 -v -run '$correctness_regex' ./pir" \
    2>&1 | tee "$result_dir/upstream-bounded-correctness.log"
fi

if [[ $run_adapter -eq 1 || $check_adapter -eq 1 ]]; then
  adapter_dir="$checkout/cmd/defra-finite-diffs-adapter"
  mkdir -p "$adapter_dir"
  cp "$script_dir/finite_diffs_artifact/main.go" "$adapter_dir/main.go"
  cp "$script_dir/finite_diffs_artifact/go.mod" "$adapter_dir/go.mod"
fi

if [[ $check_adapter -eq 1 ]]; then
  run_guarded "$adapter_limit_kib" timeout --signal=TERM --kill-after=5s 2m \
    bash -lc "cd '$adapter_dir' && go test ." \
    2>&1 | tee "$result_dir/adapter-compile.log"
fi

if [[ $run_adapter -eq 1 ]]; then
  run_guarded "$adapter_limit_kib" bash -lc \
    "cd '$adapter_dir' && timeout --signal=TERM --kill-after=10s 10m \
      go run . \
      --corpus '$corpus_dir/pages.bin' \
      --manifest '$corpus_dir/manifest.json' \
      --samples '$samples' \
      --output '$result_dir/common-corpus.json'" \
    2>&1 | tee "$result_dir/common-corpus.log"
fi

printf 'revision=%s\ncorpus=%s\nresults=%s\n' "$revision" "$corpus_dir" "$result_dir"
