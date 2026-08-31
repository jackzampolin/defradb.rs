#!/usr/bin/env bash
set -euo pipefail

gpu_dpf_commit=ce23a06af884ee54300b5bc5fd5350e445f10b0b
inspire_gpu_commit=c14d1d84a425cdaa9f86ed09465b09c9c9802f13
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
result_root=${DEFRA_PIR_RESULT_ROOT:-"$repo_root/target/pir-research-results"}
repeat_base=${DEFRA_REPEAT_RESULT_DIR:-"$result_root/repeated-gpu-comparison"}
run_id=${DEFRA_REPEAT_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
repeat_root="$repeat_base/$run_id"
repetitions=${DEFRA_GPU_REPETITIONS:-5}
cuda_home=${DEFRA_CUDA_HOME:-/opt/cuda-12.4}

if (( repetitions < 3 )); then
  echo "DEFRA_GPU_REPETITIONS must be at least 3" >&2
  exit 2
fi
for command in jq lscpu nvidia-smi python3; do
  command -v "$command" >/dev/null 2>&1 || { echo "$command is required" >&2; exit 1; }
done

# One qualification/build pass also runs the pinned upstream correctness tests.
bash "$script_dir/run-full-gpu-comparison.sh" quick

dense_binary="$result_root/gpu-dpf-$gpu_dpf_commit/gpu-pir-snapshot"
inspire_binary="$result_root/inspire-gpu-$inspire_gpu_commit/build/bench_e2e"
[[ -x "$dense_binary" ]] || { echo "missing $dense_binary" >&2; exit 1; }
[[ -x "$inspire_binary" ]] || { echo "missing $inspire_binary" >&2; exit 1; }
mkdir -p "$repeat_root"

gpu=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -1)
compute_capability=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -1)
driver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)
total_memory_mib=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits | head -1)
cpu=$(lscpu | awk -F: '/^Model name:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')
cuda=$($cuda_home/bin/nvcc --version | tail -1)

run_dense() {
  local repetition=$1
  local order=dense-first
  if (( repetition % 2 == 0 )); then order=dpf-first; fi
  local output="$repeat_root/dense-dpf-rep-$repetition.jsonl"
  : > "$output"
  for batch in 1 2 4 8 16 32; do
    "$dense_binary" --entries 8388608 --batch "$batch" --samples 7 \
      --min-sample-ms 50 --protocol-order "$order" | tee -a "$output"
  done
}

run_inspire() {
  local repetition=$1
  local log="$repeat_root/inspire-rep-$repetition.log"
  INSPIRE_MAX_BATCH=32 "$inspire_binary" 1 | tee "$log"
  python3 "$script_dir/inspire_gpu_adapter/parse.py" \
    --log "$log" --output "$repeat_root/inspire-rep-$repetition.json" \
    --profile repeated --commit "$inspire_gpu_commit" --gpu "$gpu" \
    --compute-capability "$compute_capability" --driver "$driver" \
    --cpu "$cpu" --cuda "$cuda" --total-memory-mib "$total_memory_mib" \
    --selected-tiers "1"
}

for repetition in $(seq 1 "$repetitions"); do
  if (( repetition % 2 == 1 )); then
    run_dense "$repetition"
    run_inspire "$repetition"
  else
    run_inspire "$repetition"
    run_dense "$repetition"
  fi
done

output="$repeat_root/comparison.json"
python3 "$script_dir/aggregate_gpu_repetitions.py" \
  --root "$repeat_root" --output "$output"
printf 'repetitions=%s\ncomparison=%s\n' "$repetitions" "$output"
