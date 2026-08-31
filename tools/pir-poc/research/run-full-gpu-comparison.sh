#!/usr/bin/env bash
set -euo pipefail

gpu_dpf_commit=ce23a06af884ee54300b5bc5fd5350e445f10b0b
inspire_gpu_commit=c14d1d84a425cdaa9f86ed09465b09c9c9802f13
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
profile=${1:-quick}
result_root=${DEFRA_PIR_RESULT_ROOT:-"$repo_root/target/pir-research-results"}

case "$profile" in
  quick|full) ;;
  *)
    echo "usage: $0 [quick|full]" >&2
    exit 2
    ;;
esac

dense_dir="$result_root/gpu-dpf-$gpu_dpf_commit"
inspire_dir="$result_root/inspire-gpu-$inspire_gpu_commit"
mkdir -p "$dense_dir" "$inspire_dir"

DEFRA_PIR_RESULT_DIR="$dense_dir" \
  bash "$script_dir/run-gpu-pir-defra.sh" "$profile"
DEFRA_PIR_RESULT_DIR="$inspire_dir" \
  bash "$script_dir/run-inspire-gpu-defra.sh" "$profile"

output="$result_root/full-gpu-snapshot-$profile.json"
python3 "$script_dir/compare_gpu_snapshot.py" \
  --dense-dpf "$dense_dir/snapshot-$profile.json" \
  --inspire "$inspire_dir/inspire-gpu-$profile.json" \
  --output "$output"

printf 'profile=%s\ncomparison=%s\n' "$profile" "$output"
