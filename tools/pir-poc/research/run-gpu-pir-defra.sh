#!/usr/bin/env bash
set -euo pipefail

upstream_url=https://github.com/facebookresearch/GPU-DPF.git
upstream_commit=ce23a06af884ee54300b5bc5fd5350e445f10b0b
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"${XDG_CACHE_HOME:-$HOME/.cache}/defra-pir-artifacts"}
checkout="$scratch_root/gpu-dpf-$upstream_commit"
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/gpu-dpf-$upstream_commit"}
cuda_home=${DEFRA_CUDA_HOME:-/opt/cuda-12.4}
cuda_arch=${DEFRA_CUDA_ARCH:-75}
profile=${1:-quick}

case "$profile" in
  quick|full) ;;
  *)
    echo "usage: $0 [quick|full]" >&2
    exit 2
    ;;
esac

for command in git g++-13 jq nvidia-smi; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required" >&2
    exit 1
  fi
done
if [[ ! -x "$cuda_home/bin/nvcc" ]]; then
  echo "CUDA nvcc is required at $cuda_home/bin/nvcc" >&2
  echo "Set DEFRA_CUDA_HOME to an installed toolkit." >&2
  exit 1
fi

cuda_math_header="$cuda_home/targets/x86_64-linux/include/crt/math_functions.h"
if [[ -f "$cuda_math_header" ]] &&
   grep -Fq 'rsqrt(double x);' "$cuda_math_header" &&
   [[ $(getconf GNU_LIBC_VERSION | awk '{print $2}') > 2.40 ]]; then
  echo "CUDA 12.x math declarations are incompatible with this newer glibc." >&2
  echo "Use a supported CUDA/base image pairing, or apply the documented local" >&2
  echo "noexcept compatibility adjustment before running this research adapter." >&2
  echo "See gpu_dpf_adapter/README.md." >&2
  exit 1
fi

mkdir -p "$scratch_root" "$result_dir"
if [[ ! -d "$checkout/.git" ]]; then
  git clone --filter=blob:none "$upstream_url" "$checkout"
fi
git -C "$checkout" fetch --quiet origin "$upstream_commit"
git -C "$checkout" checkout --quiet --detach "$upstream_commit"
if [[ $(git -C "$checkout" rev-parse HEAD) != "$upstream_commit" ]]; then
  echo "GPU-DPF checkout did not resolve to the pinned commit" >&2
  exit 1
fi
if [[ -n $(git -C "$checkout" status --short) ]]; then
  echo "GPU-DPF checkout is dirty; refusing an unpinned benchmark" >&2
  exit 1
fi

source_file="$script_dir/gpu_dpf_adapter/benchmark.cu"
snapshot_binary="$result_dir/gpu-pir-snapshot"
live_binary="$result_dir/gpu-pir-live"
common_flags=(
  -O3
  -std=c++17
  -allow-unsupported-compiler
  -ccbin g++-13
  "-arch=sm_$cuda_arch"
  -lineinfo
  -I"$checkout"
  -Xcompiler=-pthread
  -ldl
)
"$cuda_home/bin/nvcc" "${common_flags[@]}" -DDEFRA_LIMBS=8 \
  "$source_file" -o "$snapshot_binary"
"$cuda_home/bin/nvcc" "${common_flags[@]}" -DDEFRA_LIMBS=1 \
  "$source_file" -o "$live_binary"

samples=5
minimum_ms=30
snapshot_cases=(
  "1048576 1"
  "1048576 8"
  "1048576 32"
  "8388608 1"
  "8388608 8"
)
live_batches=(1 32 128 512)
if [[ $profile == full ]]; then
  samples=9
  minimum_ms=75
  snapshot_cases+=(
    "1048576 128"
    "8388608 32"
    "8388608 128"
    "33554432 1"
    "33554432 8"
    "33554432 32"
    "33554432 128"
  )
  live_batches+=(1024 2048)
fi

snapshot_jsonl="$result_dir/snapshot-$profile.jsonl"
live_jsonl="$result_dir/live-$profile.jsonl"
: > "$snapshot_jsonl"
: > "$live_jsonl"

for case_spec in "${snapshot_cases[@]}"; do
  read -r entries batch <<< "$case_spec"
  "$snapshot_binary" \
    --entries "$entries" \
    --batch "$batch" \
    --samples "$samples" \
    --min-sample-ms "$minimum_ms" | tee -a "$snapshot_jsonl"
done

for batch in "${live_batches[@]}"; do
  "$live_binary" \
    --entries 65536 \
    --batch "$batch" \
    --samples "$samples" \
    --min-sample-ms "$minimum_ms" \
    --live | tee -a "$live_jsonl"
done

jq -s \
  --arg profile "$profile" \
  --arg upstream_url "$upstream_url" \
  --arg upstream_commit "$upstream_commit" \
  --arg cuda "$($cuda_home/bin/nvcc --version | tail -1)" \
  '{schema:"defradb-gpu-pir-suite-v1",profile:$profile,
    upstream:{url:$upstream_url,commit:$upstream_commit},cuda:$cuda,
    snapshot:.}' "$snapshot_jsonl" > "$result_dir/snapshot-$profile.json"
jq -s \
  --arg profile "$profile" \
  --arg upstream_url "$upstream_url" \
  --arg upstream_commit "$upstream_commit" \
  '{schema:"defradb-gpu-live-suite-v1",profile:$profile,
    upstream:{url:$upstream_url,commit:$upstream_commit},
    live_epoch_histogram:.}' "$live_jsonl" > "$result_dir/live-$profile.json"

printf 'gpu=%s\nupstream_commit=%s\nprofile=%s\nsnapshot=%s\nlive=%s\n' \
  "$(nvidia-smi --query-gpu=name --format=csv,noheader | head -1)" \
  "$upstream_commit" "$profile" \
  "$result_dir/snapshot-$profile.json" "$result_dir/live-$profile.json"
