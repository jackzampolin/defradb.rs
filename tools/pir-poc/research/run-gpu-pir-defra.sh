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
protocol_order=${DEFRA_GPU_PROTOCOL_ORDER:-dense-first}

case "$profile" in
  quick|full) ;;
  *)
    echo "usage: $0 [quick|full]" >&2
    exit 2
    ;;
esac
case "$protocol_order" in
  dense-first|dpf-first) ;;
  *)
    echo "DEFRA_GPU_PROTOCOL_ORDER must be dense-first or dpf-first" >&2
    exit 2
    ;;
esac

for command in git g++-13 jq lscpu nvidia-smi; do
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
total_memory_mib=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits | head -1)

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
  "8388608 2"
  "8388608 4"
  "8388608 8"
  "8388608 16"
  "8388608 32"
)
live_batches=(1 32 128 512)
if [[ $profile == full ]]; then
  samples=9
  minimum_ms=75
  snapshot_cases+=(
    "1048576 128"
    "8388608 128"
    "33554432 1"
    "33554432 8"
    "33554432 32"
    "33554432 128"
  )
  if (( total_memory_mib >= 30000 )); then
    snapshot_cases+=(
      "134217728 1"
      "134217728 8"
      "134217728 32"
      "134217728 128"
    )
  fi
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
    --min-sample-ms "$minimum_ms" \
    --protocol-order "$protocol_order" | tee -a "$snapshot_jsonl"
done

for batch in "${live_batches[@]}"; do
  "$live_binary" \
    --entries 65536 \
    --batch "$batch" \
    --samples "$samples" \
    --min-sample-ms "$minimum_ms" \
    --live | tee -a "$live_jsonl"
done

gpu=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -1)
compute_capability=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -1)
driver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)
cpu=$(lscpu | awk -F: '/^Model name:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')
jq -s \
  --arg profile "$profile" \
  --arg upstream_url "$upstream_url" \
  --arg upstream_commit "$upstream_commit" \
  --arg cuda "$($cuda_home/bin/nvcc --version | tail -1)" \
  --arg gpu "$gpu" \
  --arg compute_capability "$compute_capability" \
  --arg driver "$driver" \
  --arg cpu "$cpu" \
  --argjson total_memory_mib "$total_memory_mib" \
  '{schema:"defradb-gpu-pir-suite-v2",profile:$profile,
    upstream:{url:$upstream_url,commit:$upstream_commit},cuda:$cuda,
    hardware:{gpu:$gpu,compute_capability:$compute_capability,
      driver:$driver,cpu:$cpu},
    capacity:{device_memory_mib:$total_memory_mib,
      completed_entries:(map(.entries)|unique),
      capacity_blocked:(if (map(.entries)|index(134217728)) then [] else
        [{entries:134217728,useful_row_bytes:120,
          physical_table_bytes_per_replica:17179869184,
          reason:"16 GiB table plus selectors/workspaces requires a 30 GiB-class GPU"}]
        end)},
    snapshot:.}' "$snapshot_jsonl" > "$result_dir/snapshot-$profile.json"
jq -s \
  --arg profile "$profile" \
  --arg upstream_url "$upstream_url" \
  --arg upstream_commit "$upstream_commit" \
  --arg gpu "$gpu" \
  --arg compute_capability "$compute_capability" \
  --arg driver "$driver" \
  --arg cpu "$cpu" \
  '{schema:"defradb-gpu-live-suite-v2",profile:$profile,
    upstream:{url:$upstream_url,commit:$upstream_commit},
    hardware:{gpu:$gpu,compute_capability:$compute_capability,
      driver:$driver,cpu:$cpu},
    live_epoch_histogram:.}' "$live_jsonl" > "$result_dir/live-$profile.json"

printf 'gpu=%s\nupstream_commit=%s\nprofile=%s\nsnapshot=%s\nlive=%s\n' \
  "$gpu" \
  "$upstream_commit" "$profile" \
  "$result_dir/snapshot-$profile.json" "$result_dir/live-$profile.json"
