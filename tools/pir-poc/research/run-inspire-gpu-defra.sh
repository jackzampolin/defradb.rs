#!/usr/bin/env bash
set -euo pipefail

upstream_url=https://github.com/keewoolee/inspire-gpu.git
upstream_commit=c14d1d84a425cdaa9f86ed09465b09c9c9802f13
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"${XDG_CACHE_HOME:-$HOME/.cache}/defra-pir-artifacts"}
checkout="$scratch_root/inspire-gpu-$upstream_commit"
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/inspire-gpu-$upstream_commit"}
build_dir="$result_dir/build"
cuda_home=${DEFRA_CUDA_HOME:-/opt/cuda-12.4}
cuda_arch=${DEFRA_CUDA_ARCH:-}
profile=${1:-quick}

case "$profile" in
  quick|full) ;;
  *)
    echo "usage: $0 [quick|full]" >&2
    exit 2
    ;;
esac

for command in cmake git g++-13 lscpu nvidia-smi python3 tar; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required" >&2
    exit 1
  fi
done
if [[ ! -x "$cuda_home/bin/nvcc" ]]; then
  echo "CUDA nvcc is required at $cuda_home/bin/nvcc" >&2
  exit 1
fi
if [[ -z "$cuda_arch" ]]; then
  cuda_arch=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -1 | tr -d '.')
fi

mkdir -p "$scratch_root" "$result_dir"
if [[ ! -d "$checkout/.git" ]]; then
  git clone --filter=blob:none "$upstream_url" "$checkout"
fi
git -C "$checkout" fetch --quiet origin "$upstream_commit"
git -C "$checkout" checkout --quiet --detach "$upstream_commit"
if [[ $(git -C "$checkout" rev-parse HEAD) != "$upstream_commit" ]]; then
  echo "inspire-gpu checkout did not resolve to the pinned commit" >&2
  exit 1
fi
source_tree=$(mktemp -d "${TMPDIR:-/tmp}/defra-inspire-gpu.XXXXXXXX")
git -C "$checkout" archive "$upstream_commit" | tar -x -C "$source_tree"
python3 "$script_dir/inspire_gpu_adapter/patch_benchmark.py" \
  "$source_tree/benches/bench_e2e.cu"

export PATH="$cuda_home/bin:$PATH"
export CC=${DEFRA_CC:-gcc-13}
export CXX=${DEFRA_CXX:-g++-13}
export CUDACXX="$cuda_home/bin/nvcc"
cmake --fresh -S "$source_tree" -B "$build_dir" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CUDA_ARCHITECTURES="$cuda_arch" \
  -DCMAKE_CUDA_HOST_COMPILER="$(command -v "$CXX")"
cmake --build "$build_dir" -j "${DEFRA_BUILD_JOBS:-8}"

if [[ $profile == full ]]; then
  ctest --test-dir "$build_dir" --output-on-failure
else
  ctest --test-dir "$build_dir" --output-on-failure \
    -R 'test_(ring|crypto|gpu_batch|capi)$'
fi

# The published resident footprints are 1.61, 6.44, and 25.77 GB.  Leave
# headroom for CUDA/WSL and batch scratch rather than risking a host OOM.
total_mib=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits | head -1)
if [[ -n ${DEFRA_INSPIRE_GPU_TIERS:-} ]]; then
  read -r -a tiers <<< "$DEFRA_INSPIRE_GPU_TIERS"
else
  if (( total_mib >= 30000 )); then
    tiers=(1 4 16)
  elif (( total_mib >= 12000 )); then
    tiers=(1 4)
  else
    tiers=(1)
  fi
fi

log="$result_dir/inspire-gpu-$profile.log"
time_log="$result_dir/inspire-gpu-$profile.time"
INSPIRE_MAX_BATCH=${DEFRA_INSPIRE_GPU_MAX_BATCH:-32} \
  /usr/bin/time -v -o "$time_log" \
  "$build_dir/bench_e2e" "${tiers[@]}" | tee "$log"

gpu=$(nvidia-smi --query-gpu=name --format=csv,noheader | head -1)
compute_capability=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -1)
driver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)
cpu=$(lscpu | awk -F: '/^Model name:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')
cuda=$($cuda_home/bin/nvcc --version | tail -1)
output="$result_dir/inspire-gpu-$profile.json"
python3 "$script_dir/inspire_gpu_adapter/parse.py" \
  --log "$log" \
  --output "$output" \
  --profile "$profile" \
  --commit "$upstream_commit" \
  --gpu "$gpu" \
  --compute-capability "$compute_capability" \
  --driver "$driver" \
  --cpu "$cpu" \
  --cuda "$cuda" \
  --total-memory-mib "$total_mib" \
  --selected-tiers "${tiers[*]}"

printf 'gpu=%s\nupstream_commit=%s\nprofile=%s\nresult=%s\n' \
  "$gpu" "$upstream_commit" "$profile" "$output"
