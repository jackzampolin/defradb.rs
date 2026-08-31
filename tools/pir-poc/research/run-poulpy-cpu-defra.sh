#!/usr/bin/env bash
set -euo pipefail

upstream_url=https://github.com/poulpy-fhe/poulpy-pir.git
upstream_commit=533081a74301c8ba6ddd5e1dfc0c9daa6e3e75ef
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"${XDG_CACHE_HOME:-$HOME/.cache}/defra-pir-artifacts"}
checkout="$scratch_root/poulpy-pir-$upstream_commit"
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/poulpy-pir-$upstream_commit"}
profile=${1:-quick}

case "$profile" in quick|full) ;; *) echo "usage: $0 [quick|full]" >&2; exit 2 ;; esac
for command in cargo git lscpu python3 rustup tar; do
  command -v "$command" >/dev/null 2>&1 || { echo "$command is required" >&2; exit 1; }
done
if ! lscpu | grep -E '^Flags:.* avx2 ' >/dev/null || \
   ! lscpu | grep -E '^Flags:.* fma ' >/dev/null; then
  echo "AVX2 and FMA are required" >&2
  exit 1
fi
toolchain=nightly-2026-05-14
if ! rustup toolchain list | grep -q "^$toolchain-"; then
  echo "poulpy-pir requires its pinned $toolchain toolchain" >&2
  echo "Install it with: rustup toolchain install $toolchain --profile minimal" >&2
  exit 1
fi

mkdir -p "$scratch_root" "$result_dir"
if [[ ! -d "$checkout/.git" ]]; then git clone --filter=blob:none "$upstream_url" "$checkout"; fi
git -C "$checkout" fetch --quiet origin "$upstream_commit"
git -C "$checkout" checkout --quiet --detach "$upstream_commit"
[[ $(git -C "$checkout" rev-parse HEAD) == "$upstream_commit" ]] || exit 1
[[ -z $(git -C "$checkout" status --short) ]] || { echo "poulpy-pir checkout is dirty" >&2; exit 1; }

source_tree=$(mktemp -d "${TMPDIR:-/tmp}/defra-poulpy-pir.XXXXXXXX")
git -C "$checkout" archive "$upstream_commit" | tar -x -C "$source_tree"
cp "$source_tree/examples/pir.rs" "$source_tree/examples/defra_bench.rs"
python3 "$script_dir/poulpy_cpu_adapter/patch_benchmark.py" \
  "$source_tree/examples/defra_bench.rs"

export RUSTFLAGS=${DEFRA_POULPY_RUSTFLAGS:-"-C target-feature=+avx2,+fma -C target-cpu=native"}
export CARGO_TARGET_DIR="$result_dir/build"
"cargo" "+$toolchain" build --release --features avx2-fhe --example defra_bench \
  --manifest-path "$source_tree/Cargo.toml"

batches=(1)
if [[ $profile == full ]]; then batches=(1 8 32); fi
if [[ -n ${DEFRA_POULPY_BATCHES:-} ]]; then
  read -r -a batches <<< "$DEFRA_POULPY_BATCHES"
fi
cpu=$(lscpu | awk -F: '/^Model name:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')
for batch in "${batches[@]}"; do
  log="$result_dir/poulpy-cpu-b$batch.log"
  PIR_THREADS=${DEFRA_POULPY_THREADS:-8} \
  PIR_SETUP_THREADS=${DEFRA_POULPY_THREADS:-8} \
  PIR_OFFLINE_THREADS=${DEFRA_POULPY_THREADS:-8} \
  PIR_ONLINE_THREADS=${DEFRA_POULPY_THREADS:-8} \
    /usr/bin/time -v -o "$result_dir/poulpy-cpu-b$batch.time" \
    "$CARGO_TARGET_DIR/release/examples/defra_bench" \
    InsPIRe2-g64-1GiB-c32768 "$batch" | tee "$log"
  python3 "$script_dir/poulpy_cpu_adapter/parse.py" \
    --log "$log" --output "$result_dir/poulpy-cpu-b$batch.json" \
    --commit "$upstream_commit" --cpu "$cpu" --batch "$batch"
done

printf 'upstream_commit=%s\nprofile=%s\nresult_dir=%s\n' \
  "$upstream_commit" "$profile" "$result_dir"
