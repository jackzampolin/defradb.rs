#!/usr/bin/env bash
set -euo pipefail

record=17361471
archive_md5=bfa9edb2d8403f0dc20830fb40608b78
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
# Avoid accidental membership in Defra's parent Cargo workspace.
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"${TMPDIR:-/tmp}/defra-pir-artifacts"}
artifact_root="$scratch_root/inspire-zenodo-$record"
archive="$artifact_root/artifact-final.zip"
checkout="$artifact_root/src/artifact_package/artifact"
corpus_dir=${DEFRA_PIR_CORPUS_DIR:-"$repo_root/target/pir-research-corpus"}
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/inspire-zenodo-$record"}
samples=${DEFRA_PIR_SAMPLES:-3}
dim0=${DEFRA_INSPIRE_DIM0:-8192}
run_smoke=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-smoke) run_smoke=0 ;;
    *)
      echo "usage: $0 [--skip-smoke]" >&2
      exit 2
      ;;
  esac
  shift
done

if ! command -v cargo >/dev/null 2>&1 && [[ -x "$HOME/.cargo/bin/cargo" ]]; then
  PATH="$HOME/.cargo/bin:$PATH"
fi
for command in cargo cc curl md5sum python3 unzip; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required" >&2
    exit 1
  fi
done
if [[ ! -f "$corpus_dir/pages.bin" || ! -f "$corpus_dir/manifest.json" ]]; then
  echo "missing exported pages.bin/manifest.json in $corpus_dir" >&2
  exit 1
fi

python3 "$script_dir/verify-corpus.py" \
  --corpus "$corpus_dir/pages.bin" \
  --manifest "$corpus_dir/manifest.json"

mkdir -p "$artifact_root" "$result_dir"
if [[ ! -f "$archive" ]]; then
  curl -fsSL \
    "https://zenodo.org/api/records/$record/files/artifact-final.zip/content" \
    -o "$archive"
fi
echo "$archive_md5  $archive" | md5sum -c -

# Re-extracting restores the exact artifact before the checked adapter patch.
unzip -o -q "$archive" -d "$artifact_root/src"
python3 "$script_dir/inspire_adapter/patch_inspire.py" \
  "$checkout/src/bin/inspire.rs"

if ! grep -qw avx512f /proc/cpuinfo; then
  cat > "$result_dir/BLOCKED.txt" <<EOF
InsPIRe Zenodo $record requires AVX-512F. This host does not expose avx512f.
The archive checksum and corpus adapter were validated, but correctness and
performance were not run. No paper or synthetic timing is substituted.
EOF
  echo "InsPIRe is blocked on this runner: AVX-512F is not available" >&2
  echo "See $result_dir/BLOCKED.txt" >&2
  exit 3
fi

if [[ $run_smoke -eq 1 ]]; then
  (
    cd "$checkout"
    cargo run --release --bin inspire -- \
      --num-items 4096 \
      --item-size-bits 8192 \
      --dim0 1024 \
      --trials 1 \
      --out-report-json "$result_dir/upstream-smoke.json"
  ) 2>&1 | tee "$result_dir/upstream-smoke.log"
fi

(
  cd "$checkout"
  /usr/bin/time -v cargo run --release --bin inspire -- \
    --num-items 262144 \
    --item-size-bits 768 \
    --dim0 "$dim0" \
    --trials "$samples" \
    --defra-corpus "$corpus_dir/pages.bin" \
    --defra-target-page 17 \
    --defra-mapping-json "$result_dir/mapping.json" \
    --out-report-json "$result_dir/upstream-measurement.json"
) > >(tee "$result_dir/common-corpus.log") \
  2> >(tee "$result_dir/common-corpus.time" >&2)

python3 "$script_dir/inspire_adapter/qualify.py" \
  --measurement "$result_dir/upstream-measurement.json" \
  --mapping "$result_dir/mapping.json" \
  --manifest "$corpus_dir/manifest.json" \
  --output "$result_dir/common-corpus.json"

printf 'record=%s\narchive_md5=%s\ndim0=%s\ncorpus=%s\nresults=%s\n' \
  "$record" "$archive_md5" "$dim0" "$corpus_dir" "$result_dir"
