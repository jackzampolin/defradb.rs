#!/usr/bin/env bash
set -euo pipefail

# USENIX Security 2024 artifact-evaluation revision, rather than moving main.
revision=b9801521301f34502496d694b2ac034857104ebc
repository=https://github.com/menonsamir/ypir.git
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
# A Cargo package below repo_root/target still discovers the parent Defra
# workspace. Keep upstream Rust artifacts outside that tree by default.
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"${TMPDIR:-/tmp}/defra-pir-artifacts"}
checkout="$scratch_root/ypir"
corpus_dir=${DEFRA_PIR_CORPUS_DIR:-"$repo_root/target/pir-research-corpus"}
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/ypir-$revision"}
samples=${DEFRA_PIR_SAMPLES:-3}
rss_limit_bytes=${DEFRA_PIR_RSS_LIMIT_BYTES:-5368709120}
watchdog="$script_dir/run-with-rss-limit.sh"
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

if ! command -v git >/dev/null 2>&1 || ! command -v cc >/dev/null 2>&1 || \
   ! command -v python3 >/dev/null 2>&1; then
  echo "git, Python 3, and a C/C++ compiler are required" >&2
  exit 1
fi
if ! command -v cargo >/dev/null 2>&1 && [[ -x "$HOME/.cargo/bin/cargo" ]]; then
  PATH="$HOME/.cargo/bin:$PATH"
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "rustup Cargo is required" >&2
  exit 1
fi
if [[ ! -f "$corpus_dir/pages.bin" || ! -f "$corpus_dir/manifest.json" ]]; then
  echo "missing exported pages.bin/manifest.json in $corpus_dir" >&2
  echo "run the pir-poc export-simplepir-corpus example before this isolated artifact runner" >&2
  exit 1
fi

python3 "$script_dir/verify-corpus.py" \
  --corpus "$corpus_dir/pages.bin" \
  --manifest "$corpus_dir/manifest.json"

mkdir -p "$scratch_root" "$result_dir"
if [[ ! -d "$checkout/.git" ]]; then
  git clone "$repository" "$checkout"
fi
if [[ $(git -C "$checkout" remote get-url origin) != "$repository" && \
      $(git -C "$checkout" remote get-url origin) != "${repository%.git}" ]]; then
  echo "refusing checkout with unexpected origin: $checkout" >&2
  exit 1
fi
git -C "$checkout" fetch --quiet origin "$revision"
git -C "$checkout" checkout --quiet --detach "$revision"
if [[ $(git -C "$checkout" rev-parse HEAD) != "$revision" ]]; then
  echo "failed to pin YPIR artifact revision" >&2
  exit 1
fi

if [[ $run_smoke -eq 1 ]]; then
  # These are the two official end-to-end correctness paths.  Each uses a
  # 256-MiB logical artifact database, so they are kept explicit rather than
  # hidden inside a broad test invocation.
  (
    cd "$checkout"
    "$watchdog" --limit-bytes "$rss_limit_bytes" \
      --metrics "$result_dir/upstream-basic.rss.json" -- \
      cargo test --release --no-default-features --features server \
      --lib scheme::test::test_ypir_basic -- --exact --nocapture
    "$watchdog" --limit-bytes "$rss_limit_bytes" \
      --metrics "$result_dir/upstream-simplepir-basic.rss.json" -- \
      cargo test --release --no-default-features --features server \
      --lib scheme::test::test_ypir_simplepir_basic -- --exact --nocapture
  ) 2>&1 | tee "$result_dir/upstream-correctness.log"
fi

adapter="$checkout/src/bin/defra_common.rs"
cp "$script_dir/ypir_adapter/main.rs" "$adapter"

feature_mode=scalar-fallback
if grep -qw avx512f /proc/cpuinfo; then
  feature_mode=explicit-avx512
fi

features=server
if [[ $feature_mode == explicit-avx512 ]]; then
  features=server,explicit_avx512
fi

(
  cd "$checkout"
  "$watchdog" --limit-bytes "$rss_limit_bytes" \
    --metrics "$result_dir/adapter-build.rss.json" -- \
    cargo build --release --bin defra_common --no-default-features --features "$features"
) > >(tee "$result_dir/adapter-build.log") \
  2> >(tee "$result_dir/adapter-build.stderr.log" >&2)

(
  cd "$checkout"
  "$watchdog" --limit-bytes "$rss_limit_bytes" \
    --metrics "$result_dir/common-corpus.rss.json" -- \
    /usr/bin/time -v target/release/defra_common \
    --corpus "$corpus_dir/pages.bin" \
    --pages 262144 \
    --page-bytes 96 \
    --samples "$samples" \
    --output "$result_dir/common-corpus.json"
) > >(tee "$result_dir/common-corpus.log") \
  2> >(tee "$result_dir/common-corpus.time" >&2)

printf 'revision=%s\nfeature_mode=%s\ncorpus=%s\nresults=%s\n' \
  "$revision" "$feature_mode" "$corpus_dir" "$result_dir"
