#!/usr/bin/env bash
set -euo pipefail

revision=e9020b03bf2872c75b8954e749e32408b5db87ed
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
poc_dir=$(cd -- "$script_dir/.." && pwd)
repo_root=$(cd -- "$poc_dir/../.." && pwd)
scratch_root=${DEFRA_PIR_ARTIFACT_DIR:-"$repo_root/target/pir-artifacts"}
checkout="$scratch_root/simplepir"
corpus_dir=${DEFRA_PIR_CORPUS_DIR:-"$repo_root/target/pir-research-corpus"}
result_dir=${DEFRA_PIR_RESULT_DIR:-"$repo_root/target/pir-research-results/simplepir-$revision"}
samples=${DEFRA_PIR_SAMPLES:-3}
run_smoke=1
export_corpus=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-smoke) run_smoke=0 ;;
    --reuse-corpus) export_corpus=0 ;;
    *)
      echo "usage: $0 [--skip-smoke] [--reuse-corpus]" >&2
      exit 2
      ;;
  esac
  shift
done

# WSL non-login shells commonly omit rustup's default bin directory.
if ! command -v cargo >/dev/null 2>&1 && [[ -x "$HOME/.cargo/bin/cargo" ]]; then
  PATH="$HOME/.cargo/bin:$PATH"
fi
if ! command -v go >/dev/null 2>&1 || ! command -v cc >/dev/null 2>&1 || \
   ! command -v python3 >/dev/null 2>&1; then
  echo "Go, Python 3, and a C compiler are required" >&2
  exit 1
fi
if [[ $export_corpus -eq 1 ]] && ! command -v cargo >/dev/null 2>&1; then
  echo "Cargo is required unless --reuse-corpus is supplied" >&2
  exit 1
fi

mkdir -p "$scratch_root" "$result_dir"
if [[ ! -d "$checkout/.git" ]]; then
  git clone https://github.com/ahenzinger/simplepir "$checkout"
fi
if [[ $(git -C "$checkout" remote get-url origin) != "https://github.com/ahenzinger/simplepir" ]]; then
  echo "refusing to use checkout with an unexpected origin: $checkout" >&2
  exit 1
fi
git -C "$checkout" fetch --quiet origin "$revision"
git -C "$checkout" checkout --quiet --detach "$revision"
if [[ $(git -C "$checkout" rev-parse HEAD) != "$revision" ]]; then
  echo "failed to pin SimplePIR revision" >&2
  exit 1
fi

if [[ $export_corpus -eq 1 ]]; then
  cargo run --quiet --manifest-path "$repo_root/Cargo.toml" -p pir-poc \
    --release --example export-simplepir-corpus -- \
    "$corpus_dir" 1048576 262144
elif [[ ! -f "$corpus_dir/pages.bin" || ! -f "$corpus_dir/manifest.json" ]]; then
  echo "--reuse-corpus requested but pages.bin or manifest.json is missing in $corpus_dir" >&2
  exit 1
fi

python3 "$script_dir/verify-corpus.py" \
  --corpus "$corpus_dir/pages.bin" \
  --manifest "$corpus_dir/manifest.json"

if [[ $run_smoke -eq 1 ]]; then
  (
    cd "$checkout/pir"
    go test -run '^(TestSimplePir|TestSimplePirCompressed|TestSimplePirLongRow|TestSimplePirBatch|TestDoublePirLongRowCompressed)$' -v
  ) | tee "$result_dir/upstream-smoke.log"
fi

adapter_dir="$checkout/cmd/defra-simplepir-adapter"
mkdir -p "$adapter_dir"
cp "$script_dir/simplepir_adapter/main.go" "$adapter_dir/main.go"

for protocol in simple double; do
  (
    cd "$checkout"
    go run ./cmd/defra-simplepir-adapter \
      --corpus "$corpus_dir/pages.bin" \
      --manifest "$corpus_dir/manifest.json" \
      --protocol "$protocol" \
      --samples "$samples" \
      --output "$result_dir/$protocol.json"
  ) | tee "$result_dir/$protocol.log"
done

printf 'revision=%s\ncorpus=%s\nresults=%s\n' "$revision" "$corpus_dir" "$result_dir"
