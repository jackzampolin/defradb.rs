#!/usr/bin/env bash
set -euo pipefail

# Build-only portability gate. It never claims latency, memory, energy, or
# mobile support. Missing targets are reported and skipped unless
# PIR_REQUIRE_PORTABLE_TARGETS=1 is set.

cd "$(dirname "$0")/../../.."

required_targets=(
  x86_64-unknown-linux-gnu
  wasm32-wasip1
  aarch64-linux-android
  aarch64-apple-ios
)

installed="$(rustup target list --installed)"
missing=0
failed=0

for target in "${required_targets[@]}"; do
  if ! grep -qx "$target" <<<"$installed"; then
    echo "PORTABLE_BUILD target=$target status=not-installed"
    missing=1
    continue
  fi

  echo "PORTABLE_BUILD target=$target status=checking"
  if cargo check -p pir-poc --lib --target "$target"; then
    echo "PORTABLE_BUILD target=$target status=passed evidence=build-only"
  else
    echo "PORTABLE_BUILD target=$target status=failed evidence=build-only" >&2
    failed=1
  fi
done

if [[ "${PIR_REQUIRE_PORTABLE_TARGETS:-0}" == "1" && "$missing" == "1" ]]; then
  echo "One or more required portability targets are not installed" >&2
  exit 1
fi

if [[ "$failed" == "1" ]]; then
  echo "One or more installed portability targets failed to compile" >&2
  exit 1
fi

echo "PORTABLE_BUILD reminder='build pass is not a device performance or energy result'"
