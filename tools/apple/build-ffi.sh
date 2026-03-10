#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/apple-ffi}"
HEADER_PATH="$OUT_DIR/include/defra.h"
FRAMEWORK_NAME="${FRAMEWORK_NAME:-DefraFFI}"
LIB_NAME="${LIB_NAME:-ffi}"
FEATURES="${FEATURES:-iroh}"
DEVICE_TARGET="${DEVICE_TARGET:-aarch64-apple-ios}"
SIM_TARGETS="${SIM_TARGETS:-aarch64-apple-ios-sim x86_64-apple-ios}"

require_tool() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required tool: $1" >&2
        exit 1
    fi
}

require_tool cargo
require_tool rustup
require_tool cbindgen

mkdir -p "$OUT_DIR/include" "$OUT_DIR/lib" "$OUT_DIR/xcframework"

feature_args=()
if [[ -n "$FEATURES" ]]; then
    feature_args=(--features "$FEATURES")
fi

build_target() {
    local target="$1"
    rustup target add "$target" >/dev/null

    cargo build \
        --release \
        -p ffi \
        --target "$target" \
        "${feature_args[@]}"

    local src="$ROOT_DIR/target/$target/release/lib${LIB_NAME}.a"
    local dest="$OUT_DIR/lib/$target"
    mkdir -p "$dest"
    cp "$src" "$dest/lib${LIB_NAME}.a"
}

build_target "$DEVICE_TARGET"

read -r -a simulator_targets <<<"$SIM_TARGETS"
for target in "${simulator_targets[@]}"; do
    build_target "$target"
done

cbindgen \
    --config "$ROOT_DIR/crates/ffi/cbindgen.toml" \
    --crate ffi \
    --output "$HEADER_PATH"

simulator_libs=()
for target in "${simulator_targets[@]}"; do
    lib_path="$OUT_DIR/lib/$target/lib${LIB_NAME}.a"
    if [[ -f "$lib_path" ]]; then
        simulator_libs+=("$lib_path")
    fi
done

if [[ "${#simulator_libs[@]}" -eq 0 ]]; then
    echo "no simulator libraries were built" >&2
    exit 1
fi

SIM_UNIVERSAL_DIR="$OUT_DIR/lib/simulator"
SIM_UNIVERSAL_LIB="$SIM_UNIVERSAL_DIR/lib${LIB_NAME}.a"
mkdir -p "$SIM_UNIVERSAL_DIR"

if [[ "${#simulator_libs[@]}" -eq 1 ]]; then
    cp "${simulator_libs[0]}" "$SIM_UNIVERSAL_LIB"
else
    require_tool lipo
    lipo -create "${simulator_libs[@]}" -output "$SIM_UNIVERSAL_LIB"
fi

XCFRAMEWORK_PATH="$OUT_DIR/xcframework/${FRAMEWORK_NAME}.xcframework"
if command -v xcodebuild >/dev/null 2>&1; then
    rm -rf "$XCFRAMEWORK_PATH"
    xcodebuild -create-xcframework \
        -library "$OUT_DIR/lib/$DEVICE_TARGET/lib${LIB_NAME}.a" \
        -headers "$OUT_DIR/include" \
        -library "$SIM_UNIVERSAL_LIB" \
        -headers "$OUT_DIR/include" \
        -output "$XCFRAMEWORK_PATH"
else
    echo "xcodebuild not found; skipped xcframework assembly" >&2
fi

cat <<EOF
Apple FFI artifacts:
  header: $HEADER_PATH
  device: $OUT_DIR/lib/$DEVICE_TARGET/lib${LIB_NAME}.a
  simulator: $SIM_UNIVERSAL_LIB
  xcframework: $XCFRAMEWORK_PATH
EOF
