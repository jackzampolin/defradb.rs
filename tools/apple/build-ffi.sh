#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/apple-ffi}"
APPLE_DEPLOYMENT_TARGET="${APPLE_DEPLOYMENT_TARGET:-15.0}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target/apple-cargo/$APPLE_DEPLOYMENT_TARGET}"
HEADER_PATH="$OUT_DIR/include/defra.h"
MODULEMAP_PATH="$OUT_DIR/include/module.modulemap"
FRAMEWORK_NAME="${FRAMEWORK_NAME:-DefraFFI}"
LIB_NAME="${LIB_NAME:-ffi}"
FEATURES="${FEATURES:-iroh,native}"
NO_DEFAULT_FEATURES="${NO_DEFAULT_FEATURES:-1}"
DEVICE_TARGET="${DEVICE_TARGET:-aarch64-apple-ios}"
SIM_TARGETS="${SIM_TARGETS:-aarch64-apple-ios-sim x86_64-apple-ios}"
SWIFT_PACKAGE_DIR="$OUT_DIR/swift"
SWIFT_SMOKE_SOURCE="$ROOT_DIR/tools/apple/swift-import-smoke.swift"

require_tool() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required tool: $1" >&2
        exit 1
    fi
}

require_sdk() {
    if ! xcrun --sdk "$1" --show-sdk-path >/dev/null 2>&1; then
        echo "missing required Apple SDK: $1" >&2
        echo "select a full Xcode installation with iOS platform support" >&2
        exit 1
    fi
}

require_tool cargo
require_tool rustup
require_tool cbindgen
require_tool xcrun
require_sdk iphoneos
require_sdk iphonesimulator

rm -rf "$OUT_DIR/include" "$OUT_DIR/lib" "$OUT_DIR/xcframework" "$SWIFT_PACKAGE_DIR"
mkdir -p "$OUT_DIR/include" "$OUT_DIR/lib" "$OUT_DIR/xcframework" "$SWIFT_PACKAGE_DIR"

feature_args=()
if [[ "$NO_DEFAULT_FEATURES" == "1" || "$NO_DEFAULT_FEATURES" == "true" ]]; then
    feature_args+=(--no-default-features)
fi
if [[ -n "$FEATURES" ]]; then
    feature_args+=(--features "$FEATURES")
fi

write_modulemap() {
    cat >"$MODULEMAP_PATH" <<EOF
module ${FRAMEWORK_NAME} {
    header "defra.h"
    export *
}
EOF
}

write_swift_package() {
    local major_version="${APPLE_DEPLOYMENT_TARGET%%.*}"
    cat >"$SWIFT_PACKAGE_DIR/Package.swift" <<EOF
// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "${FRAMEWORK_NAME}",
    platforms: [
        .iOS(.v${major_version})
    ],
    products: [
        .library(name: "${FRAMEWORK_NAME}", targets: ["${FRAMEWORK_NAME}"])
    ],
    targets: [
        .binaryTarget(
            name: "${FRAMEWORK_NAME}",
            path: "../xcframework/${FRAMEWORK_NAME}.xcframework"
        )
    ]
)
EOF
}

inject_modulemap_into_xcframework() {
    local xcframework_path="$1"
    find "$xcframework_path" -type d -name Headers | while read -r headers_dir; do
        local slice_dir
        slice_dir="$(dirname "$headers_dir")"
        mkdir -p "$slice_dir/Modules"
        cat >"$slice_dir/Modules/module.modulemap" <<EOF
module ${FRAMEWORK_NAME} {
    header "../Headers/defra.h"
    export *
}
EOF
    done
}

validate_swift_import() {
    if ! xcrun --sdk iphonesimulator --find swiftc >/dev/null 2>&1; then
        echo "swiftc not found; skipped Swift import validation" >&2
        return
    fi

    if grep -q "Option_DefraRemoteSignCallback" "$HEADER_PATH"; then
        echo "generated header still exposes Option_DefraRemoteSignCallback" >&2
        exit 1
    fi

    local sdk_path
    local swiftc_path
    local module_cache_dir
    sdk_path="$(xcrun --sdk iphonesimulator --show-sdk-path)"
    swiftc_path="$(xcrun --sdk iphonesimulator --find swiftc)"
    module_cache_dir="$OUT_DIR/swift-module-cache"
    rm -rf "$module_cache_dir"
    mkdir -p "$module_cache_dir"

    "$swiftc_path" \
        -typecheck \
        -target "arm64-apple-ios${APPLE_DEPLOYMENT_TARGET}-simulator" \
        -sdk "$sdk_path" \
        -I "$OUT_DIR/include" \
        -module-cache-path "$module_cache_dir" \
        "$SWIFT_SMOKE_SOURCE"
}

build_target() {
    local target="$1"
    local sdk="iphoneos"

    if [[ "$target" == *"-ios-sim" || "$target" == "x86_64-apple-ios" ]]; then
        sdk="iphonesimulator"
    fi

    rustup target add "$target" >/dev/null
    SDKROOT="$(xcrun --sdk "$sdk" --show-sdk-path)"

    env \
        CARGO_TARGET_DIR="$CARGO_TARGET_DIR" \
        IPHONEOS_DEPLOYMENT_TARGET="$APPLE_DEPLOYMENT_TARGET" \
        SDKROOT="$SDKROOT" \
        cargo build \
        --release \
        -p ffi \
        --target "$target" \
        "${feature_args[@]}"

    local src="$CARGO_TARGET_DIR/$target/release/lib${LIB_NAME}.a"
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
write_modulemap

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
    inject_modulemap_into_xcframework "$XCFRAMEWORK_PATH"
else
    echo "xcodebuild not found; skipped xcframework assembly" >&2
fi

write_swift_package
validate_swift_import

cat <<EOF
Apple FFI artifacts:
  deployment_target: $APPLE_DEPLOYMENT_TARGET
  cargo_target_dir: $CARGO_TARGET_DIR
  header: $HEADER_PATH
  modulemap: $MODULEMAP_PATH
  device: $OUT_DIR/lib/$DEVICE_TARGET/lib${LIB_NAME}.a
  simulator: $SIM_UNIVERSAL_LIB
  xcframework: $XCFRAMEWORK_PATH
  swift_package: $SWIFT_PACKAGE_DIR
  swift_smoke: ok
EOF
