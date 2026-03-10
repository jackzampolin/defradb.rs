# Apple Embedding

`tools/apple/build-ffi.sh` packages the `ffi` crate for Apple embedding with header generation, per-target static libraries, and `.xcframework` assembly when `xcodebuild` is available.

## Prerequisites

- `cargo`
- `rustup`
- `cbindgen`
- Apple Rust targets:
  - `aarch64-apple-ios`
  - `aarch64-apple-ios-sim`
  - `x86_64-apple-ios`
- `xcodebuild` for `.xcframework` creation
- `lipo` when combining multiple simulator architectures

## Usage

```bash
tools/apple/build-ffi.sh
```

The script defaults to an iroh-capable build by setting `FEATURES=iroh`.

Override the defaults with environment variables when needed:

```bash
FEATURES="iroh rocksdb" \
OUT_DIR="$PWD/dist/apple" \
FRAMEWORK_NAME="DefraMobile" \
tools/apple/build-ffi.sh
```

## Outputs

The default output root is `target/apple-ffi/`.

- `include/defra.h`
- `lib/aarch64-apple-ios/libffi.a`
- `lib/aarch64-apple-ios-sim/libffi.a`
- `lib/x86_64-apple-ios/libffi.a`
- `lib/simulator/libffi.a`
- `xcframework/DefraFFI.xcframework`

If `xcodebuild` is unavailable, the script still emits the header and static libraries and skips the `.xcframework` step with a clear warning.
