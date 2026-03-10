# Apple Embedding

`tools/apple/build-ffi.sh` packages the `ffi` crate for Apple embedding with a modern iOS deployment target, per-target static libraries, a Clang module map, `.xcframework` assembly, and a local Swift Package wrapper for Xcode.

## Prerequisites

- `cargo`
- `rustup`
- `cbindgen`
- `xcrun`
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
It also defaults to `APPLE_DEPLOYMENT_TARGET=15.0` so Rust and native dependencies
are linked against a modern iOS target instead of Rust's legacy iOS 10 default.

Override the defaults with environment variables when needed:

```bash
APPLE_DEPLOYMENT_TARGET=16.0 \
FEATURES="iroh rocksdb" \
OUT_DIR="$PWD/dist/apple" \
FRAMEWORK_NAME="DefraMobile" \
tools/apple/build-ffi.sh
```

`CARGO_TARGET_DIR` can also be overridden when you want the Apple build cache
somewhere other than `target/apple-cargo/<deployment-target>`.

## Outputs

The default output root is `target/apple-ffi/`.

- `include/defra.h`
- `include/module.modulemap`
- `lib/aarch64-apple-ios/libffi.a`
- `lib/aarch64-apple-ios-sim/libffi.a`
- `lib/x86_64-apple-ios/libffi.a`
- `lib/simulator/libffi.a`
- `xcframework/DefraFFI.xcframework`
- `swift/Package.swift`

If `xcodebuild` is unavailable, the script still emits the header and static libraries and skips the `.xcframework` step with a clear warning.

## Swift / Xcode

There are two supported integration paths:

1. Add `target/apple-ffi/swift` as a local Swift package in Xcode.
   The generated `Package.swift` wraps the XCFramework directly.
2. Add `target/apple-ffi/xcframework/DefraFFI.xcframework` to Xcode manually.
   The build script injects `module.modulemap` into every slice so Swift can
   `import DefraFFI` without a bridging header.

The generated C module is named after `FRAMEWORK_NAME` and defaults to `DefraFFI`.

## Mobile FFI Surface

The mobile-oriented entry points are:

- `defra_mobile_init()`
- `defra_mobile_open_node(config_json)`
- `defra_mobile_close_node(node)`
- `defra_mobile_ensure_schema(node, schema_sdl)`
- `defra_mobile_execute(node, request_json)`
- `defra_mobile_peer_info(node)`
- `defra_mobile_connect(node, addr)`
- `defra_mobile_sync_collection(node, request_json)`
- `register_remote_identity(...)`
- `bind_identity_bearer_token(did, bearer_token)`
- `node_set_default_identity(node, did)`

`defra_mobile_open_node` accepts a JSON blob so Swift can avoid hand-building
`NodeInitOptions`. Example:

```json
{
  "dbPath": "/path/to/defra.redb",
  "defaultIdentityDid": "did:key:z...",
  "p2p": {
    "transport": "iroh",
    "iroh": {
      "relayUrl": "https://relay.iroh.network",
      "discovery": true
    }
  }
}
```

`defra_mobile_execute` accepts:

```json
{
  "identityDid": "did:key:z...",
  "query": "mutation { add_Book(input: {name: \"Dune\"}) { _docID } }"
}
```

For device-bound or delegated identities:

- register the logical signing identity with `register_remote_identity`
- bind the host-issued bearer token to that DID with `bind_identity_bearer_token`
- set that DID as the node default via `node_set_default_identity`
