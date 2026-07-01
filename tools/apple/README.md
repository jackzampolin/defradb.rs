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

The script defaults to an iroh-capable Redb build by setting
`FEATURES=redb,iroh,native` and `NO_DEFAULT_FEATURES=1`.
It also defaults to `APPLE_DEPLOYMENT_TARGET=15.0` so Rust and native dependencies
are linked against a modern iOS target instead of Rust's legacy iOS 10 default.
The default iOS feature set avoids desktop-only FFI defaults such as RocksDB,
Lark, and the Wasmtime lens runtime.

Override the defaults with environment variables when needed:

```bash
APPLE_DEPLOYMENT_TARGET=16.0 \
FEATURES="redb,iroh,native" \
OUT_DIR="$PWD/dist/apple" \
FRAMEWORK_NAME="DefraMobile" \
tools/apple/build-ffi.sh
```

Set `NO_DEFAULT_FEATURES=0` only for a desktop-style experiment where the full
FFI default feature set is intentionally desired.

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
- `defra_mobile_disconnect(node, addr)`
- `defra_mobile_sync_collection(node, request_json)`
- `defra_mobile_add_replicator(node, request_json)`
- `register_remote_identity(...)`
- `register_remote_identity_bytes(...)`
- `bind_identity_bearer_token(did, bearer_token)`
- `node_set_default_identity(node, did)`

Rebuild the XCFramework after updating the FFI crate so the generated header
includes newly added symbols such as `defra_mobile_add_replicator` and
`defra_mobile_disconnect`.

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

`defra_mobile_add_replicator` accepts a single peer address, collection names,
and optional per-collection filters.

```json
{
  "identityDid": "did:key:z...",
  "collections": ["Users"],
  "peerAddr": "/ip4/1.2.3.4/tcp/9000/p2p/12D3Koo...",
  "filters": {
    "Users": {"predicate": {"agent_did": {"_eq": "did:key:z..."}}}
  }
}
```

The `filters` field may be omitted or set to `null` for unfiltered replication.
It also accepts the HTTP `Conditions` form and the legacy scalar form:

```json
{
  "Users": {"Conditions": {"agent_did": {"_eq": "did:key:z..."}}},
  "Posts": {"Field": "agent_did", "Value": "did:key:z..."}
}
```

For device-bound or delegated identities:

- register the device signing DID with `register_remote_identity` or `register_remote_identity_bytes`
- optionally bind a delegated bearer token to a logical DID with `bind_identity_bearer_token`
- set the effective DID as the node default via `node_set_default_identity`

## Device-Bound DID Flow

For Secure Enclave-backed P-256 (`secp256r1`) identities on iOS:

1. Generate or load the Secure Enclave private key in the host app.
2. Export or derive the public key bytes, then derive the matching `did:key`.
3. Call one of:
   - `register_remote_identity(did, public_key_hex, "secp256r1", signer_handle, callback)`
   - `register_remote_identity_bytes(did, public_key_ptr, public_key_len, "secp256r1", signer_handle, callback)`
4. If the app uses a logical DID with delegated ACP authorization, call
   `bind_identity_bearer_token(logical_did, bearer_token)`.
5. Call `node_set_default_identity(node, did)` with either the device DID or
   the logical DID that should back GraphQL and block-signing requests.

`register_remote_identity_bytes` is the most direct Swift path when the app
already has the public key as `Data`. For `secp256r1`, pass the uncompressed
X9.63 / SEC1 public key bytes (`0x04 || X || Y`).

## Callback Contract

`DefraRemoteSignCallback` is the host signing hook for non-exportable keys.

- The host app keeps the private key. Defra never receives or persists it.
- `signer_handle` is an opaque host context value passed back on every sign.
- `payload_ptr` / `payload_len` are the exact message bytes to sign.
- On success, write the signature into the provided output buffer, set
  `out_signature_len`, and return `0`.
- For ECDSA keys (`secp256r1`, `secp256k1`), Defra expects ASN.1 DER / X9.62
  signatures, not raw `r || s`.
- For Secure Enclave P-256 signing, use
  `SecKeyCreateSignature(..., .ecdsaSignatureMessageX962SHA256, ...)`.

## Validation

`tools/apple/build-ffi.sh` now runs a Swift import smoke test after packaging.
It typechecks [swift-import-smoke.swift](/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/tools/apple/swift-import-smoke.swift)
against the generated `target/apple-ffi/include` module and fails if the
header regresses to `Option_DefraRemoteSignCallback`.
