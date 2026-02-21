# Finding: serde_cbor #[serde(flatten)] Produces Indefinite-Length CBOR Maps — Signature Divergence Risk

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Cross-Implementation Compatibility
**Status**: CONFIRMED (documented workaround exists for PushLogReply)

## Summary

`serde_cbor` v0.11.2 produces indefinite-length CBOR maps (major type `0xBF`) when `#[serde(flatten)]` is used. Go's `fxamacker/cbor` produces definite-length CBOR maps. Since signatures are computed over CBOR-serialized bytes, this encoding difference would cause cross-implementation signature verification to fail. `PushLogReply` has already been fixed (fields duplicated without flatten), but `PushLogRequest` STILL USES `#[serde(flatten)]` for its MetaData.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/message/pushlog.rs` | 16-17 | `PushLogRequest` uses `#[serde(flatten)]` for metadata — produces indefinite-length map |
| `crates/p2p/src/message/pushlog.rs` | 110-113 | `PushLogReply` comment documents the flatten issue — fields duplicated as workaround |
| `crates/p2p/src/message/metadata.rs` | 12-44 | `MetaData` struct flattened into PushLogRequest |

## Details

### The Divergence

`serde_cbor` has a known behavior: when `#[serde(flatten)]` is used, the serializer switches from a definite-length map (`0xA6` for 6 entries) to an indefinite-length map (`0xBF ... 0xFF`). This is because flatten requires dynamic map merging, and serde_cbor uses indefinite-length encoding for dynamically-sized maps.

Go's `fxamacker/cbor` always produces definite-length maps for structs.

### PushLogReply — Already Fixed

```rust
// pushlog.rs:110-113 — Comment documents the fix
/// Note: We don't use `#[serde(flatten)]` because serde_cbor produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
```

### PushLogRequest — Still Uses Flatten

```rust
// pushlog.rs:14-17
pub struct PushLogRequest {
    #[serde(flatten)]       // <-- Still uses flatten!
    pub metadata: MetaData,
    #[serde(rename = "DocID")]
    pub doc_id: String,
    // ...
}
```

This means `PushLogRequest` serializes with `0xBF` (indefinite-length map), while Go's equivalent struct serializes with a definite-length map. If cross-implementation signature verification were enabled, PushLogRequest signatures would fail.

### Why This Hasn't Caused Problems Yet

Finding 12 shows that the two-stream handler doesn't verify signatures at all. The signature divergence from flatten doesn't matter when nobody checks signatures. But if/when signature verification is added to the two-stream path, PushLogRequest's flatten will break cross-Go verification.

### serde_cbor Is Unmaintained

`serde_cbor` v0.11.2 was last updated in 2021 and is effectively unmaintained. The indefinite-length map behavior is unlikely to be fixed upstream. The community successor is `ciborium`, which does not have this issue.

## Impact

- PushLogRequest cross-implementation signature verification would fail if enabled
- PushLogReply is already fixed but PushLogRequest is not
- DocSyncRequest, BranchableSyncRequest, and SE messages may also be affected (need audit of their serde attributes)

## Remediation

Apply the same fix used for PushLogReply: duplicate the MetaData fields directly in PushLogRequest instead of using `#[serde(flatten)]`. Alternatively, migrate from `serde_cbor` to `ciborium` which handles flatten correctly.

## Test Gap

No test compares the CBOR byte output of Rust serialization against Go serialization for the same message. No test verifies that `PushLogRequest` produces definite-length CBOR maps.
