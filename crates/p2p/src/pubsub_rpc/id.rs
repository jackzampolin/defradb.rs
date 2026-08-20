//! Request-ID derivation.
//!
//! Go's pubsub_rpc uses `cid.NewCidV1(cid.Raw, util.Hash(data))` as the
//! request ID (`rpc.go:217,363`). `cid.Raw = 0x55`; `util.Hash` is
//! `multihash(sha2-256, sha256(data))` — a 32-byte digest with the 0x12/0x20
//! multihash prefix. Rust must derive an identical string or responses from
//! Go won't correlate back to the outstanding request.

use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};

/// Multicodec for raw binary data. Matches Go's `cid.Raw`.
const RAW_CODEC: u64 = 0x55;

/// Derive the request ID used for RPC correlation.
///
/// Returns the CIDv1 over the raw request bytes, using SHA-256. Stringifying
/// with the default Cid::to_string produces a base32-encoded lowercase string
/// beginning with `bafk` — the same shape Go's `cid.Cid.String()` emits.
pub fn derive_request_id(data: &[u8]) -> Cid {
    let mh = Code::Sha2_256.digest(data);
    Cid::new_v1(RAW_CODEC, mh)
}
