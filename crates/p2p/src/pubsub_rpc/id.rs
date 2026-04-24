//! Request-ID derivation.
//!
//! Go's pubsub_rpc uses `cid.NewCidV1(cid.Raw, util.Hash(data))` as the
//! request ID (`rpc.go:217,363`). `cid.Raw = 0x55`; `util.Hash` is
//! `multihash(sha2-256, sha256(data))` — a 32-byte digest with the 0x12/0x20
//! multihash prefix. Rust must derive an identical string or responses from
//! Go won't correlate back to the outstanding request.

use cid::multihash::{Code, MultihashDigest};
use cid::Cid;

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

#[cfg(test)]
mod tests {
    use super::*;

    // Expected CIDv1 for sha256("hello") with the raw codec. Derived from
    // the multiformats spec (same algorithm Go's `cid.NewCidV1(cid.Raw,
    // util.Hash([]byte("hello")))` implements):
    //   sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    //   multihash      = 1220 || <32-byte digest>
    //   cidv1 bytes    = 01 55 || multihash
    //   base32 encode  = lowercase, no-pad, multibase prefix `b`
    const HELLO_CID: &str = "bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq";

    #[test]
    fn matches_go_known_vector() {
        let c = derive_request_id(b"hello");
        assert_eq!(c.to_string(), HELLO_CID);
    }

    #[test]
    fn deterministic() {
        let c1 = derive_request_id(b"some request bytes");
        let c2 = derive_request_id(b"some request bytes");
        assert_eq!(c1, c2);
    }

    #[test]
    fn empty_input_has_stable_cid() {
        // SHA-256 of empty is a well-known value; just check we don't panic
        // and the CID round-trips via its string form.
        let c = derive_request_id(b"");
        let parsed: Cid = c.to_string().parse().expect("round trip");
        assert_eq!(parsed, c);
    }
}
