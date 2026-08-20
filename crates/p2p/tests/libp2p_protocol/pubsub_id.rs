use cid::Cid;
use p2p::pubsub_rpc::derive_request_id;

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
