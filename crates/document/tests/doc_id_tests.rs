// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Integration tests for DocID type

use cid::Cid;
use document::{DocID, DOC_ID_V0, SDN_NAMESPACE_V0};
use multihash::MultihashGeneric;
use sha2::{Digest, Sha256};

const SHA2_256_CODE: u64 = 0x12;

fn test_cid() -> Cid {
    let mut hasher = Sha256::new();
    hasher.update(b"test document content");
    let hash_bytes = hasher.finalize();
    let mh: MultihashGeneric<64> = MultihashGeneric::wrap(SHA2_256_CODE, &hash_bytes).unwrap();
    Cid::new_v1(0x55, mh) // 0x55 = raw codec
}

#[test]
fn test_sdn_namespace_matches_go() {
    // Verify the namespace UUID matches Go's SDNNamespaceV0
    assert_eq!(
        SDN_NAMESPACE_V0.to_string(),
        "c94acbfa-dd53-40d0-97f3-29ce16c333fc"
    );
}

#[test]
fn test_new_v0() {
    let cid = test_cid();
    let doc_id = DocID::new_v0(cid.clone());

    assert_eq!(doc_id.version(), DOC_ID_V0);
    assert_eq!(doc_id.cid(), Some(&cid));
    assert!(!doc_id.uuid().is_nil());
}

#[test]
fn test_deterministic_uuid_from_cid() {
    let cid = test_cid();
    let doc_id1 = DocID::new_v0(cid.clone());
    let doc_id2 = DocID::new_v0(cid);

    // Same CID should produce same UUID
    assert_eq!(doc_id1.uuid(), doc_id2.uuid());
}

#[test]
fn test_different_cids_produce_different_uuids() {
    let mut hasher1 = Sha256::new();
    hasher1.update(b"document 1");
    let hash1 = hasher1.finalize();
    let mh1: MultihashGeneric<64> = MultihashGeneric::wrap(SHA2_256_CODE, &hash1).unwrap();

    let mut hasher2 = Sha256::new();
    hasher2.update(b"document 2");
    let hash2 = hasher2.finalize();
    let mh2: MultihashGeneric<64> = MultihashGeneric::wrap(SHA2_256_CODE, &hash2).unwrap();

    let cid1 = Cid::new_v1(0x55, mh1);
    let cid2 = Cid::new_v1(0x55, mh2);

    let doc_id1 = DocID::new_v0(cid1);
    let doc_id2 = DocID::new_v0(cid2);

    assert_ne!(doc_id1.uuid(), doc_id2.uuid());
}

#[test]
fn test_string_roundtrip() {
    let cid = test_cid();
    let doc_id = DocID::new_v0(cid);
    let s = doc_id.to_string();

    let parsed = DocID::from_string(&s).unwrap();
    assert_eq!(doc_id.version(), parsed.version());
    assert_eq!(doc_id.uuid(), parsed.uuid());
    // Note: CID is not preserved in string roundtrip
    assert!(parsed.cid().is_none());
}

#[test]
fn test_bytes_roundtrip() {
    let cid = test_cid();
    let doc_id = DocID::new_v0(cid);
    let bytes = doc_id.to_bytes();

    let parsed = DocID::from_bytes(&bytes).unwrap();
    assert_eq!(doc_id.version(), parsed.version());
    assert_eq!(doc_id.uuid(), parsed.uuid());
}

#[test]
fn test_from_str_impl() {
    let cid = test_cid();
    let doc_id = DocID::new_v0(cid);
    let s = doc_id.to_string();

    let parsed: DocID = s.parse().unwrap();
    assert_eq!(doc_id.uuid(), parsed.uuid());
}

#[test]
fn test_invalid_string_no_separator() {
    let result = DocID::from_string("nodash");
    assert!(result.is_err());
}

#[test]
fn test_invalid_string_bad_uuid() {
    let result = DocID::from_string("bae-not-a-uuid");
    assert!(result.is_err());
}

// === Error path tests ===

#[test]
fn test_from_bytes_too_short_empty() {
    let result = DocID::from_bytes(&[]);
    assert!(result.is_err());
}

#[test]
fn test_from_bytes_too_short_partial() {
    // Only 10 bytes (need at least 17: 1 version + 16 uuid)
    let result = DocID::from_bytes(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert!(result.is_err());
}

#[test]
fn test_from_bytes_invalid_version() {
    // Version 0x02 is invalid (only 0x01 is valid)
    let mut bytes = vec![0x02];
    bytes.extend_from_slice(&[0x00; 16]); // 16 bytes for UUID
    let result = DocID::from_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_from_bytes_version_zero_invalid() {
    // Version 0x00 is also invalid
    let mut bytes = vec![0x00];
    bytes.extend_from_slice(&[0x00; 16]);
    let result = DocID::from_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_from_string_empty_version() {
    // Empty version part "-uuid-here"
    let result = DocID::from_string("-c94acbfa-dd53-40d0-97f3-29ce16c333fc");
    assert!(result.is_err());
}

#[test]
fn test_from_string_invalid_base32_version() {
    // Invalid base32 characters in version
    let result = DocID::from_string("!!!-c94acbfa-dd53-40d0-97f3-29ce16c333fc");
    assert!(result.is_err());
}
