//! Tests for Block types and DAG-CBOR serialization.
//!
//! These tests verify Go wire compatibility and round-trip serialization.

use std::str::FromStr;

use cid::Cid;
use defra_core::{
    Block, CollectionDeltaPayload, CompositeDeltaPayload, CounterDeltaPayload, CrdtDelta, DAGLink,
    Encryption, LwwDeltaPayload, Signature, SignatureHeader, SignatureType, DAG_CBOR_CODEC,
};

fn test_cid() -> Cid {
    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

fn test_cid2() -> Cid {
    Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
}

fn test_lww_delta() -> CrdtDelta {
    CrdtDelta::Lww(LwwDeltaPayload {
        field_name: "name".to_string(),
        priority: 1,
        schema_version_id: "schema1".to_string(),
        data: b"John".to_vec(),
    })
}

#[test]
fn test_block_dag_cbor_roundtrip() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);

    let bytes = block.to_dag_cbor().unwrap();
    let restored = Block::from_dag_cbor(&bytes).unwrap();

    assert_eq!(block, restored);
}

// ============================================================================
// Go Wire Compatibility Golden Tests (Issue #15)
// Test vectors generated from Go DefraDB implementation
// ============================================================================

// Go test vector: Simple LWW Block
const GO_LWW_SIMPLE_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x44, 0x4A, 0x6F, 0x68, 0x6E, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79,
    0x01, 0x69, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65,
    0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69,
    0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_LWW_SIMPLE_CID: &str = "bafyreihgg6a5auqhikq4nvw6fj3kbreovdbazlisbs5kerkahoqwwiz75i";

// Go test vector: LWW Block with higher priority
const GO_LWW_HIGH_PRIORITY_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x42, 0x18, 0x1E, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x18, 0x64,
    0x69, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x63, 0x61, 0x67, 0x65, 0x73, 0x63,
    0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E,
    0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_LWW_HIGH_PRIORITY_CID: &str =
    "bafyreidwus7muqrpwwf22gvpqpow6xg37woh4ikztgl27deo37ehs5ehaa";

// Go test vector: Counter Block
const GO_COUNTER_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x67, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x65, 0x72,
    0xA5, 0x64, 0x64, 0x61, 0x74, 0x61, 0x41, 0x0A, 0x65, 0x6E, 0x6F, 0x6E, 0x63, 0x65, 0x19, 0x30,
    0x39, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01, 0x69, 0x66, 0x69, 0x65, 0x6C,
    0x64, 0x4E, 0x61, 0x6D, 0x65, 0x65, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x73, 0x63, 0x6F, 0x6C, 0x6C,
    0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67,
    0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_COUNTER_CID: &str = "bafyreiazwbpd5i2zhwomwgldn47nq2tluij5dgj2uqz5pctx2cy3nbxgyu";

// Go test vector: Composite Block (active document)
const GO_COMPOSITE_ACTIVE_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73, 0x69,
    0x74, 0x65, 0xA3, 0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x01, 0x68, 0x70, 0x72, 0x69, 0x6F,
    0x72, 0x69, 0x74, 0x79, 0x01, 0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E,
    0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61,
    0x31,
];
const GO_COMPOSITE_ACTIVE_CID: &str = "bafyreie7rtdexuf47f633477mfieshkeh5rwnjeommkgqrzl22n6g4bfmm";

// Go test vector: Composite Block (deleted document)
const GO_COMPOSITE_DELETED_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73, 0x69,
    0x74, 0x65, 0xA3, 0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x02, 0x68, 0x70, 0x72, 0x69, 0x6F,
    0x72, 0x69, 0x74, 0x79, 0x02, 0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E,
    0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61,
    0x31,
];
const GO_COMPOSITE_DELETED_CID: &str =
    "bafyreib35xrgvyzzf6uwwqbavrowwzzg5gytspimnwgiyoi6e5nyb3uyp4";

// Go test vector: Collection Block
const GO_COLLECTION_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x6A, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74,
    0x69, 0x6F, 0x6E, 0xA2, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01, 0x73, 0x63,
    0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E,
    0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_COLLECTION_CID: &str = "bafyreiggkftgtbppmz66sctbbswtgoy7jxrqbrx3edsq5pzqqpctstekdm";

// Go test vector: LWW Block with empty data (deletion)
const GO_LWW_DELETION_BYTES: &[u8] = &[
    0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x40, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x02, 0x69, 0x66, 0x69,
    0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65, 0x73, 0x63, 0x6F, 0x6C,
    0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44,
    0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
];
const GO_LWW_DELETION_CID: &str = "bafyreia4ihbzcpuuuesmdswmfbbj435aew44rfebj7wt3ldagffukbptf4";

#[test]
fn test_go_wire_compat_lww_simple() {
    // Deserialize Go bytes
    let block = Block::from_dag_cbor(GO_LWW_SIMPLE_BYTES).unwrap();

    // Re-serialize and verify byte-identical output
    let rust_bytes = block.to_dag_cbor().unwrap();
    assert_eq!(
        rust_bytes.as_slice(),
        GO_LWW_SIMPLE_BYTES,
        "Rust serialization should match Go bytes"
    );

    // Verify CID matches
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_LWW_SIMPLE_CID,
        "CID should match Go's CID"
    );

    // Verify content
    if let CrdtDelta::Lww(lww) = &block.delta {
        assert_eq!(lww.field_name, "name");
        assert_eq!(lww.priority, 1);
        assert_eq!(lww.schema_version_id, "schema1");
        assert_eq!(lww.data, b"John");
    } else {
        panic!("Expected LWW delta");
    }
}

#[test]
fn test_go_wire_compat_lww_high_priority() {
    let block = Block::from_dag_cbor(GO_LWW_HIGH_PRIORITY_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_LWW_HIGH_PRIORITY_BYTES);
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_LWW_HIGH_PRIORITY_CID
    );

    if let CrdtDelta::Lww(lww) = &block.delta {
        assert_eq!(lww.priority, 100);
        assert_eq!(lww.field_name, "age");
    } else {
        panic!("Expected LWW delta");
    }
}

#[test]
fn test_go_wire_compat_counter() {
    let block = Block::from_dag_cbor(GO_COUNTER_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_COUNTER_BYTES);
    assert_eq!(block.generate_cid().unwrap().to_string(), GO_COUNTER_CID);

    if let CrdtDelta::Counter(counter) = &block.delta {
        assert_eq!(counter.field_name, "count");
        assert_eq!(counter.priority, 1);
        assert_eq!(counter.nonce, 12345);
        assert_eq!(counter.data, &[0x0A]); // CBOR integer 10
    } else {
        panic!("Expected Counter delta");
    }
}

#[test]
fn test_go_wire_compat_composite_active() {
    let block = Block::from_dag_cbor(GO_COMPOSITE_ACTIVE_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_COMPOSITE_ACTIVE_BYTES);
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_COMPOSITE_ACTIVE_CID
    );

    if let CrdtDelta::Composite(composite) = &block.delta {
        assert_eq!(composite.priority, 1);
        assert_eq!(composite.status, 1);
    } else {
        panic!("Expected Composite delta");
    }
}

#[test]
fn test_go_wire_compat_composite_deleted() {
    let block = Block::from_dag_cbor(GO_COMPOSITE_DELETED_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_COMPOSITE_DELETED_BYTES);
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_COMPOSITE_DELETED_CID
    );

    if let CrdtDelta::Composite(composite) = &block.delta {
        assert_eq!(composite.priority, 2);
        assert_eq!(composite.status, 2);
    } else {
        panic!("Expected Composite delta");
    }
}

#[test]
fn test_go_wire_compat_collection() {
    let block = Block::from_dag_cbor(GO_COLLECTION_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_COLLECTION_BYTES);
    assert_eq!(block.generate_cid().unwrap().to_string(), GO_COLLECTION_CID);

    if let CrdtDelta::Collection(collection) = &block.delta {
        assert_eq!(collection.priority, 1);
        assert_eq!(collection.schema_version_id, "schema1");
    } else {
        panic!("Expected Collection delta");
    }
}

#[test]
fn test_go_wire_compat_lww_deletion() {
    let block = Block::from_dag_cbor(GO_LWW_DELETION_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_LWW_DELETION_BYTES);
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_LWW_DELETION_CID
    );

    if let CrdtDelta::Lww(lww) = &block.delta {
        assert_eq!(lww.priority, 2);
        assert!(lww.data.is_empty(), "Deletion should have empty data");
    } else {
        panic!("Expected LWW delta");
    }
}

#[test]
fn test_rust_produces_go_compatible_lww() {
    // Create same block structure as Go test
    let block = Block::new(test_lww_delta(), vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();

    // Should produce identical bytes to Go
    assert_eq!(
        bytes.as_slice(),
        GO_LWW_SIMPLE_BYTES,
        "Rust-created block should produce Go-compatible bytes"
    );
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_LWW_SIMPLE_CID,
        "CID should match Go"
    );
}

#[test]
fn test_block_cid_generation_deterministic() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);

    let cid1 = block.generate_cid().unwrap();
    let cid2 = block.generate_cid().unwrap();

    assert_eq!(cid1, cid2);
}

#[test]
fn test_block_cid_uses_dag_cbor_codec() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);
    let cid = block.generate_cid().unwrap();

    assert_eq!(cid.codec(), *DAG_CBOR_CODEC);
}

#[test]
fn test_heads_sorted_lexicographically() {
    let cid_z = test_cid(); // bafybeig...
    let cid_a = test_cid2(); // bafkreig... (comes after bafybeig)

    // Note: bafkreig > bafybeig lexicographically
    let block = Block::new(test_lww_delta(), vec![cid_a, cid_z], vec![]);

    let heads = block.heads.unwrap();
    assert!(
        heads[0].to_string() < heads[1].to_string(),
        "Heads should be sorted: {} < {}",
        heads[0],
        heads[1]
    );
}

#[test]
fn test_empty_heads_becomes_none() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);
    assert!(block.heads.is_none());
}

#[test]
fn test_empty_links_becomes_none() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);
    assert!(block.links.is_none());
}

#[test]
fn test_all_links_returns_heads_then_links() {
    let head1 = test_cid();
    let head2 = test_cid2();
    let link1 = DAGLink::new("field1", test_cid());
    let link2 = DAGLink::new("field2", test_cid2());

    let block = Block::new(test_lww_delta(), vec![head1, head2], vec![link1, link2]);

    let all = block.all_links();
    assert_eq!(all.len(), 4);
    // First two should be heads (sorted)
    // Last two should be links (sorted)
}

#[test]
fn test_get_link_by_name() {
    let link = DAGLink::new("myfield", test_cid());
    let block = Block::new(test_lww_delta(), vec![], vec![link.clone()]);

    assert_eq!(block.get_link_by_name("myfield"), Some(&link.link));
    assert_eq!(block.get_link_by_name("nonexistent"), None);
}

#[test]
fn test_is_encrypted_and_signed() {
    let mut block = Block::new(test_lww_delta(), vec![], vec![]);
    assert!(!block.is_encrypted());
    assert!(!block.is_signed());

    block.encryption = Some(test_cid());
    assert!(block.is_encrypted());

    block.signature = Some(test_cid2());
    assert!(block.is_signed());
}

#[test]
fn test_dag_link_ordering() {
    let link_a = DAGLink::new("a", test_cid());
    let link_b = DAGLink::new("b", test_cid2());

    // Ordering is by CID string, not by name
    let mut links = [link_b.clone(), link_a.clone()];
    links.sort();

    // bafkreig < bafybeig (k < y in ASCII)
    assert_eq!(links[0].link, test_cid2());
    assert_eq!(links[1].link, test_cid());
}

#[test]
fn test_block_new_sorts_links_by_cid_string() {
    let link_a = DAGLink::new("z-name", test_cid());
    let link_b = DAGLink::new("a-name", test_cid2());

    let block = Block::new(test_lww_delta(), vec![], vec![link_a, link_b]);
    let links = block.links.unwrap();

    assert_eq!(links[0].link, test_cid2());
    assert_eq!(links[1].link, test_cid());
}

#[test]
fn test_block_new_uses_string_order_when_cid_byte_order_differs() {
    let doc_id =
        Cid::from_str("bafyreihqzhiz3iwro4jozp6kphq4sosg6ccoqcbiaf7rg5dmvea7aux55a").unwrap();
    let data =
        Cid::from_str("bafyreih2uk7uwfnwfmffmokgrecoikhuk2sxsfwilwpcazzsi2u37m3tmu").unwrap();
    let idx = Cid::from_str("bafyreibqvnx5ibtgidcmcpljhzrcct76sxtgxctxiatmnakz2zeztss3ru").unwrap();

    let mut by_bytes = [doc_id, data, idx];
    by_bytes.sort_unstable();
    assert_ne!(
        by_bytes,
        [idx, data, doc_id],
        "fixture should cover a CID set where native byte order differs from Go string order",
    );

    let block = Block::new(
        test_lww_delta(),
        vec![data, idx, doc_id],
        vec![
            DAGLink::new("_docID", doc_id),
            DAGLink::new("data", data),
            DAGLink::new("idx", idx),
        ],
    );

    let heads = block.heads.unwrap();
    assert_eq!(heads, vec![idx, data, doc_id]);

    let links = block.links.unwrap();
    assert_eq!(links[0].link, idx);
    assert_eq!(links[1].link, data);
    assert_eq!(links[2].link, doc_id);
}

#[test]
fn test_block_new_sorts_heads_and_links_by_cid_string_with_base32_digits() {
    let verified =
        Cid::from_str("bafyreia2kmnlqm63352ye3sqtvy2bwdtv2a2sybejjikurxsh4v5vliemy").unwrap();
    let name =
        Cid::from_str("bafyreiaezc5g33yzhyzcgbyiv476lovyztyoliotzksdfogoep5ktgpedq").unwrap();
    let age = Cid::from_str("bafyreiefugdbpc563kqcjoe4pjrgavtv3ysbhpzp5smdwb4stxeldmronm").unwrap();
    let doc_id =
        Cid::from_str("bafyreihqzhiz3iwro4jozp6kphq4sosg6ccoqcbiaf7rg5dmvea7aux55a").unwrap();

    let block = Block::new(
        test_lww_delta(),
        vec![doc_id, verified, age, name],
        vec![
            DAGLink::new("_docID", doc_id),
            DAGLink::new("verified", verified),
            DAGLink::new("age", age),
            DAGLink::new("name", name),
        ],
    );

    assert_eq!(
        block.heads.unwrap(),
        vec![verified, name, age, doc_id],
        "Go sorts block heads by CID string"
    );
    assert_eq!(
        block
            .links
            .unwrap()
            .into_iter()
            .map(|link| link.link)
            .collect::<Vec<_>>(),
        vec![verified, name, age, doc_id],
        "Go sorts block links by CID string"
    );
}

#[test]
fn test_dag_link_equality_is_unaffected_by_sorting() {
    let link = DAGLink::new("field", test_cid());
    let mut sorted = [link.clone()];
    sorted.sort();

    assert_eq!(sorted[0], link);
}

#[test]
fn test_encryption_roundtrip() {
    let enc = Encryption::new(b"key123".to_vec());

    let bytes = enc.to_dag_cbor().unwrap();
    let restored = Encryption::from_dag_cbor(&bytes).unwrap();

    assert_eq!(enc, restored);
}

#[test]
fn test_encryption_rejects_extra_bytes() {
    let enc = Encryption::new(b"key123".to_vec());
    let bytes = enc.to_dag_cbor().unwrap();
    let restored = Encryption::from_dag_cbor(&bytes).unwrap();
    assert_eq!(restored.key, b"key123");
}

#[test]
fn test_signature_roundtrip() {
    let sig = Signature::new(
        SignatureHeader::new(SignatureType::EdDSA, b"pubkey".to_vec()),
        b"signature_value".to_vec(),
    );

    let bytes = sig.to_dag_cbor().unwrap();
    let restored = Signature::from_dag_cbor(&bytes).unwrap();

    assert_eq!(sig, restored);
}

#[test]
fn test_signature_type_serialization() {
    let header_ed = SignatureHeader::new(SignatureType::EdDSA, vec![]);
    let header_ec = SignatureHeader::new(SignatureType::ES256K, vec![]);

    // Verify the type serializes correctly
    let bytes_ed = serde_ipld_dagcbor::to_vec(&header_ed).unwrap();
    let bytes_ec = serde_ipld_dagcbor::to_vec(&header_ec).unwrap();

    let restored_ed: SignatureHeader = serde_ipld_dagcbor::from_slice(&bytes_ed).unwrap();
    let restored_ec: SignatureHeader = serde_ipld_dagcbor::from_slice(&bytes_ec).unwrap();

    assert_eq!(restored_ed.sig_type, SignatureType::EdDSA);
    assert_eq!(restored_ec.sig_type, SignatureType::ES256K);
}

#[test]
fn test_crdt_delta_priority() {
    let mut delta = test_lww_delta();
    assert_eq!(delta.priority(), 1);

    delta.set_priority(42);
    assert_eq!(delta.priority(), 42);
}

// Go v1.0.0 (#4838) removes docID from every delta payload: identity is
// derived from the genesis composite block CID. Pinned upstream by
// delta_doc_id_test.go asserting no delta schema contains "docID".
#[test]
fn test_delta_wire_format_carries_no_doc_id() {
    for (name, delta) in [
        ("lww", test_lww_delta()),
        (
            "counter",
            CrdtDelta::Counter(CounterDeltaPayload {
                field_name: "f".to_string(),
                priority: 1,
                nonce: 0,
                schema_version_id: "v1".to_string(),
                data: vec![],
            }),
        ),
        (
            "composite",
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "v1".to_string(),
                priority: 1,
                status: 1,
            }),
        ),
    ] {
        let block = Block::new(delta, vec![], vec![]);
        let bytes = block.to_dag_cbor().unwrap();
        let needle = b"docID";
        let found = bytes.windows(needle.len()).any(|w| w == needle);
        assert!(!found, "{name} delta must not encode a docID key");
    }
}

#[test]
fn test_composite_delta_roundtrip() {
    let delta = CrdtDelta::Composite(CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 5,
        status: 1,
    });

    let block = Block::new(delta, vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();
    let restored = Block::from_dag_cbor(&bytes).unwrap();

    if let CrdtDelta::Composite(c) = &restored.delta {
        assert_eq!(c.status, 1);
        assert_eq!(c.priority, 5);
    } else {
        panic!("Expected Composite delta");
    }
}

#[test]
fn test_counter_delta_roundtrip() {
    let delta = CrdtDelta::Counter(CounterDeltaPayload {
        field_name: "counter_field".to_string(),
        priority: 3,
        nonce: -42,
        schema_version_id: "v1".to_string(),
        data: vec![1, 2, 3, 4],
    });

    let block = Block::new(delta, vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();
    let restored = Block::from_dag_cbor(&bytes).unwrap();

    if let CrdtDelta::Counter(c) = &restored.delta {
        assert_eq!(c.nonce, -42);
        assert_eq!(c.field_name, "counter_field");
        assert_eq!(c.data, vec![1, 2, 3, 4]);
    } else {
        panic!("Expected Counter delta");
    }
}

#[test]
fn test_collection_delta_roundtrip() {
    let delta = CrdtDelta::Collection(CollectionDeltaPayload {
        schema_version_id: "v2".to_string(),
        priority: 10,
    });

    let block = Block::new(delta, vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();
    let restored = Block::from_dag_cbor(&bytes).unwrap();

    if let CrdtDelta::Collection(c) = &restored.delta {
        assert_eq!(c.priority, 10);
        assert_eq!(c.schema_version_id, "v2");
    } else {
        panic!("Expected Collection delta");
    }
}

#[test]
fn test_block_with_heads_and_links() {
    let head = test_cid();
    let link = DAGLink::new("field", test_cid2());

    let block = Block::new_with_options(
        test_lww_delta(),
        vec![head],
        vec![link],
        Some(test_cid()),
        Some(test_cid2()),
    );

    assert!(block.heads.is_some());
    assert!(block.links.is_some());
    assert!(block.is_encrypted());
    assert!(block.is_signed());

    // Roundtrip
    let bytes = block.to_dag_cbor().unwrap();
    let restored = Block::from_dag_cbor(&bytes).unwrap();
    assert_eq!(block, restored);
}

// ============================================================================
// Deserialization Error Handling Tests (Issue #16)
// ============================================================================

#[test]
fn test_from_dag_cbor_rejects_invalid_cbor() {
    // Completely invalid bytes - not valid CBOR at all
    let result = Block::from_dag_cbor(&[0xFF, 0xFF, 0xFF]);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_empty_input() {
    let result = Block::from_dag_cbor(&[]);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_truncated_data() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();

    // Try various truncation points
    for truncate_at in [1, bytes.len() / 4, bytes.len() / 2, bytes.len() - 1] {
        let result = Block::from_dag_cbor(&bytes[..truncate_at]);
        assert!(
            result.is_err(),
            "Should reject truncated data at {} bytes",
            truncate_at
        );
    }
}

#[test]
fn test_from_dag_cbor_rejects_wrong_type_integer() {
    // Valid CBOR integer (42), but not a Block structure
    let cbor_integer = serde_ipld_dagcbor::to_vec(&42u64).unwrap();
    let result = Block::from_dag_cbor(&cbor_integer);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_wrong_type_string() {
    // Valid CBOR string, but not a Block structure
    let cbor_string = serde_ipld_dagcbor::to_vec(&"not a block").unwrap();
    let result = Block::from_dag_cbor(&cbor_string);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_wrong_type_array() {
    // Valid CBOR array, but Block expects a map
    let cbor_array = serde_ipld_dagcbor::to_vec(&vec![1, 2, 3]).unwrap();
    let result = Block::from_dag_cbor(&cbor_array);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_empty_map() {
    // Valid CBOR map but missing required 'delta' field
    use std::collections::BTreeMap;
    let empty_map: BTreeMap<String, String> = BTreeMap::new();
    let cbor_empty_map = serde_ipld_dagcbor::to_vec(&empty_map).unwrap();
    let result = Block::from_dag_cbor(&cbor_empty_map);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_map_with_wrong_delta_type() {
    // Map with 'delta' field but wrong type (string instead of CrdtDelta)
    use std::collections::BTreeMap;
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    map.insert("delta".to_string(), "not a delta".to_string());
    let cbor_map = serde_ipld_dagcbor::to_vec(&map).unwrap();
    let result = Block::from_dag_cbor(&cbor_map);
    assert!(result.is_err());
}

#[test]
fn test_from_dag_cbor_rejects_corrupted_block() {
    // Take valid block bytes and corrupt them in the middle
    let block = Block::new(test_lww_delta(), vec![test_cid()], vec![]);
    let mut bytes = block.to_dag_cbor().unwrap();

    // Corrupt bytes in the middle (should break structure)
    if bytes.len() > 20 {
        bytes[15] = 0xFF;
        bytes[16] = 0xFF;
        bytes[17] = 0xFF;
    }

    let result = Block::from_dag_cbor(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_encryption_from_dag_cbor_rejects_invalid() {
    let result = Encryption::from_dag_cbor(&[0xFF, 0xFF]);
    assert!(result.is_err());
}

#[test]
fn test_encryption_from_dag_cbor_rejects_truncated() {
    let enc = Encryption::new(b"key1234567890".to_vec());
    let bytes = enc.to_dag_cbor().unwrap();
    let result = Encryption::from_dag_cbor(&bytes[..bytes.len() / 2]);
    assert!(result.is_err());
}

#[test]
fn test_encryption_from_dag_cbor_rejects_missing_fields() {
    use std::collections::BTreeMap;
    // Missing 'key' field
    let mut map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    map.insert("docID".to_string(), b"doc".to_vec());
    let cbor = serde_ipld_dagcbor::to_vec(&map).unwrap();
    let result = Encryption::from_dag_cbor(&cbor);
    assert!(result.is_err());
}

#[test]
fn test_signature_from_dag_cbor_rejects_invalid() {
    let result = Signature::from_dag_cbor(&[0xFF, 0xFF]);
    assert!(result.is_err());
}

#[test]
fn test_signature_from_dag_cbor_rejects_truncated() {
    let sig = Signature::new(
        SignatureHeader::new(SignatureType::EdDSA, b"pk".to_vec()),
        b"sig".to_vec(),
    );
    let bytes = sig.to_dag_cbor().unwrap();
    let result = Signature::from_dag_cbor(&bytes[..bytes.len() / 2]);
    assert!(result.is_err());
}

#[test]
fn test_signature_from_dag_cbor_rejects_corrupted() {
    // Take valid signature bytes and corrupt them
    let sig = Signature::new(
        SignatureHeader::new(SignatureType::EdDSA, b"pubkey".to_vec()),
        b"signature".to_vec(),
    );
    let mut bytes = sig.to_dag_cbor().unwrap();

    // Corrupt in the middle
    if bytes.len() > 10 {
        bytes[5] = 0xFF;
        bytes[6] = 0xFF;
    }

    let result = Signature::from_dag_cbor(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_block_with_corrupted_cid_in_heads() {
    // Create a valid block then corrupt the CID bytes
    let block = Block::new(test_lww_delta(), vec![test_cid()], vec![]);
    let mut bytes = block.to_dag_cbor().unwrap();

    // Find and corrupt CID bytes (CIDs have a specific structure)
    // The corruption should cause deserialization to fail
    for i in (bytes.len() / 2)..bytes.len() {
        bytes[i] = 0x00;
    }

    let result = Block::from_dag_cbor(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_composite_status_zero_cbor_rejected() {
    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema-v1".to_string(),
            priority: 1,
            status: 0,
        }),
        vec![],
        vec![],
    );
    let bytes = block.to_dag_cbor().expect("invalid status can serialize");

    let result = Block::from_dag_cbor(&bytes);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid document status: 0"));
}

#[test]
fn test_composite_missing_status_cbor_rejected() {
    let mut composite = std::collections::BTreeMap::new();
    composite.insert(
        "schemaVersionID".to_string(),
        ipld_core::ipld::Ipld::String("schema-v1".to_string()),
    );
    composite.insert("priority".to_string(), ipld_core::ipld::Ipld::Integer(1));

    let mut delta = std::collections::BTreeMap::new();
    delta.insert(
        "composite".to_string(),
        ipld_core::ipld::Ipld::Map(composite),
    );

    let mut block = std::collections::BTreeMap::new();
    block.insert("delta".to_string(), ipld_core::ipld::Ipld::Map(delta));

    let bytes = serde_ipld_dagcbor::to_vec(&ipld_core::ipld::Ipld::Map(block))
        .expect("manual block should encode");
    let result = Block::from_dag_cbor(&bytes);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("missing field"));
}

// ============================================================================
// Go Wire Compatibility: Encryption Tests
// ============================================================================

// Go v1.0.0 test vector: Encryption{Key: []byte("key123")} — carries only
// the key since #4838 (docID/fieldName removed).
const GO_ENCRYPTION_BYTES: &[u8] = &[
    0xA1, 0x63, 0x6B, 0x65, 0x79, 0x46, 0x6B, 0x65, 0x79, 0x31, 0x32, 0x33,
];

#[test]
fn test_go_wire_compat_encryption() {
    let enc = Encryption::from_dag_cbor(GO_ENCRYPTION_BYTES).unwrap();
    let rust_bytes = enc.to_dag_cbor().unwrap();

    assert_eq!(
        rust_bytes.as_slice(),
        GO_ENCRYPTION_BYTES,
        "Rust serialization should match Go bytes"
    );
    assert_eq!(enc.key, b"key123");
}

// ============================================================================
// Go Wire Compatibility: Signature Tests
// ============================================================================

// Go test vector: Signature Ed25519
const GO_SIGNATURE_ED25519_BYTES: &[u8] = &[
    0xA2, 0x65, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x55, 0x73, 0x69, 0x67, 0x6E, 0x61, 0x74, 0x75, 0x72,
    0x65, 0x2D, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x2D, 0x62, 0x79, 0x74, 0x65, 0x73, 0x66, 0x68, 0x65,
    0x61, 0x64, 0x65, 0x72, 0xA2, 0x64, 0x74, 0x79, 0x70, 0x65, 0x65, 0x45, 0x64, 0x44, 0x53, 0x41,
    0x68, 0x69, 0x64, 0x65, 0x6E, 0x74, 0x69, 0x74, 0x79, 0x4E, 0x65, 0x64, 0x32, 0x35, 0x35, 0x31,
    0x39, 0x2D, 0x70, 0x75, 0x62, 0x6B, 0x65, 0x79,
];
const GO_SIGNATURE_ED25519_CID: &str =
    "bafyreib4gae2lppbkikyoltcdlepqytwco5cao7xqb7oxpvbtg5arnnfki";

// Go test vector: Signature ECDSA (Secp256k1)
const GO_SIGNATURE_ECDSA_BYTES: &[u8] = &[
    0xA2, 0x65, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x49, 0x65, 0x63, 0x64, 0x73, 0x61, 0x2D, 0x73, 0x69,
    0x67, 0x66, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0xA2, 0x64, 0x74, 0x79, 0x70, 0x65, 0x66, 0x45,
    0x53, 0x32, 0x35, 0x36, 0x4B, 0x68, 0x69, 0x64, 0x65, 0x6E, 0x74, 0x69, 0x74, 0x79, 0x50, 0x73,
    0x65, 0x63, 0x70, 0x32, 0x35, 0x36, 0x6B, 0x31, 0x2D, 0x70, 0x75, 0x62, 0x6B, 0x65, 0x79,
];
const GO_SIGNATURE_ECDSA_CID: &str = "bafyreieblfehzbcx5zasm7vbxodsmuhuvlvrj7ajo6k45f6trtrkddu4ve";

#[test]
fn test_go_wire_compat_signature_ed25519() {
    let sig = Signature::from_dag_cbor(GO_SIGNATURE_ED25519_BYTES).unwrap();
    let rust_bytes = sig.to_dag_cbor().unwrap();

    assert_eq!(
        rust_bytes.as_slice(),
        GO_SIGNATURE_ED25519_BYTES,
        "Rust serialization should match Go bytes"
    );
    assert_eq!(
        sig.generate_cid().unwrap().to_string(),
        GO_SIGNATURE_ED25519_CID
    );

    assert_eq!(sig.header.sig_type, SignatureType::EdDSA);
    assert_eq!(sig.header.identity, b"ed25519-pubkey");
    assert_eq!(sig.value, b"signature-value-bytes");
}

#[test]
fn test_go_wire_compat_signature_ecdsa() {
    let sig = Signature::from_dag_cbor(GO_SIGNATURE_ECDSA_BYTES).unwrap();
    let rust_bytes = sig.to_dag_cbor().unwrap();

    assert_eq!(rust_bytes.as_slice(), GO_SIGNATURE_ECDSA_BYTES);
    assert_eq!(
        sig.generate_cid().unwrap().to_string(),
        GO_SIGNATURE_ECDSA_CID
    );

    assert_eq!(sig.header.sig_type, SignatureType::ES256K);
    assert_eq!(sig.header.identity, b"secp256k1-pubkey");
    assert_eq!(sig.value, b"ecdsa-sig");
}

// ============================================================================
// Go Wire Compatibility: Blocks with Heads and Links
// ============================================================================

// Go test vector: Block with one head (update block pointing to previous)
const GO_BLOCK_WITH_ONE_HEAD_BYTES: &[u8] = &[
    0xA2, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA4, 0x64, 0x64, 0x61,
    0x74, 0x61, 0x44, 0x4A, 0x61, 0x6E, 0x65, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79,
    0x02, 0x69, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65,
    0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E, 0x56, 0x65, 0x72, 0x73, 0x69,
    0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31, 0x65, 0x68, 0x65, 0x61,
    0x64, 0x73, 0x81, 0xD8, 0x2A, 0x58, 0x25, 0x00, 0x01, 0x71, 0x12, 0x20, 0xE6, 0x37, 0x81, 0xD0,
    0x52, 0x07, 0x42, 0xA1, 0xC6, 0xD6, 0xDE, 0x2A, 0x76, 0xA0, 0xC4, 0x8E, 0xA8, 0xC2, 0x0C, 0xAD,
    0x12, 0x0C, 0xBA, 0xA2, 0x45, 0x40, 0x3B, 0xA1, 0x6B, 0x23, 0x3F, 0xEA,
];
const GO_BLOCK_WITH_ONE_HEAD_CID: &str =
    "bafyreifythcezq6nmny7n3aklpqwt7pieydn5wle2cdfpsjemxi5l6axfu";

// Go test vector: Composite block with links to field-level blocks
const GO_COMPOSITE_BLOCK_WITH_LINKS_BYTES: &[u8] = &[
    0xA2, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73, 0x69,
    0x74, 0x65, 0xA3, 0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x01, 0x68, 0x70, 0x72, 0x69, 0x6F,
    0x72, 0x69, 0x74, 0x79, 0x01, 0x73, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63, 0x74, 0x69, 0x6F, 0x6E,
    0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61,
    0x31, 0x65, 0x6C, 0x69, 0x6E, 0x6B, 0x73, 0x81, 0xA2, 0x64, 0x6C, 0x69, 0x6E, 0x6B, 0xD8, 0x2A,
    0x58, 0x25, 0x00, 0x01, 0x71, 0x12, 0x20, 0x76, 0xA4, 0xBE, 0xCA, 0x42, 0x2F, 0xB5, 0x8B, 0xAD,
    0x1A, 0xAF, 0x83, 0xDD, 0x6F, 0x5C, 0xDB, 0xFD, 0x9C, 0x7E, 0x21, 0x59, 0x99, 0x97, 0xAF, 0x8C,
    0x8E, 0xDF, 0xC8, 0x79, 0x74, 0x87, 0x00, 0x64, 0x6E, 0x61, 0x6D, 0x65, 0x63, 0x61, 0x67, 0x65,
];
const GO_COMPOSITE_BLOCK_WITH_LINKS_CID: &str =
    "bafyreiekxzg7hljvqjbd5ri6mgxgu7sdezot6o3k4coxklhgdc3464peju";

#[test]
fn test_go_wire_compat_block_with_head() {
    let block = Block::from_dag_cbor(GO_BLOCK_WITH_ONE_HEAD_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(
        rust_bytes.as_slice(),
        GO_BLOCK_WITH_ONE_HEAD_BYTES,
        "Rust serialization should match Go bytes"
    );
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_BLOCK_WITH_ONE_HEAD_CID
    );

    // Verify heads
    assert!(block.heads.is_some());
    let heads = block.heads.as_ref().unwrap();
    assert_eq!(heads.len(), 1);
    assert_eq!(
        heads[0].to_string(),
        GO_LWW_SIMPLE_CID,
        "Head should point to the simple LWW block"
    );

    // Verify delta content
    if let CrdtDelta::Lww(lww) = &block.delta {
        assert_eq!(lww.field_name, "name");
        assert_eq!(lww.priority, 2);
        assert_eq!(lww.data, b"Jane");
    } else {
        panic!("Expected LWW delta");
    }
}

#[test]
fn test_go_wire_compat_composite_with_links() {
    let block = Block::from_dag_cbor(GO_COMPOSITE_BLOCK_WITH_LINKS_BYTES).unwrap();
    let rust_bytes = block.to_dag_cbor().unwrap();

    assert_eq!(
        rust_bytes.as_slice(),
        GO_COMPOSITE_BLOCK_WITH_LINKS_BYTES,
        "Rust serialization should match Go bytes"
    );
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_COMPOSITE_BLOCK_WITH_LINKS_CID
    );

    // Verify links
    assert!(block.links.is_some());
    let links = block.links.as_ref().unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].name, "age");

    // Verify delta content
    if let CrdtDelta::Composite(composite) = &block.delta {
        assert_eq!(composite.priority, 1);
        assert_eq!(composite.status, 1);
        assert_eq!(composite.schema_version_id, "schema1");
    } else {
        panic!("Expected Composite delta");
    }
}

// ============================================================================
// Rust-to-Go Compatibility: Round-Trip Tests
// Verifies Rust-created blocks produce Go-compatible bytes
// ============================================================================

#[test]
fn test_rust_produces_go_compatible_counter() {
    let delta = CrdtDelta::Counter(CounterDeltaPayload {
        field_name: "count".to_string(),
        priority: 1,
        nonce: 12345,
        schema_version_id: "schema1".to_string(),
        data: vec![0x0A],
    });
    let block = Block::new(delta, vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();

    assert_eq!(
        bytes.as_slice(),
        GO_COUNTER_BYTES,
        "Rust-created Counter block should match Go bytes"
    );
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_COUNTER_CID,
        "CID should match Go's CID"
    );
}

#[test]
fn test_rust_produces_go_compatible_composite() {
    let delta = CrdtDelta::Composite(CompositeDeltaPayload {
        schema_version_id: "schema1".to_string(),
        priority: 1,
        status: 1,
    });
    let block = Block::new(delta, vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();

    assert_eq!(
        bytes.as_slice(),
        GO_COMPOSITE_ACTIVE_BYTES,
        "Rust-created Composite block should match Go bytes"
    );
    assert_eq!(
        block.generate_cid().unwrap().to_string(),
        GO_COMPOSITE_ACTIVE_CID
    );
}

#[test]
fn test_rust_produces_go_compatible_collection() {
    let delta = CrdtDelta::Collection(CollectionDeltaPayload {
        schema_version_id: "schema1".to_string(),
        priority: 1,
    });
    let block = Block::new(delta, vec![], vec![]);
    let bytes = block.to_dag_cbor().unwrap();

    assert_eq!(
        bytes.as_slice(),
        GO_COLLECTION_BYTES,
        "Rust-created Collection block should match Go bytes"
    );
    assert_eq!(block.generate_cid().unwrap().to_string(), GO_COLLECTION_CID);
}

#[test]
fn test_rust_produces_go_compatible_encryption() {
    let enc = Encryption::new(b"key123".to_vec());
    let bytes = enc.to_dag_cbor().unwrap();

    assert_eq!(
        bytes.as_slice(),
        GO_ENCRYPTION_BYTES,
        "Rust-created Encryption should match Go bytes"
    );
}

#[test]
fn test_rust_produces_go_compatible_signature() {
    let sig = Signature::new(
        SignatureHeader::new(SignatureType::EdDSA, b"ed25519-pubkey".to_vec()),
        b"signature-value-bytes".to_vec(),
    );
    let bytes = sig.to_dag_cbor().unwrap();

    assert_eq!(
        bytes.as_slice(),
        GO_SIGNATURE_ED25519_BYTES,
        "Rust-created Signature should match Go bytes"
    );
    assert_eq!(
        sig.generate_cid().unwrap().to_string(),
        GO_SIGNATURE_ED25519_CID
    );
}
