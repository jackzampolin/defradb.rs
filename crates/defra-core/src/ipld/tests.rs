//! Tests for IPLD conversions.

use std::collections::BTreeMap;
use std::str::FromStr;

use libipld::Ipld;

use super::cid_convert::{cid_from_libipld, cid_to_libipld};
use super::traversal::{collect_block_links, extract_links, walk_ipld, IpldVisitor};
use crate::block::{
    Block, CollectionDefinitionDeltaPayload, CollectionDeltaPayload, CompositeDeltaPayload,
    CounterDeltaPayload, CrdtDelta, DAGLink, Encryption, FieldDefinitionDeltaPayload,
    LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
};

fn test_cid() -> cid::Cid {
    cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

fn test_lww_delta() -> CrdtDelta {
    CrdtDelta::Lww(LwwDeltaPayload {
        doc_id: b"doc1".to_vec(),
        field_name: "name".to_string(),
        priority: 1,
        schema_version_id: "schema1".to_string(),
        data: b"John".to_vec(),
    })
}

// ============================================================================
// Basic Roundtrip Tests
// ============================================================================

#[test]
fn test_block_ipld_roundtrip() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);

    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    assert_eq!(block.delta.priority(), restored.delta.priority());
    if let (CrdtDelta::Lww(orig), CrdtDelta::Lww(rest)) = (&block.delta, &restored.delta) {
        assert_eq!(orig.doc_id, rest.doc_id);
        assert_eq!(orig.field_name, rest.field_name);
        assert_eq!(orig.data, rest.data);
    }
}

#[test]
fn test_block_with_heads_ipld_roundtrip() {
    let head = test_cid();
    let block = Block::new(test_lww_delta(), vec![head], vec![]);

    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    assert!(restored.heads.is_some());
    assert_eq!(restored.heads.unwrap().len(), 1);
}

#[test]
fn test_block_with_links_ipld_roundtrip() {
    let link = DAGLink::new("field", test_cid());
    let block = Block::new(test_lww_delta(), vec![], vec![link]);

    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    assert!(restored.links.is_some());
    let links = restored.links.unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].name, "field");
}

#[test]
fn test_counter_delta_ipld_roundtrip() {
    let delta = CrdtDelta::Counter(CounterDeltaPayload {
        doc_id: b"doc1".to_vec(),
        field_name: "count".to_string(),
        priority: 5,
        nonce: -42,
        schema_version_id: "v1".to_string(),
        data: vec![1, 2, 3],
    });

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::Counter(c) = &restored.delta {
        assert_eq!(c.nonce, -42);
        assert_eq!(c.priority, 5);
    } else {
        panic!("Expected Counter delta");
    }
}

#[test]
fn test_composite_delta_ipld_roundtrip() {
    let delta = CrdtDelta::Composite(CompositeDeltaPayload {
        doc_id: b"doc1".to_vec(),
        schema_version_id: "v1".to_string(),
        priority: 10,
        status: 2,
    });

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::Composite(c) = &restored.delta {
        assert_eq!(c.status, 2);
        assert_eq!(c.priority, 10);
    } else {
        panic!("Expected Composite delta");
    }
}

#[test]
fn test_collection_delta_ipld_roundtrip() {
    let delta = CrdtDelta::Collection(CollectionDeltaPayload {
        schema_version_id: "v2".to_string(),
        priority: 3,
    });

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::Collection(c) = &restored.delta {
        assert_eq!(c.priority, 3);
        assert_eq!(c.schema_version_id, "v2");
    } else {
        panic!("Expected Collection delta");
    }
}

// ============================================================================
// FieldDefinition/CollectionDefinition Roundtrip Tests (NEW)
// ============================================================================

#[test]
fn test_field_definition_delta_ipld_roundtrip_minimal() {
    let delta = CrdtDelta::FieldDefinition(FieldDefinitionDeltaPayload::new(1));

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::FieldDefinition(fd) = &restored.delta {
        assert_eq!(fd.priority, 1);
        assert!(fd.name.is_none());
        assert!(fd.crdt.is_none());
        assert!(fd.scalar_kind.is_none());
        assert!(fd.collection_id.is_none());
        assert!(fd.relative_id.is_none());
    } else {
        panic!("Expected FieldDefinition delta");
    }
}

#[test]
fn test_field_definition_delta_ipld_roundtrip_full() {
    let delta = CrdtDelta::FieldDefinition(
        FieldDefinitionDeltaPayload::new(42)
            .with_name("age")
            .with_crdt(1)
            .with_scalar_kind(3),
    );

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::FieldDefinition(fd) = &restored.delta {
        assert_eq!(fd.priority, 42);
        assert_eq!(fd.name, Some("age".to_string()));
        assert_eq!(fd.crdt, Some(1));
        assert_eq!(fd.scalar_kind, Some(3));
        assert!(fd.collection_id.is_none());
        assert!(fd.relative_id.is_none());
    } else {
        panic!("Expected FieldDefinition delta");
    }
}

#[test]
fn test_field_definition_delta_ipld_roundtrip_with_relation() {
    let delta = CrdtDelta::FieldDefinition(
        FieldDefinitionDeltaPayload::new(5)
            .with_name("author")
            .with_crdt(2)
            .with_collection_id("bafkreiusers123"),
    );

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::FieldDefinition(fd) = &restored.delta {
        assert_eq!(fd.priority, 5);
        assert_eq!(fd.name, Some("author".to_string()));
        assert_eq!(fd.crdt, Some(2));
        assert_eq!(fd.collection_id, Some("bafkreiusers123".to_string()));
        assert!(fd.scalar_kind.is_none());
        assert!(fd.relative_id.is_none());
    } else {
        panic!("Expected FieldDefinition delta");
    }
}

#[test]
fn test_field_definition_delta_ipld_roundtrip_with_relative_id() {
    let delta = CrdtDelta::FieldDefinition(
        FieldDefinitionDeltaPayload::new(10)
            .with_name("parent")
            .with_crdt(2)
            .with_relative_id(-1),
    );

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::FieldDefinition(fd) = &restored.delta {
        assert_eq!(fd.priority, 10);
        assert_eq!(fd.name, Some("parent".to_string()));
        assert_eq!(fd.crdt, Some(2));
        assert_eq!(fd.relative_id, Some(-1));
        assert!(fd.scalar_kind.is_none());
        assert!(fd.collection_id.is_none());
    } else {
        panic!("Expected FieldDefinition delta");
    }
}

#[test]
fn test_collection_definition_delta_ipld_roundtrip_minimal() {
    let delta = CrdtDelta::CollectionDefinition(CollectionDefinitionDeltaPayload::new(1));

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::CollectionDefinition(cd) = &restored.delta {
        assert_eq!(cd.priority, 1);
        assert!(cd.name.is_none());
    } else {
        panic!("Expected CollectionDefinition delta");
    }
}

#[test]
fn test_collection_definition_delta_ipld_roundtrip_with_name() {
    let delta = CrdtDelta::CollectionDefinition(
        CollectionDefinitionDeltaPayload::new(1).with_name("users"),
    );

    let block = Block::new(delta, vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    if let CrdtDelta::CollectionDefinition(cd) = &restored.delta {
        assert_eq!(cd.priority, 1);
        assert_eq!(cd.name, Some("users".to_string()));
    } else {
        panic!("Expected CollectionDefinition delta");
    }
}

// ============================================================================
// Error Path Tests (NEW)
// ============================================================================

#[test]
fn test_block_from_ipld_not_map_fails() {
    let ipld = Ipld::String("not a map".to_string());
    let result = Block::try_from(&ipld);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Expected IPLD map"));
}

#[test]
fn test_block_from_ipld_missing_delta_fails() {
    let ipld = Ipld::Map(BTreeMap::new());
    let result = Block::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Missing 'delta'"));
}

#[test]
fn test_crdt_delta_from_ipld_unknown_type_fails() {
    let mut map = BTreeMap::new();
    map.insert("unknownDelta".to_string(), Ipld::Map(BTreeMap::new()));
    let ipld = Ipld::Map(map);
    let result = CrdtDelta::try_from(&ipld);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unknown CRDT delta"));
}

#[test]
fn test_lww_payload_from_ipld_missing_field_fails() {
    let mut map = BTreeMap::new();
    map.insert("docID".to_string(), Ipld::Bytes(b"doc".to_vec()));
    // Missing fieldName, priority, schemaVersionID, data
    let ipld = Ipld::Map(map);
    let result = LwwDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Missing field"));
}

#[test]
fn test_lww_payload_from_ipld_wrong_type_fails() {
    let mut map = BTreeMap::new();
    map.insert(
        "docID".to_string(),
        Ipld::String("should be bytes".to_string()),
    );
    map.insert("fieldName".to_string(), Ipld::String("name".to_string()));
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert(
        "schemaVersionID".to_string(),
        Ipld::String("v1".to_string()),
    );
    map.insert("data".to_string(), Ipld::Bytes(vec![]));
    let ipld = Ipld::Map(map);
    let result = LwwDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("expected Bytes"));
}

#[test]
fn test_parse_u64_out_of_range_fails() {
    let mut map = BTreeMap::new();
    map.insert("priority".to_string(), Ipld::Integer(-1));
    map.insert(
        "schemaVersionID".to_string(),
        Ipld::String("v1".to_string()),
    );
    let ipld = Ipld::Map(map);
    let result = CollectionDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("out of u64 range"));
}

#[test]
fn test_parse_u8_out_of_range_fails() {
    let mut map = BTreeMap::new();
    map.insert("docID".to_string(), Ipld::Bytes(b"doc".to_vec()));
    map.insert(
        "schemaVersionID".to_string(),
        Ipld::String("v1".to_string()),
    );
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert("status".to_string(), Ipld::Integer(300)); // out of u8 range
    let ipld = Ipld::Map(map);
    let result = CompositeDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("out of u8 range"));
}

#[test]
fn test_composite_status_zero_fails() {
    let mut map = BTreeMap::new();
    map.insert("docID".to_string(), Ipld::Bytes(b"doc".to_vec()));
    map.insert(
        "schemaVersionID".to_string(),
        Ipld::String("v1".to_string()),
    );
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert("status".to_string(), Ipld::Integer(0));
    let ipld = Ipld::Map(map);

    let result = CompositeDeltaPayload::try_from(&ipld);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid document status: 0"));
}

#[test]
fn test_composite_status_missing_fails() {
    let mut map = BTreeMap::new();
    map.insert("docID".to_string(), Ipld::Bytes(b"doc".to_vec()));
    map.insert(
        "schemaVersionID".to_string(),
        Ipld::String("v1".to_string()),
    );
    map.insert("priority".to_string(), Ipld::Integer(1));
    let ipld = Ipld::Map(map);

    let result = CompositeDeltaPayload::try_from(&ipld);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Missing field 'status'"));
}

#[test]
fn test_field_definition_optional_field_wrong_type_fails() {
    let mut map = BTreeMap::new();
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert("name".to_string(), Ipld::Integer(123)); // should be String
    let ipld = Ipld::Map(map);
    let result = FieldDefinitionDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("expected String"));
}

#[test]
fn test_field_definition_optional_u8_overflow_fails() {
    let mut map = BTreeMap::new();
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert("crdt".to_string(), Ipld::Integer(256)); // out of u8 range
    let ipld = Ipld::Map(map);
    let result = FieldDefinitionDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("out of u8 range"));
}

#[test]
fn test_field_definition_optional_i32_overflow_fails() {
    let mut map = BTreeMap::new();
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert("relativeID".to_string(), Ipld::Integer(i64::MAX as i128)); // out of i32 range
    let ipld = Ipld::Map(map);
    let result = FieldDefinitionDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("out of i32 range"));
}

#[test]
fn test_dag_link_missing_link_fails() {
    let mut map = BTreeMap::new();
    map.insert("name".to_string(), Ipld::String("field".to_string()));
    // missing "link"
    let ipld = Ipld::Map(map);
    let result = DAGLink::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Missing 'link'"));
}

#[test]
fn test_signature_header_unknown_type_fails() {
    let mut map = BTreeMap::new();
    map.insert("type".to_string(), Ipld::String("UnknownAlgo".to_string()));
    map.insert("identity".to_string(), Ipld::Bytes(b"key".to_vec()));
    let ipld = Ipld::Map(map);
    let result = SignatureHeader::try_from(&ipld);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unknown signature type"));
}

// ============================================================================
// Encryption/Signature Roundtrip Tests
// ============================================================================

#[test]
fn test_encryption_ipld_roundtrip() {
    let enc = Encryption::new_for_field(b"doc1".to_vec(), "secret".to_string(), b"key123".to_vec());

    let ipld = Ipld::from(&enc);
    let restored = Encryption::try_from(&ipld).unwrap();

    assert_eq!(enc.doc_id, restored.doc_id);
    assert_eq!(enc.field_name, restored.field_name);
    assert_eq!(enc.key, restored.key);
}

#[test]
fn test_signature_ipld_roundtrip() {
    let sig = Signature::new(
        SignatureHeader::new(SignatureType::EdDSA, b"pubkey".to_vec()),
        b"signature".to_vec(),
    );

    let ipld = Ipld::from(&sig);
    let restored = Signature::try_from(&ipld).unwrap();

    assert_eq!(sig.header.sig_type, restored.header.sig_type);
    assert_eq!(sig.header.identity, restored.header.identity);
    assert_eq!(sig.value, restored.value);
}

// ============================================================================
// Link Traversal Tests
// ============================================================================

#[test]
fn test_extract_links() {
    let head = test_cid();
    let link_cid =
        cid::Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();
    let link = DAGLink::new("field", link_cid);

    let block = Block::new(test_lww_delta(), vec![head], vec![link]);
    let ipld = Ipld::try_from(&block).unwrap();

    let links = extract_links(&ipld).unwrap();
    assert_eq!(links.len(), 2);
}

#[test]
fn test_extract_links_empty_structures() {
    let block = Block::new(test_lww_delta(), vec![], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();
    let links = extract_links(&ipld).unwrap();
    assert!(links.is_empty());
}

#[test]
fn test_extract_links_handles_deeply_nested_lists_iteratively() {
    let link = cid_to_libipld(&test_cid()).unwrap();
    let mut ipld = Ipld::Link(link);
    for _ in 0..10_000 {
        ipld = Ipld::List(vec![ipld]);
    }

    let links = extract_links(&ipld).unwrap();
    assert_eq!(links, vec![test_cid()]);
    std::mem::forget(ipld);
}

#[test]
fn test_collect_block_links() {
    let head = test_cid();
    let link = DAGLink::new("field", test_cid());
    let block = Block::new(test_lww_delta(), vec![head], vec![link.clone()]);

    let links = collect_block_links(&block).unwrap();
    assert_eq!(links.len(), 2);
}

struct LinkCollector {
    links: Vec<cid::Cid>,
}

impl IpldVisitor for LinkCollector {
    fn visit(&mut self, _ipld: &Ipld) -> bool {
        true
    }

    fn visit_link(&mut self, cid: &cid::Cid) {
        self.links.push(*cid);
    }
}

#[test]
fn test_walk_ipld_visitor() {
    let head = test_cid();
    let block = Block::new(test_lww_delta(), vec![head], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();

    let mut collector = LinkCollector { links: Vec::new() };
    walk_ipld(&ipld, &mut collector).unwrap();

    assert_eq!(collector.links.len(), 1);
    assert_eq!(collector.links[0], head);
}

#[test]
fn test_walk_ipld_visitor_stops_traversal() {
    struct StopVisitor {
        count: usize,
    }
    impl IpldVisitor for StopVisitor {
        fn visit(&mut self, _ipld: &Ipld) -> bool {
            self.count += 1;
            // Stop after first visit
            false
        }
    }

    let block = Block::new(test_lww_delta(), vec![test_cid()], vec![]);
    let ipld = Ipld::try_from(&block).unwrap();

    let mut visitor = StopVisitor { count: 0 };
    walk_ipld(&ipld, &mut visitor).unwrap();

    // Should only visit the root Map
    assert_eq!(visitor.count, 1);
}

#[test]
fn test_walk_ipld_handles_deeply_nested_lists_iteratively() {
    struct CountVisitor {
        visits: usize,
        links: usize,
    }

    impl IpldVisitor for CountVisitor {
        fn visit(&mut self, _ipld: &Ipld) -> bool {
            self.visits += 1;
            true
        }

        fn visit_link(&mut self, _cid: &cid::Cid) {
            self.links += 1;
        }
    }

    let mut ipld = Ipld::Link(cid_to_libipld(&test_cid()).unwrap());
    for _ in 0..10_000 {
        ipld = Ipld::List(vec![ipld]);
    }

    let mut visitor = CountVisitor {
        visits: 0,
        links: 0,
    };
    walk_ipld(&ipld, &mut visitor).unwrap();

    assert_eq!(visitor.links, 1);
    assert_eq!(visitor.visits, 10_001);
    std::mem::forget(ipld);
}

// ============================================================================
// CID Conversion Tests
// ============================================================================

#[test]
fn test_cid_conversion() {
    let cid = test_cid();
    let libipld_cid = cid_to_libipld(&cid).unwrap();
    let back = cid_from_libipld(&libipld_cid).unwrap();
    assert_eq!(cid, back);
}

// ============================================================================
// is_definition and doc_id Tests (NEW)
// ============================================================================

#[test]
fn test_crdt_delta_is_definition() {
    let lww = CrdtDelta::Lww(LwwDeltaPayload {
        doc_id: b"doc".to_vec(),
        field_name: "f".to_string(),
        priority: 1,
        schema_version_id: "v1".to_string(),
        data: vec![],
    });
    assert!(!lww.is_definition());

    let counter = CrdtDelta::Counter(CounterDeltaPayload {
        doc_id: b"doc".to_vec(),
        field_name: "f".to_string(),
        priority: 1,
        nonce: 0,
        schema_version_id: "v1".to_string(),
        data: vec![],
    });
    assert!(!counter.is_definition());

    let field_def = CrdtDelta::FieldDefinition(FieldDefinitionDeltaPayload::new(1));
    assert!(field_def.is_definition());

    let col_def = CrdtDelta::CollectionDefinition(CollectionDefinitionDeltaPayload::new(1));
    assert!(col_def.is_definition());
}

#[test]
fn test_crdt_delta_doc_id_returns_none_for_definitions() {
    let field_def = CrdtDelta::FieldDefinition(FieldDefinitionDeltaPayload::new(1));
    assert!(field_def.doc_id().is_none());

    let col_def = CrdtDelta::CollectionDefinition(CollectionDefinitionDeltaPayload::new(1));
    assert!(col_def.doc_id().is_none());
}

// ============================================================================
// Additional Edge Case Tests
// ============================================================================

#[test]
fn test_counter_delta_nonce_i64_overflow_fails() {
    let mut map = BTreeMap::new();
    map.insert("docID".to_string(), Ipld::Bytes(b"doc".to_vec()));
    map.insert("fieldName".to_string(), Ipld::String("count".to_string()));
    map.insert("priority".to_string(), Ipld::Integer(1));
    map.insert("nonce".to_string(), Ipld::Integer(i128::MAX)); // out of i64 range
    map.insert(
        "schemaVersionID".to_string(),
        Ipld::String("v1".to_string()),
    );
    map.insert("data".to_string(), Ipld::Bytes(vec![]));
    let ipld = Ipld::Map(map);
    let result = CounterDeltaPayload::try_from(&ipld);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("out of i64 range"));
}

#[test]
fn test_dag_link_missing_name_fails() {
    let mut map = BTreeMap::new();
    map.insert(
        "link".to_string(),
        Ipld::Link(cid_to_libipld(&test_cid()).unwrap()),
    );
    // missing "name"
    let ipld = Ipld::Map(map);
    let result = DAGLink::try_from(&ipld);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Missing field 'name'"));
}

#[test]
fn test_block_with_encryption_and_signature_ipld_roundtrip() {
    let enc_cid =
        cid::Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();
    let sig_cid =
        cid::Cid::from_str("bafkreidgvpkjawlxz6sffxzwgooowe5zt6dcz3rfz4j6mmzclvggsluaby").unwrap();

    let block = Block::new_with_options(
        test_lww_delta(),
        vec![test_cid()],
        vec![],
        Some(enc_cid),
        Some(sig_cid),
    );

    let ipld = Ipld::try_from(&block).unwrap();
    let restored = Block::try_from(&ipld).unwrap();

    assert_eq!(block.encryption, restored.encryption);
    assert_eq!(block.signature, restored.signature);
}
