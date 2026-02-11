use super::*;
use blockstore::{Blockstore, DefraBlockstore};
use std::sync::Arc;
use storage::backends::MemoryStore;

fn make_test_blockstore() -> Arc<DefraBlockstore<MemoryStore>> {
    let store = Arc::new(MemoryStore::new());
    Arc::new(DefraBlockstore::new(store, false))
}

#[tokio::test]
async fn test_build_blocks_creates_proper_structure() {
    let mut doc = Document::new();
    doc.generate_and_set_doc_id().unwrap();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));

    let blockstore = make_test_blockstore();
    let schema_version_id = "bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq";

    let result = build_blocks_from_document(&doc, schema_version_id, &blockstore)
        .await
        .unwrap();

    // Should have created 2 field blocks (name, age)
    assert_eq!(result.field_cids.len(), 2);
    assert!(!result.doc_id.is_empty());

    // Composite block should be in blockstore
    let stored = blockstore.get(&result.cid).await.unwrap();
    assert!(stored.is_some());

    // Each field block should be in blockstore
    for field_cid in &result.field_cids {
        let stored = blockstore.get(field_cid).await.unwrap();
        assert!(stored.is_some());
    }
}

#[tokio::test]
async fn test_build_blocks_requires_doc_id() {
    let doc = Document::new();
    let blockstore = make_test_blockstore();

    let result = build_blocks_from_document(&doc, "schema-v1", &blockstore).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must have an ID"));
}

#[tokio::test]
async fn test_field_block_contains_lww_delta() {
    let mut doc = Document::new();
    doc.generate_and_set_doc_id().unwrap();
    doc.set("name", NormalValue::String("Bob".to_string()));

    let blockstore = make_test_blockstore();
    let schema_version_id = "schema-v1";

    let result = build_blocks_from_document(&doc, schema_version_id, &blockstore)
        .await
        .unwrap();

    // Get the field block
    let field_cid = &result.field_cids[0];
    let field_bytes = blockstore.get(field_cid).await.unwrap().unwrap();

    // Decode and verify it's an LWW block
    let field_block = Block::from_dag_cbor(&field_bytes).unwrap();
    match &field_block.delta {
        CrdtDelta::Lww(payload) => {
            assert_eq!(payload.field_name, "name");
            assert_eq!(payload.schema_version_id, schema_version_id);
            assert_eq!(payload.priority, 1);
        }
        _ => panic!("Expected LWW delta"),
    }
}

#[tokio::test]
async fn test_composite_block_has_field_links() {
    let mut doc = Document::new();
    doc.generate_and_set_doc_id().unwrap();
    doc.set("name", NormalValue::String("Charlie".to_string()));
    doc.set("age", NormalValue::Int(25));

    let blockstore = make_test_blockstore();

    let result = build_blocks_from_document(&doc, "schema-v1", &blockstore)
        .await
        .unwrap();

    // Decode the composite block
    let composite_block = Block::from_dag_cbor(&result.block).unwrap();

    // Verify it's a Composite delta
    match &composite_block.delta {
        CrdtDelta::Composite(payload) => {
            assert_eq!(payload.status, 1); // Active
            assert_eq!(payload.priority, 1);
        }
        _ => panic!("Expected Composite delta"),
    }

    // Verify links to field blocks
    let links = composite_block.links.as_ref().expect("Should have links");
    assert_eq!(links.len(), 2);

    // Links should reference field CIDs
    let link_cids: Vec<Cid> = links.iter().map(|l| l.link).collect();
    for field_cid in &result.field_cids {
        assert!(link_cids.contains(field_cid));
    }
}
