use super::*;
use crate::backends::MemoryStore;
use crate::corekv::Store;
use crate::index::{CollectionIndex, SimpleIndex, UniqueIndex};
use schema::IndexedFieldDescription;

fn test_index_description(unique: bool) -> IndexDescription {
    IndexDescription {
        id: 1,
        name: "test_index".to_string(),
        unique,
        fields: vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
    }
}

#[tokio::test]
async fn test_in_iterator_simple_finds_multiple_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description(false);
    let index = SimpleIndex::new(1, desc.clone());

    // Insert documents with different values
    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc3",
            &[NormalValue::String("charlie".to_string())],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc4",
            &[NormalValue::String("david".to_string())],
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_simple(
        txn.as_ref(),
        1,
        &desc,
        &[
            NormalValue::String("alice".to_string()),
            NormalValue::String("charlie".to_string()),
        ],
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 2);

    let doc_ids: Vec<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
    assert!(doc_ids.contains(&"doc1"));
    assert!(doc_ids.contains(&"doc3"));
}

#[tokio::test]
async fn test_in_iterator_simple_handles_duplicates_same_value() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description(false);
    let index = SimpleIndex::new(1, desc.clone());

    // Insert multiple documents with same indexed value
    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc2",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(&mut txn, "doc3", &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_simple(
        txn.as_ref(),
        1,
        &desc,
        &[NormalValue::String("alice".to_string())],
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 2);

    let doc_ids: Vec<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
    assert!(doc_ids.contains(&"doc1"));
    assert!(doc_ids.contains(&"doc2"));
}

#[tokio::test]
async fn test_in_iterator_unique_finds_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description(true);
    let index = UniqueIndex::new(1, desc.clone());

    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc3",
            &[NormalValue::String("charlie".to_string())],
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_unique(
        txn.as_ref(),
        1,
        &desc,
        &[
            NormalValue::String("alice".to_string()),
            NormalValue::String("bob".to_string()),
        ],
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn test_in_iterator_empty_result() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description(false);
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_simple(
        txn.as_ref(),
        1,
        &desc,
        &[
            NormalValue::String("bob".to_string()),
            NormalValue::String("charlie".to_string()),
        ],
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 0);
}

#[tokio::test]
async fn test_in_iterator_reset() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description(false);
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_simple(
        txn.as_ref(),
        1,
        &desc,
        &[
            NormalValue::String("alice".to_string()),
            NormalValue::String("bob".to_string()),
        ],
    )
    .await
    .unwrap();

    // Consume all entries
    let entries1 = iter.collect_all().await.unwrap();
    assert_eq!(entries1.len(), 2);

    // Reset and collect again
    iter.reset().await.unwrap();
    let entries2 = iter.collect_all().await.unwrap();
    assert_eq!(entries2.len(), 2);
}

#[tokio::test]
async fn test_in_iterator_with_null_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description(false);
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, "doc1", &[NormalValue::Null])
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc2",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_simple(
        txn.as_ref(),
        1,
        &desc,
        &[NormalValue::Null, NormalValue::String("alice".to_string())],
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn test_in_iterator_integer_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = IndexDescription {
        id: 1,
        name: "age_index".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "age".to_string(),
            descending: false,
        }],
    };
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, "doc1", &[NormalValue::Int(25)])
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::Int(30)])
        .await
        .unwrap();
    index
        .save(&mut txn, "doc3", &[NormalValue::Int(35)])
        .await
        .unwrap();
    index
        .save(&mut txn, "doc4", &[NormalValue::Int(40)])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = InIterator::new_simple(
        txn.as_ref(),
        1,
        &desc,
        &[NormalValue::Int(25), NormalValue::Int(35)],
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 2);

    let doc_ids: Vec<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
    assert!(doc_ids.contains(&"doc1"));
    assert!(doc_ids.contains(&"doc3"));
}
