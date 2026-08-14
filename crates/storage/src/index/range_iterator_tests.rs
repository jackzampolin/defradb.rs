use super::*;
use crate::backends::MemoryStore;
use crate::corekv::Store;
use crate::index::{CollectionIndex, SimpleIndex};
use schema::IndexedFieldDescription;

fn test_index_description() -> IndexDescription {
    IndexDescription {
        id: 1,
        name: "test_index".to_string(),
        unique: false,
        kind: None,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "age".to_string(),
            descending: false,
        }],
    }
}

fn composite_index_description() -> IndexDescription {
    IndexDescription {
        id: 2,
        name: "composite_index".to_string(),
        unique: false,
        kind: None,
        auto_generated: false,
        fields: vec![
            IndexedFieldDescription {
                name: "category".to_string(),
                descending: false,
            },
            IndexedFieldDescription {
                name: "score".to_string(),
                descending: false,
            },
        ],
    }
}

#[tokio::test]
async fn test_range_scan_all() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, 1, &[NormalValue::Int(10)])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::Int(20)])
        .await
        .unwrap();
    index
        .save(&mut txn, 3, &[NormalValue::Int(30)])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, false)
        .await
        .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 3);
}

#[tokio::test]
async fn test_range_scan_reverse() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, 1, &[NormalValue::Int(10)])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::Int(20)])
        .await
        .unwrap();
    index
        .save(&mut txn, 3, &[NormalValue::Int(30)])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, true)
        .await
        .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 3);

    // Should be in reverse order
    assert_eq!(entries[0].values[0], NormalValue::Int(30));
    assert_eq!(entries[2].values[0], NormalValue::Int(10));
}

#[tokio::test]
async fn test_range_prefix_scan() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = composite_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    // Save documents with different categories
    index
        .save(
            &mut txn,
            1,
            &[NormalValue::String("A".to_string()), NormalValue::Int(100)],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            2,
            &[NormalValue::String("A".to_string()), NormalValue::Int(200)],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            3,
            &[NormalValue::String("B".to_string()), NormalValue::Int(150)],
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Scan only category "A"
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_prefix(
        txn.as_ref(),
        1,
        &desc,
        false,
        &[NormalValue::String("A".to_string())],
        false,
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn test_range_bounded() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    for i in 1..=10 {
        index
            .save(&mut txn, i as u64, &[NormalValue::Int(i * 10)])
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();

    // Range: 30 < x <= 70
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_range(
        txn.as_ref(),
        1,
        &desc,
        false,
        &[],
        Bound::gt(NormalValue::Int(30)),
        Bound::le(NormalValue::Int(70)),
        false,
    )
    .await
    .unwrap();

    let entries = iter.collect_all().await.unwrap();
    // Should include 40, 50, 60, 70
    assert_eq!(entries.len(), 4);
}

/// Build an encoded seek key for an integer value on the test index (collection=1, index=1).
fn build_age_seek_key(age: i64) -> Vec<u8> {
    let key_prefix = IndexDataStoreKey::index_prefix(1, 1);
    encode_field_value(key_prefix, &NormalValue::Int(age), false).unwrap()
}

#[tokio::test]
async fn test_cursor_seek_forward_exclusive_skips_boundary() {
    // Setup: age index with doc 1 = 20, doc 2 = 30, doc 3 = 40.
    // Forward exclusive seek at age=30 → should return only doc 3 (age=40).
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, 1, &[NormalValue::Int(20)])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::Int(30)])
        .await
        .unwrap();
    index
        .save(&mut txn, 3, &[NormalValue::Int(40)])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, false)
        .await
        .unwrap();

    // Seek to age=30 exclusive (forward): iterator should land at doc 3 (age=40).
    let seek_key = build_age_seek_key(30);
    iter.apply_cursor_seek(seek_key, false, false)
        .await
        .unwrap();

    let entries = iter.collect_all().await.unwrap();
    let doc_ids: Vec<u64> = entries.iter().map(|e| e.doc_short_id).collect();

    assert!(!doc_ids.contains(&1), "doc 1 should be before the cursor");
    assert!(
        !doc_ids.contains(&2),
        "doc 2 is the exclusive boundary and must be skipped"
    );
    assert!(
        doc_ids.contains(&3),
        "doc 3 must be included (after boundary)"
    );
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn test_cursor_seek_backward_inclusive_starts_at_boundary() {
    // Setup: age index with doc 1 = 20, doc 2 = 30, doc 3 = 40.
    // Backward inclusive seek at age=30 → should return doc 2 then doc 1.
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, 1, &[NormalValue::Int(20)])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::Int(30)])
        .await
        .unwrap();
    index
        .save(&mut txn, 3, &[NormalValue::Int(40)])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, true)
        .await
        .unwrap();

    // Seek to age=30 inclusive (reverse): iterator should include docs 2 and 1.
    // The seek_key encodes age=30; for a reverse scan the storage seek lands at or before it.
    let seek_key = build_age_seek_key(30);
    iter.apply_cursor_seek(seek_key, true, true).await.unwrap();

    let entries = iter.collect_all().await.unwrap();
    let doc_ids: Vec<u64> = entries.iter().map(|e| e.doc_short_id).collect();

    assert!(
        !doc_ids.contains(&3),
        "doc 3 is after the cursor and must be excluded"
    );
    assert!(
        doc_ids.contains(&2),
        "doc 2 is the inclusive boundary and must be included"
    );
    assert!(
        doc_ids.contains(&1),
        "doc 1 must be included (before boundary in reverse)"
    );
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn test_cursor_seek_reversed_param_controls_upper_bound() {
    // Verify that the `reversed` parameter controls which bound slot is used,
    // overriding `self.reverse`. This is the P1.1 regression guard: the
    // `reversed` field in CursorSeek must be propagated explicitly rather than
    // silently relying on `self.reverse` already being set correctly.
    //
    // Scenario: build a reverse iterator (self.reverse=true), then call
    // apply_cursor_seek with reversed=true at age=30 inclusive.
    // The upper_bound_key should be set (reverse path), not lower_bound_key.
    // The existing test_cursor_seek_backward_inclusive_starts_at_boundary already
    // validates the behavioral outcome; this test checks the bound slot directly.
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let desc = test_index_description();
    let index = SimpleIndex::new(1, desc.clone());

    index
        .save(&mut txn, 1, &[NormalValue::Int(20)])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::Int(30)])
        .await
        .unwrap();
    index
        .save(&mut txn, 3, &[NormalValue::Int(40)])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Build a reverse iterator to match the reversed=true param.
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, true)
        .await
        .unwrap();

    let seek_key = build_age_seek_key(30);
    iter.apply_cursor_seek(seek_key.clone(), true, true)
        .await
        .unwrap();

    // The reversed=true path must set upper_bound_key, not lower_bound_key.
    assert!(
        iter.upper_bound_key.is_some(),
        "reversed=true must set upper_bound_key"
    );
    assert!(
        iter.lower_bound_key.is_none(),
        "reversed=true must NOT set lower_bound_key"
    );
    assert_eq!(iter.upper_bound_key.as_deref(), Some(seek_key.as_slice()));
    assert!(
        iter.upper_inclusive,
        "inclusive=true must set upper_inclusive"
    );

    // Behavioral check: docs 2 and 1 returned (inclusive at 30, exclude doc 3 > 30).
    let entries = iter.collect_all().await.unwrap();
    let doc_ids: Vec<u64> = entries.iter().map(|e| e.doc_short_id).collect();
    assert!(!doc_ids.contains(&3), "doc 3 is above the cursor boundary");
    assert!(doc_ids.contains(&2), "doc 2 is at the boundary (inclusive)");
    assert!(doc_ids.contains(&1), "doc 1 is before boundary");
    assert_eq!(entries.len(), 2);
}
