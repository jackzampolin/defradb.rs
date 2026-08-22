use datastore::NamespaceView;
use datastore::SharedTxn;
use db::docid::map::*;
use db::error::Error;
use db::DB;
use std::sync::Arc;
use storage::backends::MemoryStore;
use storage::corekv::Key;
use storage::corekv::Store;
use storage::keys::doc_id_index::DocShortIDSequenceKey;
use storage::keys::doc_id_index::DocShortIDToDocIDAliasKey;
use storage::namespace::Namespace;
use storage::stores::Systemstore;

async fn systemstore() -> NamespaceView {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    NamespaceView::new(SharedTxn::new(txn), Namespace::Systemstore)
}

async fn persisted_sequence(store: Arc<MemoryStore>) -> u64 {
    let systemstore = Systemstore::new(store);
    let txn = systemstore.new_txn(true).await.unwrap();
    decode_sequence(
        txn.get(&DocShortIDSequenceKey::new().bytes())
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn short_ids_allocate_from_one() {
    let store = systemstore().await;
    assert_eq!(next_doc_short_id(&store).await.unwrap(), 1);
    assert_eq!(next_doc_short_id(&store).await.unwrap(), 2);
}

#[tokio::test]
async fn malformed_sequence_is_rejected() {
    let store = Arc::new(MemoryStore::new());
    let systemstore = Systemstore::new(store.clone());
    let mut txn = systemstore.new_txn(false).await.unwrap();
    txn.set(&DocShortIDSequenceKey::new().bytes(), b"invalid")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let error = DocShortIdAllocator::with_reservation_size(store, 4)
        .next()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Other(message)
            if message == "invalid persisted document short-ID sequence"
    ));
}

#[tokio::test]
async fn sequence_floor_preserves_reused_short_ids() {
    let store = systemstore().await;
    ensure_doc_short_id_sequence_at_least(&store, 41)
        .await
        .unwrap();
    ensure_doc_short_id_sequence_at_least(&store, 7)
        .await
        .unwrap();

    assert_eq!(next_doc_short_id(&store).await.unwrap(), 42);
}

#[tokio::test]
async fn mapping_roundtrip() {
    let store = systemstore().await;
    let short_id = next_doc_short_id(&store).await.unwrap();
    set_doc_id_mapping(&store, 7, short_id, "bae-x")
        .await
        .unwrap();

    assert_eq!(
        get_doc_id(&store, short_id).await.unwrap().as_deref(),
        Some("bae-x")
    );
    assert_eq!(
        get_doc_short_id(&store, 7, "bae-x").await.unwrap(),
        Some(short_id)
    );
    // Wrong collection: not visible.
    assert_eq!(get_doc_short_id(&store, 8, "bae-x").await.unwrap(), None);
}

#[tokio::test]
async fn allocator_reserves_ranges_across_instances_and_restarts() {
    let store = Arc::new(MemoryStore::new());
    let allocator_a = DocShortIdAllocator::with_reservation_size(store.clone(), 4);
    let allocator_b = DocShortIdAllocator::with_reservation_size(store.clone(), 4);

    let (a, b) = tokio::join!(allocator_a.next(), allocator_b.next());
    let mut first_ids = vec![a.unwrap(), b.unwrap()];
    first_ids.sort_unstable();
    assert_eq!(first_ids, vec![1, 5]);
    assert_eq!(persisted_sequence(store.clone()).await, 8);

    drop((allocator_a, allocator_b));
    let restarted = DocShortIdAllocator::with_reservation_size(store, 4);
    assert_eq!(restarted.next().await.unwrap(), 9);
}

#[tokio::test]
async fn allocator_keeps_ids_unique_across_many_database_instances() {
    const DATABASES: usize = 8;
    const IDS_PER_DATABASE: usize = 128;

    let store = Arc::new(MemoryStore::new());
    let mut tasks = Vec::with_capacity(DATABASES);
    for _ in 0..DATABASES {
        let db = DB::from_arc(store.clone()).unwrap();
        tasks.push(tokio::spawn(async move {
            let mut ids = Vec::with_capacity(IDS_PER_DATABASE);
            for _ in 0..IDS_PER_DATABASE {
                ids.push(db.next_doc_short_id().await.unwrap());
            }
            ids
        }));
    }

    let mut ids = Vec::with_capacity(DATABASES * IDS_PER_DATABASE);
    for task in tasks {
        ids.extend(task.await.unwrap());
    }
    ids.sort_unstable();
    ids.dedup();

    assert_eq!(ids.len(), DATABASES * IDS_PER_DATABASE);
    assert_eq!(persisted_sequence(store).await, 8192);
}

#[tokio::test]
async fn allocations_do_not_conflict_unrelated_caller_transactions() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::from_arc(store).unwrap();
    let txn_a = db.new_txn(false).await.unwrap();
    let txn_b = db.new_txn(false).await.unwrap();

    let a = db.next_doc_short_id().await.unwrap();
    let b = db.next_doc_short_id().await.unwrap();
    set_doc_id_mapping(&txn_a.systemstore().unwrap(), 1, a, "bae-a")
        .await
        .unwrap();
    set_doc_id_mapping(&txn_b.systemstore().unwrap(), 2, b, "bae-b")
        .await
        .unwrap();

    txn_a.commit().await.unwrap();
    txn_b.commit().await.unwrap();

    let txn = db.new_txn(true).await.unwrap();
    let systemstore = txn.systemstore().unwrap();
    assert_eq!(
        get_doc_short_id(&systemstore, 1, "bae-a").await.unwrap(),
        Some(a)
    );
    assert_eq!(
        get_doc_short_id(&systemstore, 2, "bae-b").await.unwrap(),
        Some(b)
    );
}

#[tokio::test]
async fn database_resolve_or_allocate_is_idempotent() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::from_arc(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let systemstore = txn.systemstore().unwrap();
    let a = db
        .resolve_or_allocate_doc_short_id(&systemstore, 3, "bae-y")
        .await
        .unwrap();
    let b = db
        .resolve_or_allocate_doc_short_id(&systemstore, 3, "bae-y")
        .await
        .unwrap();
    assert_eq!(a, b);
}

#[tokio::test]
async fn alias_cannot_replace_an_existing_document() {
    let store = systemstore().await;
    set_doc_id_mapping(&store, 3, 1, "bae-a").await.unwrap();
    set_doc_id_mapping(&store, 3, 2, "bae-b").await.unwrap();

    let error = set_doc_id_alias(&store, 3, 2, "bae-a").await.unwrap_err();

    assert!(error.to_string().contains("already belongs"));
    assert_eq!(get_doc_short_id(&store, 3, "bae-a").await.unwrap(), Some(1));
}

#[tokio::test]
async fn setting_the_same_alias_is_idempotent() {
    let store = systemstore().await;
    set_doc_id_mapping(&store, 3, 1, "bae-a").await.unwrap();
    store
        .delete(&DocShortIDToDocIDAliasKey::new(1, "bae-a").bytes())
        .await
        .unwrap();

    set_doc_id_alias(&store, 3, 1, "bae-a").await.unwrap();

    assert_eq!(get_doc_short_id(&store, 3, "bae-a").await.unwrap(), Some(1));
    assert!(store
        .has(&DocShortIDToDocIDAliasKey::new(1, "bae-a").bytes())
        .await
        .unwrap());
}

#[tokio::test]
async fn ownerless_document_blocks_are_not_authorizable() {
    let store = systemstore().await;
    let field_block = defra_core::Block::new(
        defra_core::CrdtDelta::Lww(defra_core::LwwDeltaPayload {
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: vec![1],
        }),
        vec![],
        vec![],
    );
    let field_cid = field_block.generate_cid().unwrap();
    assert_eq!(
        resolve_block_doc_ids(&store, &field_cid, &field_block)
            .await
            .unwrap(),
        None
    );

    let genesis = defra_core::Block::new(
        defra_core::CrdtDelta::Composite(defra_core::CompositeDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![],
    );
    let genesis_cid = genesis.generate_cid().unwrap();
    assert_eq!(
        resolve_block_doc_ids(&store, &genesis_cid, &genesis)
            .await
            .unwrap(),
        Some(vec![document::DocID::new_v0(genesis_cid).to_string()])
    );
}

#[tokio::test]
async fn block_ownership_is_a_set() {
    let store = systemstore().await;
    set_block_doc_id_mapping(&store, "bafy1", "bae-a")
        .await
        .unwrap();
    set_block_doc_id_mapping(&store, "bafy1", "bae-b")
        .await
        .unwrap();

    let mut owners = get_doc_ids_for_block(&store, "bafy1").await.unwrap();
    owners.sort();
    assert_eq!(owners, vec!["bae-a".to_string(), "bae-b".to_string()]);

    delete_block_doc_id_mapping(&store, "bafy1", "bae-a")
        .await
        .unwrap();
    assert_eq!(
        get_doc_ids_for_block(&store, "bafy1").await.unwrap(),
        vec!["bae-b".to_string()]
    );
}

#[tokio::test]
async fn delete_doc_id_mappings_removes_all() {
    let store = systemstore().await;
    let short_id = next_doc_short_id(&store).await.unwrap();
    set_doc_id_mapping(&store, 2, short_id, "bae-z")
        .await
        .unwrap();

    delete_doc_id_mappings(&store, short_id).await.unwrap();

    assert_eq!(get_doc_id(&store, short_id).await.unwrap(), None);
    assert_eq!(get_doc_short_id(&store, 2, "bae-z").await.unwrap(), None);
}
