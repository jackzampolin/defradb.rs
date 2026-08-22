use datastore::NamespaceView;
use datastore::SharedTxn;
use db::read::seek::*;
use query::planner::index_selection::CursorSeek;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::keys::doc_id_index::encode_doc_short_id;
use storage::keys::SEPARATOR;
use storage::namespace::Namespace;

#[tokio::test]
async fn cursor_boundary_uses_doc_short_id_suffix() {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    let systemstore = NamespaceView::new(SharedTxn::new(txn), Namespace::Systemstore);
    db::docid::map::set_doc_id_mapping(&systemstore, 7, 42, "bae-boundary")
        .await
        .unwrap();

    let seek = CursorSeek {
        seek_key: vec![1, 2, 3],
        boundary_doc_id: Some("bae-boundary".to_string()),
        inclusive: false,
        reversed: false,
        expected_index_name: "idx".to_string(),
        fetch_limit: None,
    };

    let key = resolve_cursor_seek_key(&seek, &systemstore, 7)
        .await
        .unwrap();
    let mut expected = vec![1, 2, 3, SEPARATOR];
    expected.extend_from_slice(&encode_doc_short_id(42));
    assert_eq!(key, expected);
}
