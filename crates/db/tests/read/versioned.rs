use async_lock::Mutex as TokioMutex;
use db::read::versioned::*;
use std::sync::Arc;

#[test]
fn test_looks_like_cidv1() {
    assert!(
        VersionedFetcher::<storage::RegolithStore>::looks_like_cidv1(
            "bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q"
        )
    );
    assert!(
        VersionedFetcher::<storage::RegolithStore>::looks_like_cidv1(
            "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist"
        )
    );
    assert!(!VersionedFetcher::<storage::RegolithStore>::looks_like_cidv1("fhbnjfahfhfhanfhga"));
    assert!(!VersionedFetcher::<storage::RegolithStore>::looks_like_cidv1("short"));
}
#[tokio::test]
async fn kms_identity_prefers_caller_then_task_then_thread() {
    let txn = Arc::new(TokioMutex::new(None));
    let caller = identity::Did::new("did:key:caller").unwrap();
    let fetcher = VersionedFetcher::<storage::RegolithStore>::with_kms(
        txn.clone(),
        None,
        Some(caller.clone()),
    );
    let ambient = VersionedFetcher::<storage::RegolithStore>::with_kms(txn, None, None);
    let _thread =
        defra_core::current_identity::scoped_current_identity(Some("did:key:thread".into()));

    defra_core::current_identity::with_scoped_identity(Some("did:key:task".into()), async {
        assert_eq!(fetcher.kms_request_context().user_identity(), Some(&caller));
        assert_eq!(
            ambient.kms_request_context().user_identity(),
            Some(&identity::Did::new("did:key:task").unwrap())
        );
    })
    .await;

    assert_eq!(
        ambient.kms_request_context().user_identity(),
        Some(&identity::Did::new("did:key:thread").unwrap())
    );
}

#[tokio::test]
async fn collection_cid_doc_id_filter_accepts_an_alias() {
    use defra_core::{Block, CollectionDeltaPayload, CompositeDeltaPayload, CrdtDelta, DAGLink};
    use storage::RegolithStore;

    const COLLECTION_SHORT_ID: u32 = 7;

    let genesis = |priority| {
        Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: format!("v{priority}"),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![],
        )
    };
    let (wanted, other) = (genesis(1), genesis(2));
    let (wanted_cid, other_cid) = (
        wanted.generate_cid().unwrap(),
        other.generate_cid().unwrap(),
    );
    let collection = Block::new(
        CrdtDelta::Collection(CollectionDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority: 1,
        }),
        vec![],
        vec![
            DAGLink::new("_C", wanted_cid),
            DAGLink::new("_C", other_cid),
        ],
    );
    let collection_cid = collection.generate_cid().unwrap();

    let wanted_doc_id = document::DocID::new_v0(wanted_cid).to_string();
    let alias = document::DocID::new_v0(
        defra_core::block::generate_cid_from_bytes(b"legacy-short-id").unwrap(),
    )
    .to_string();

    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    {
        let blockstore = txn.blockstore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        for (cid, block) in [
            (wanted_cid, &wanted),
            (other_cid, &other),
            (collection_cid, &collection),
        ] {
            blockstore
                .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
                .await
                .unwrap();
        }
        for (short_id, doc_id) in [
            (1u64, &wanted_doc_id),
            (2, &document::DocID::new_v0(other_cid).to_string()),
        ] {
            db::docid::map::set_doc_id_mapping(&systemstore, COLLECTION_SHORT_ID, short_id, doc_id)
                .await
                .unwrap();
        }
        db::docid::map::set_doc_id_alias(&systemstore, COLLECTION_SHORT_ID, 1, &alias)
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();

    let version_txn = db.new_txn(true).await.unwrap();
    let documents = VersionedFetcher::new(Arc::new(TokioMutex::new(Some(version_txn))))
        .get_documents_at_cid(
            &collection_cid.to_string(),
            Some(&alias),
            Some(COLLECTION_SHORT_ID),
        )
        .await
        .unwrap();

    let ids: Vec<_> = documents
        .iter()
        .filter_map(|document| document.id().map(|id| id.to_string()))
        .collect();
    assert_eq!(ids, vec![wanted_doc_id]);
}
