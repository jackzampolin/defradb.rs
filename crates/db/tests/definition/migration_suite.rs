use crate::common::schema::users_schema;
use async_trait::async_trait;
use bytes::Bytes;
use db::AutoCommitMutator;
use db::DB;
use futures::StreamExt;
use lens::LensConfig;
use lens::LensDocResultStream;
use lens::LensDocStream;
use lens::LensModule;
use lens::TransformId;
use lens::TransformStore;
use query::txn::GetTransactionResult;
use query::txn::TransactionRegistry;
use query::DocFetcher;
use query::DocMutator;
use query::QueryExecutor;
use query::QueryRequest;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use storage::corekv::IterOptions;
use storage::index::IndexIterator;
use storage::RegolithStore;
use tokio::sync::Notify;

#[derive(Default)]
struct SetVerifiedStore {
    transforms: RwLock<HashSet<TransformId>>,
    transform_calls: AtomicUsize,
}

#[async_trait]
impl TransformStore for SetVerifiedStore {
    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        let id = TransformId::new(format!("test-transform-{}", config.lenses.len()));
        self.transforms.write().unwrap().insert(id.clone());
        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, _config: LensConfig) -> lens::Result<()> {
        self.transforms.write().unwrap().insert(id);
        Ok(())
    }

    async fn list(&self) -> lens::Result<HashMap<String, LensModule>> {
        Ok(HashMap::new())
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        if !self.has_transform(id) {
            return Err(lens::Error::TransformNotFound(id.to_string()));
        }
        self.transform_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(docs.map(|mut doc| {
            doc.insert("verified".to_string(), serde_json::Value::Bool(true));
            Ok(doc)
        })))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        self.transform(id, docs)
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.transforms.read().unwrap().contains(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        self.transforms.write().unwrap().remove(id);
        Ok(())
    }
}

#[derive(Default)]
struct BlockingVerifiedStore {
    transforms: RwLock<HashSet<TransformId>>,
    transform_calls: AtomicUsize,
    block_on_call: AtomicUsize,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl BlockingVerifiedStore {
    fn arm(&self) {
        self.arm_on_call(self.transform_calls.load(Ordering::SeqCst) + 1);
    }

    fn arm_on_call(&self, call: usize) {
        self.block_on_call.store(call, Ordering::SeqCst);
    }
}

#[async_trait]
impl TransformStore for BlockingVerifiedStore {
    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        let id = TransformId::new(format!("blocking-transform-{}", config.lenses.len()));
        self.transforms.write().unwrap().insert(id.clone());
        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, _config: LensConfig) -> lens::Result<()> {
        self.transforms.write().unwrap().insert(id);
        Ok(())
    }

    async fn list(&self) -> lens::Result<HashMap<String, LensModule>> {
        Ok(HashMap::new())
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        if !self.has_transform(id) {
            return Err(lens::Error::TransformNotFound(id.to_string()));
        }
        let call = self.transform_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let should_block = self.block_on_call.load(Ordering::SeqCst) == call;
        let entered = self.entered.clone();
        let release = self.release.clone();
        Ok(Box::pin(docs.then(move |mut doc| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                if should_block {
                    entered.notify_one();
                    release.notified().await;
                }
                doc.insert("verified".to_string(), serde_json::Value::Bool(true));
                Ok(doc)
            }
        })))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        self.transform(id, docs)
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.transforms.read().unwrap().contains(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        self.transforms.write().unwrap().remove(id);
        Ok(())
    }
}

#[derive(Default)]
struct StepTransformStore {
    next_id: AtomicUsize,
    fields: RwLock<HashMap<TransformId, String>>,
}

#[async_trait]
impl TransformStore for StepTransformStore {
    async fn add(&self, _config: LensConfig) -> lens::Result<TransformId> {
        let sequence = self.next_id.fetch_add(1, Ordering::SeqCst);
        let id = TransformId::new(format!("step-transform-{sequence}"));
        let field = if sequence == 0 { "latest" } else { "middle" };
        self.fields
            .write()
            .unwrap()
            .insert(id.clone(), field.to_string());
        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, _config: LensConfig) -> lens::Result<()> {
        self.fields
            .write()
            .unwrap()
            .insert(id, "restored".to_string());
        Ok(())
    }

    async fn list(&self) -> lens::Result<HashMap<String, LensModule>> {
        Ok(HashMap::new())
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        let field = self
            .fields
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| lens::Error::TransformNotFound(id.to_string()))?;
        Ok(Box::pin(docs.map(move |mut doc| {
            doc.insert(field.clone(), serde_json::Value::String("set".to_string()));
            Ok(doc)
        })))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        self.transform(id, docs)
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.fields.read().unwrap().contains_key(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        self.fields.write().unwrap().remove(id);
        Ok(())
    }
}

fn indexed_users_schema() -> CollectionVersion {
    let mut schema = CollectionVersion::new(
        "Users",
        "users-v1",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "verified", FieldKind::bool()),
        ],
    );
    schema.indexes =
        vec![schema::IndexDescription::new("idx_verified").with_field("verified", false)];
    schema.is_materialized = true;
    schema
}

async fn seed_user(db: &Arc<DB<RegolithStore>>) -> document::DocID {
    let mut doc = document::Document::new();
    doc.set("name", document::NormalValue::String("Alice".to_string()));
    AutoCommitMutator::new(db.clone())
        .create("Users", doc)
        .await
        .unwrap()
        .doc_id
}

async fn add_verified_version(db: &Arc<DB<RegolithStore>>) -> String {
    db.patch_collection(
        "Users",
        r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"verified","Kind":"Boolean"}}]"#,
        None,
    )
    .await
    .unwrap()
    .version_id
}

async fn add_placeholder_version(db: &Arc<DB<RegolithStore>>, field_name: &str) -> String {
    db.patch_collection(
        "Users",
        &format!(
            r#"[{{"op":"add","path":"/Users/Fields/-","value":{{"Name":"{field_name}","Kind":"String"}}}}]"#
        ),
        None,
    )
    .await
    .unwrap()
    .version_id
}

async fn seed_old_user(
    db: &Arc<DB<RegolithStore>>,
    version_id: &str,
    name: &str,
) -> document::DocID {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let index_manager = db::index::IndexManager::from_collection(
        collection.resolved_root_id(),
        collection.schema(),
    )
    .unwrap();
    let mut doc = document::Document::new();
    let seed = format!("late-{name}");
    let doc_id = document::DocID::new_v0_from_seed(&seed);
    doc.set_id(doc_id.clone());
    doc.set("name", document::NormalValue::String(name.to_string()));
    doc.set_schema_version_id(version_id);

    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let doc_short_id = db::docid::map::next_doc_short_id(&systemstore)
        .await
        .unwrap();
    db::docid::map::set_doc_id_mapping(
        &systemstore,
        collection.resolved_root_id(),
        doc_short_id,
        &doc_id.to_string(),
    )
    .await
    .unwrap();
    collection
        .create_with_indexes(&datastore, &doc, doc_short_id, &index_manager)
        .await
        .unwrap();
    collection
        .save_with_datastore(&datastore, &doc, doc_short_id)
        .await
        .unwrap();
    drop(datastore);
    drop(systemstore);
    txn.commit().await.unwrap();
    doc_id
}

async fn verified_index_count(db: &Arc<DB<RegolithStore>>) -> usize {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let index_manager = db::index::IndexManager::from_collection(
        collection.resolved_root_id(),
        collection.schema(),
    )
    .unwrap();
    let index = index_manager.get_index("idx_verified").unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let mut iter = index
        .get(&datastore, &[document::NormalValue::Bool(true)])
        .await
        .unwrap();
    let entries = iter.collect_all().await.unwrap();
    drop(datastore);
    txn.discard().unwrap();
    entries.len()
}

async fn load_user(db: &Arc<DB<RegolithStore>>, doc_id: &document::DocID) -> document::Document {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let doc = collection
        .get_by_doc_id(
            &txn.datastore().unwrap(),
            &txn.systemstore().unwrap(),
            doc_id,
        )
        .await
        .unwrap()
        .unwrap();
    txn.discard().unwrap();
    doc
}

async fn load_user_blob(db: &Arc<DB<RegolithStore>>, doc_id: &document::DocID) -> Bytes {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let doc_short_id = collection
        .resolve_doc_short_id(&systemstore, doc_id)
        .await
        .unwrap()
        .unwrap();
    let blob = datastore
        .get(&collection.doc_key(doc_short_id))
        .await
        .unwrap()
        .unwrap();
    drop(datastore);
    drop(systemstore);
    txn.discard().unwrap();
    blob
}

async fn namespace_counts(db: &Arc<DB<RegolithStore>>) -> (usize, usize, usize) {
    async fn count(view: datastore::NamespaceView) -> usize {
        let mut iter = view.iterator(IterOptions::new()).await.unwrap();
        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        iter.close().await.unwrap();
        count
    }

    let txn = db.new_txn(true).await.unwrap();
    let counts = (
        count(txn.blockstore().unwrap()).await,
        count(txn.headstore().unwrap()).await,
        count(txn.peerstore().unwrap()).await,
    );
    txn.discard().unwrap();
    counts
}

#[tokio::test]
async fn lazy_lensed_read_writes_back_without_new_commits() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::from_arc(store.clone()).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let doc_id = seed_user(&db).await;
    let v2 = add_verified_version(&db).await;
    let fetcher = db::LensedAutoCommitFetcher::new(db.clone());
    let before_migration = fetcher.get_all("Users").await.unwrap();
    assert_eq!(before_migration[0].get("verified"), None);

    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let before_counts = namespace_counts(&db).await;
    let docs = fetcher.get_all("Users").await.unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0].get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(docs[0].schema_version_id(), Some(v2.as_str()));
    assert_eq!(transform_store.transform_calls.load(Ordering::SeqCst), 1);

    let persisted = load_user(&db, &doc_id).await;
    assert_eq!(
        persisted.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(persisted.schema_version_id(), Some(v2.as_str()));
    assert_eq!(namespace_counts(&db).await, before_counts);

    fetcher.get_all("Users").await.unwrap();
    assert_eq!(
        transform_store.transform_calls.load(Ordering::SeqCst),
        1,
        "the cached version must bypass the lens on subsequent reads"
    );

    drop(fetcher);
    drop(db);
    let reopened = Arc::new(DB::open_from_arc(store).await.unwrap());
    let persisted_after_restart = load_user(&reopened, &doc_id).await;
    assert_eq!(
        persisted_after_restart.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(
        persisted_after_restart.schema_version_id(),
        Some(v2.as_str())
    );
}

#[tokio::test]
async fn eager_materialize_restamps_transformless_paths() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store).unwrap());

    db.create_collection(users_schema()).await.unwrap();
    let doc_id = seed_user(&db).await;
    let v2 = add_verified_version(&db).await;
    assert_ne!(
        load_user(&db, &doc_id).await.schema_version_id(),
        Some(v2.as_str())
    );

    let before_counts = namespace_counts(&db).await;
    let before_blob = load_user_blob(&db, &doc_id).await;
    assert_eq!(db.materialize_collection("Users").await.unwrap(), 1);
    assert_eq!(
        load_user(&db, &doc_id).await.schema_version_id(),
        Some(v2.as_str())
    );
    assert_eq!(
        load_user_blob(&db, &doc_id).await,
        before_blob,
        "identity materialization must update only the version key"
    );
    assert_eq!(db.materialize_collection("Users").await.unwrap(), 0);
    assert_eq!(namespace_counts(&db).await, before_counts);
}

#[tokio::test]
async fn migration_context_cache_invalidates_when_graph_changes_without_version_change() {
    let transform_store = Arc::new(StepTransformStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store);
    let db = Arc::new(raw_db);

    db.create_collection(users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "middle").await;
    let v3 = add_placeholder_version(&db, "latest").await;

    db.set_migration(
        LensConfig::new(
            &v2,
            &v3,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let fetcher = db::LensedAutoCommitFetcher::new(db.clone());
    assert!(fetcher.get_all("Users").await.unwrap().is_empty());

    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let doc_id = seed_old_user(&db, &v1, "CachedGraph").await;
    let docs = fetcher
        .get_by_ids("Users", &[doc_id.to_string()])
        .await
        .unwrap()
        .into_docs();
    assert_eq!(
        docs[0].get("middle"),
        Some(&document::NormalValue::String("set".to_string())),
        "the newly registered v1→v2 transform must be present in refreshed history"
    );
    assert_eq!(
        docs[0].get("latest"),
        Some(&document::NormalValue::String("set".to_string())),
        "the previously cached v2→v3 transform must remain in the complete history"
    );
}

#[tokio::test]
async fn lazy_write_back_updates_secondary_indexes_for_late_document() {
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store);
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let doc_id = seed_old_user(&db, &v1, "Late").await;
    assert_eq!(verified_index_count(&db).await, 0);

    let fetcher = db::LensedAutoCommitFetcher::new(db.clone());
    let docs = fetcher
        .get_by_ids("Users", &[doc_id.to_string()])
        .await
        .unwrap()
        .into_docs();
    assert_eq!(
        docs[0].get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(
        verified_index_count(&db).await,
        1,
        "lazy migration must atomically replace the document's index entries"
    );
}

#[tokio::test]
async fn lazy_full_scan_flushes_write_back_in_bounded_batches() {
    let transform_store = Arc::new(BlockingVerifiedStore::default());
    let options = db::DbOptions::new().with_migration_write_back_batch_size(
        NonZeroUsize::new(2).expect("batch size is non-zero"),
    );
    let mut raw_db = DB::with_options(RegolithStore::in_memory().unwrap(), options).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let mut doc_ids = Vec::new();
    for name in ["Batch-1", "Batch-2", "Batch-3"] {
        doc_ids.push(seed_old_user(&db, &v1, name).await);
    }

    transform_store.arm_on_call(3);
    let fetch_db = db.clone();
    let fetch = tokio::spawn(async move {
        db::LensedAutoCommitFetcher::new(fetch_db)
            .get_all("Users")
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        transform_store.entered.notified(),
    )
    .await
    .expect("third migration reached the transform");

    assert_eq!(
        verified_index_count(&db).await,
        2,
        "the completed batch must commit before the next batch is transformed"
    );

    transform_store.release.notify_one();
    let docs = fetch.await.unwrap().unwrap();
    assert_eq!(docs.len(), 3);
    assert!(docs
        .iter()
        .all(|doc| doc.schema_version_id() == Some(v2.as_str())));
    assert_eq!(verified_index_count(&db).await, 3);
    for doc_id in doc_ids {
        assert_eq!(
            load_user(&db, &doc_id).await.schema_version_id(),
            Some(v2.as_str())
        );
    }
}

#[tokio::test]
async fn implicit_full_scan_defers_one_collection_instead_of_each_document() {
    let transform_store = Arc::new(SetVerifiedStore::default());
    let options = db::DbOptions::new().with_migration_write_back_batch_size(
        NonZeroUsize::new(2).expect("batch size is non-zero"),
    );
    let mut raw_db = DB::with_options(RegolithStore::in_memory().unwrap(), options).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    for name in ["Deferred-1", "Deferred-2", "Deferred-3"] {
        seed_old_user(&db, &v1, name).await;
    }

    let txn = db.new_txn(true).await.unwrap();
    let fetcher = db::LensedDocFetcher::new(db, txn, transform_store, true);
    let docs = fetcher.get_all("Users").await.unwrap();
    assert_eq!(docs.len(), 3);

    let pending = fetcher.take_pending_write_backs().await;
    assert!(
        pending.documents.is_empty(),
        "full scans must not retain one write-back candidate per stale document"
    );
    assert_eq!(pending.full_scans.get("Users"), Some(&false));

    fetcher.take_txn().await.unwrap().discard().unwrap();
}

#[tokio::test]
async fn implicit_query_flushes_lazy_migration_after_read_snapshot_closes() {
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store);
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();
    let doc_id = seed_old_user(&db, &v1, "Implicit").await;

    let registry = Arc::new(db::DbTransactionRegistry::new(db.clone()));

    // A user-managed read-only transaction remains side-effect free.
    let explicit = registry.begin(true).await.unwrap();
    let explicit_ctx = match registry.get(&explicit) {
        GetTransactionResult::Found(context) => context,
        other => panic!("expected explicit transaction context, got {other:?}"),
    };
    let explicit_docs = explicit_ctx
        .doc_fetcher()
        .get_by_ids("Users", &[doc_id.to_string()])
        .await
        .unwrap()
        .into_docs();
    assert_eq!(
        explicit_docs[0].get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    registry.rollback(&explicit).await.unwrap();
    assert_eq!(verified_index_count(&db).await, 0);

    let fetcher = db::LensedAutoCommitFetcher::new(db.clone());
    let provider = db::DbCollectionProvider::new_arc(db.clone());
    let runner = query::QueryRunner::with_arc_registry_and_provider(fetcher, provider, registry);

    let response = runner
        .execute(QueryRequest::new(
            "query { Users { name verified } }".to_string(),
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(verified_index_count(&db).await, 1);

    let indexed_response = runner
        .execute(QueryRequest::new(
            "query { Users(filter: {verified: {_eq: true}}) { name verified } }".to_string(),
        ))
        .await;
    assert!(
        indexed_response.errors.is_empty(),
        "{:?}",
        indexed_response.errors
    );
    assert_eq!(
        indexed_response.data,
        Some(serde_json::json!({
            "Users": [{"name": "Implicit", "verified": true}]
        }))
    );
}

#[tokio::test]
async fn field_value_fetch_filters_after_lens_transform() {
    let transform_store = Arc::new(StepTransformStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store);
    let db = Arc::new(raw_db);

    db.create_collection(users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "latest").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();
    let doc_id = seed_old_user(&db, &v1, "Filter").await;

    let docs = db::LensedAutoCommitFetcher::new(db)
        .get_by_field_value("Users", "latest", "set")
        .await
        .unwrap();

    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].id(), Some(&doc_id));
}

#[tokio::test]
async fn concurrent_lazy_reads_of_same_document_are_idempotent() {
    let transform_store = Arc::new(BlockingVerifiedStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();
    let doc_id = seed_old_user(&db, &v1, "Concurrent").await;

    transform_store.arm();
    let first_db = db.clone();
    let first_doc_id = doc_id.to_string();
    let first = tokio::spawn(async move {
        db::LensedAutoCommitFetcher::new(first_db)
            .get_by_ids("Users", &[first_doc_id])
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        transform_store.entered.notified(),
    )
    .await
    .expect("first lensed read reached the transform");

    let second_docs = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        db::LensedAutoCommitFetcher::new(db.clone()).get_by_ids("Users", &[doc_id.to_string()]),
    )
    .await
    .expect("second lensed read completed while the first was paused")
    .unwrap()
    .into_docs();

    transform_store.release.notify_one();
    let first_docs = first.await.unwrap().unwrap().into_docs();

    for docs in [&first_docs, &second_docs] {
        assert_eq!(
            docs[0].get("verified"),
            Some(&document::NormalValue::Bool(true))
        );
    }

    let persisted = load_user(&db, &doc_id).await;
    assert_eq!(
        persisted.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(persisted.schema_version_id(), Some(v2.as_str()));
    assert_eq!(
        verified_index_count(&db).await,
        1,
        "concurrent write-back must leave exactly one indexed document"
    );
}

#[tokio::test]
async fn concurrent_update_wins_over_stale_lazy_write_back() {
    let transform_store = Arc::new(BlockingVerifiedStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();
    let doc_id = seed_old_user(&db, &v1, "Alice").await;

    transform_store.arm();
    let query_db = db.clone();
    let query_doc_id = doc_id.to_string();
    let query = tokio::spawn(async move {
        db::LensedAutoCommitFetcher::new(query_db)
            .get_by_ids("Users", &[query_doc_id])
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        transform_store.entered.notified(),
    )
    .await
    .expect("lensed read reached the transform");

    let mut update = document::Document::new();
    update.set_id(doc_id.clone());
    update.set("name", document::NormalValue::String("Bob".to_string()));
    AutoCommitMutator::new(db.clone())
        .update("Users", update, HashSet::from(["name".to_string()]))
        .await
        .unwrap();

    transform_store.release.notify_one();
    query.await.unwrap().unwrap();

    let persisted = load_user(&db, &doc_id).await;
    assert_eq!(
        persisted.get("name"),
        Some(&document::NormalValue::String("Bob".to_string())),
        "lazy write-back must not overwrite a mutation committed after its read snapshot"
    );
    assert_eq!(
        persisted.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(persisted.schema_version_id(), Some(v2.as_str()));
}

/// The streaming counterpart of `implicit_full_scan_defers_one_collection_instead_of_each_document`:
/// a streamed read must defer per document (matching Go's read-through
/// `updateDataStore`, called per document from `FetchNext`), never as one
/// full-scan marker - `defer_full_scan_write_back` discards per-document
/// candidates on the assumption the whole collection was already read, which
/// is false for a stream a caller may stop early.
#[tokio::test]
async fn streaming_lensed_read_defers_per_document_not_full_scan() {
    let transform_store = Arc::new(SetVerifiedStore::default());
    let options = db::DbOptions::new().with_migration_write_back_batch_size(
        NonZeroUsize::new(2).expect("batch size is non-zero"),
    );
    let mut raw_db = DB::with_options(RegolithStore::in_memory().unwrap(), options).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(indexed_users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_placeholder_version(&db, "placeholder").await;
    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    for name in ["Deferred-1", "Deferred-2", "Deferred-3"] {
        seed_old_user(&db, &v1, name).await;
    }

    let txn = db.new_txn(true).await.unwrap();
    let fetcher = db::LensedDocFetcher::new(db, txn, transform_store, true);

    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    let mut docs = Vec::new();
    while let Some((doc, _is_deleted)) = stream.next().await.unwrap() {
        docs.push(doc);
    }
    assert_eq!(docs.len(), 3);
    assert!(docs
        .iter()
        .all(|doc| doc.get("verified") == Some(&document::NormalValue::Bool(true))));
    drop(stream);

    let pending = fetcher.take_pending_write_backs().await;
    assert!(
        pending.full_scans.is_empty(),
        "a stream must never mark a whole-collection full scan"
    );
    assert_eq!(
        pending.documents.len(),
        3,
        "a stream must defer one write-back candidate per migrated document"
    );

    fetcher.take_txn().await.unwrap().discard().unwrap();
}

/// A lens-migrated collection must return correct, migrated documents through
/// the streaming path, and persist them exactly as the eager auto-commit path
/// does - proving `LensedAutoCommitFetcher::stream_all_with_deleted` isn't
/// just correct in isolation but matches `get_all`'s observable contract.
#[tokio::test]
async fn streaming_auto_commit_read_migrates_and_persists() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::from_arc(store.clone()).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let doc_id = seed_user(&db).await;
    let v2 = add_verified_version(&db).await;

    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    let fetcher = db::LensedAutoCommitFetcher::new(db.clone());
    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    let mut docs = Vec::new();
    while let Some((doc, _is_deleted)) = stream.next().await.unwrap() {
        docs.push(doc);
    }
    drop(stream);

    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0].get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(docs[0].schema_version_id(), Some(v2.as_str()));
    assert_eq!(transform_store.transform_calls.load(Ordering::SeqCst), 1);

    let persisted = load_user(&db, &doc_id).await;
    assert_eq!(
        persisted.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );
    assert_eq!(persisted.schema_version_id(), Some(v2.as_str()));
}

/// The early-termination counterpart of
/// `streaming_auto_commit_read_migrates_and_persists`: a `LimitNode` stops
/// pulling long before the write-back batch boundary, and `ScanNode::close`
/// then tears the stream down. Documents already migrated through the lens
/// must be persisted at that point - otherwise every limited query re-runs the
/// transform forever, and the eager path's write-back is silently lost.
#[tokio::test]
async fn streaming_auto_commit_read_persists_after_partial_consumption() {
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    raw_db.set_lens_store(transform_store.clone());
    let db = Arc::new(raw_db);

    db.create_collection(users_schema()).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = add_verified_version(&db).await;

    db.set_migration(
        LensConfig::new(
            &v1,
            &v2,
            LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
        ),
        None,
    )
    .await
    .unwrap();

    for name in ["Partial-1", "Partial-2", "Partial-3"] {
        seed_old_user(&db, &v1, name).await;
    }

    let fetcher = db::LensedAutoCommitFetcher::new(db.clone());
    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();

    // One document out of three, well under the default write-back batch size.
    let (migrated, _is_deleted) = stream
        .next()
        .await
        .unwrap()
        .expect("stream yields the first document");
    assert_eq!(
        migrated.get("verified"),
        Some(&document::NormalValue::Bool(true))
    );

    stream.close().await.unwrap();
    drop(stream);

    let doc_id = migrated.id().expect("migrated document keeps its DocID");
    let persisted = load_user(&db, doc_id).await;
    assert_eq!(
        persisted.get("verified"),
        Some(&document::NormalValue::Bool(true)),
        "a partially consumed stream must persist the documents it did migrate"
    );
    assert_eq!(persisted.schema_version_id(), Some(v2.as_str()));
}
