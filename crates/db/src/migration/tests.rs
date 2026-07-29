use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::StreamExt;
use lens::{
    LensConfig, LensDocResultStream, LensDocStream, LensModule, TransformId, TransformStore,
};
use query::txn::{GetTransactionResult, TransactionRegistry};
use query::{DocFetcher, DocMutator, QueryExecutor, QueryRequest};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::corekv::IterOptions;
use storage::index::IndexIterator;
use storage::MemoryStore;
use tokio::sync::Notify;

use crate::{AutoCommitMutator, DB};

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
    armed: AtomicBool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl BlockingVerifiedStore {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
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
        let should_block = self.armed.swap(false, Ordering::SeqCst);
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

fn users_schema() -> CollectionVersion {
    let mut schema = CollectionVersion::new(
        "Users",
        "users-v1",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    schema.is_materialized = true;
    schema
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

async fn seed_user(db: &Arc<DB<MemoryStore>>) -> document::DocID {
    let mut doc = document::Document::new();
    doc.set("name", document::NormalValue::String("Alice".to_string()));
    AutoCommitMutator::new(db.clone())
        .create("Users", doc)
        .await
        .unwrap()
        .doc_id
}

async fn add_verified_version(db: &Arc<DB<MemoryStore>>) -> String {
    db.patch_collection(
        "Users",
        r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"verified","Kind":"Boolean"}}]"#,
        None,
    )
    .await
    .unwrap()
    .version_id
}

async fn add_placeholder_version(db: &Arc<DB<MemoryStore>>, field_name: &str) -> String {
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

async fn seed_old_user(db: &Arc<DB<MemoryStore>>, version_id: &str, name: &str) -> document::DocID {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let index_manager = crate::index_manager::IndexManager::from_collection(
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
    let doc_short_id = crate::doc_id_map::next_doc_short_id(&systemstore)
        .await
        .unwrap();
    crate::doc_id_map::set_doc_id_mapping(
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

async fn verified_index_count(db: &Arc<DB<MemoryStore>>) -> usize {
    let collection = db.get_collection("Users").unwrap().unwrap();
    let index_manager = crate::index_manager::IndexManager::from_collection(
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

async fn load_user(db: &Arc<DB<MemoryStore>>, doc_id: &document::DocID) -> document::Document {
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

async fn load_user_blob(db: &Arc<DB<MemoryStore>>, doc_id: &document::DocID) -> Vec<u8> {
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

async fn namespace_counts(db: &Arc<DB<MemoryStore>>) -> (usize, usize, usize) {
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
    let store = Arc::new(MemoryStore::new());
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::from_arc(store.clone()).unwrap();
    raw_db.lens_store = transform_store.clone();
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
    let fetcher = crate::LensedAutoCommitFetcher::new(db.clone());
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
    let store = Arc::new(MemoryStore::new());
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
    let mut raw_db = DB::new(MemoryStore::new()).unwrap();
    raw_db.lens_store = transform_store;
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

    let fetcher = crate::LensedAutoCommitFetcher::new(db.clone());
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
    let mut raw_db = DB::new(MemoryStore::new()).unwrap();
    raw_db.lens_store = transform_store;
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

    let fetcher = crate::LensedAutoCommitFetcher::new(db.clone());
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
async fn implicit_query_flushes_lazy_migration_after_read_snapshot_closes() {
    let transform_store = Arc::new(SetVerifiedStore::default());
    let mut raw_db = DB::new(MemoryStore::new()).unwrap();
    raw_db.lens_store = transform_store;
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

    let registry = Arc::new(crate::DbTransactionRegistry::new(db.clone()));

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

    let fetcher = crate::LensedAutoCommitFetcher::new(db.clone());
    let provider = crate::DbCollectionProvider::new_arc(db.clone());
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
    let mut raw_db = DB::new(MemoryStore::new()).unwrap();
    raw_db.lens_store = transform_store;
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

    let docs = crate::LensedAutoCommitFetcher::new(db)
        .get_by_field_value("Users", "latest", "set")
        .await
        .unwrap();

    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].id(), Some(&doc_id));
}

#[tokio::test]
async fn concurrent_update_wins_over_stale_lazy_write_back() {
    let transform_store = Arc::new(BlockingVerifiedStore::default());
    let mut raw_db = DB::new(MemoryStore::new()).unwrap();
    raw_db.lens_store = transform_store.clone();
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
        crate::LensedAutoCommitFetcher::new(query_db)
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
