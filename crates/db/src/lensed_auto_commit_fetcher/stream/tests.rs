use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use document::{DocID, Document, NormalValue};
use lens::{
    LensConfig, LensDocResultStream, LensDocStream, LensModule, TransformId, TransformStore,
};
use query::doc_stream::DocStream;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

use super::LensedAutoCommitDocStream;
use crate::lensed_auto_commit_fetcher::migration::MigrationWriteBack;
use crate::write::doc::DbDocMutator;
use crate::{Collection, LensedAutoCommitFetcher, DB};

fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

async fn fixture_with_docs(n: usize) -> Arc<DB<MemoryStore>> {
    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
    db.create_collection(test_schema()).await.unwrap();

    for i in 0..n {
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(db.clone(), txn);
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(format!("user-{i}")));
        mutator.create("Users", doc).await.unwrap();
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();
    }

    db
}

/// Duplicated from `auto_commit_fetcher::tests::RecordingStream`: that struct
/// is private to its own module tree, unreachable from here.
struct RecordingStream {
    inner: Box<dyn DocStream>,
    closed: Arc<AtomicBool>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocStream for RecordingStream {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        self.inner.next().await
    }

    async fn close(&mut self) -> query::error::Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.inner.close().await
    }
}

fn wrapped_stream(
    db: Arc<DB<MemoryStore>>,
    collection: Collection,
    inner: Box<dyn DocStream>,
    closed: Arc<AtomicBool>,
) -> LensedAutoCommitDocStream<MemoryStore> {
    LensedAutoCommitDocStream {
        inner: Some(Box::new(RecordingStream { inner, closed })),
        txn: std::sync::Mutex::new(None),
        fetcher: LensedAutoCommitFetcher::new(db),
        collection,
        migration_generation: 0,
        has_migrations: false,
        preloaded_history: None,
        write_backs: Vec::new(),
    }
}

/// Draining a stream to exhaustion must close the storage iterator, not just
/// drop it: `release_read_txn` clears `inner`, so a later `ScanNode::close`
/// cannot reach it.
#[tokio::test]
async fn exhaustion_closes_the_inner_stream() {
    let db = fixture_with_docs(3).await;
    let collection = db.get_collection("Users").unwrap().unwrap();
    let closed = Arc::new(AtomicBool::new(false));

    let inner = LensedAutoCommitFetcher::new(db.clone())
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    let mut stream = wrapped_stream(db.clone(), collection, inner, closed.clone());

    while stream.next().await.unwrap().is_some() {}

    assert!(
        closed.load(Ordering::SeqCst),
        "exhaustion dropped the inner stream without closing it"
    );
}

/// An explicit close before exhaustion must also reach it.
#[tokio::test]
async fn explicit_close_closes_the_inner_stream() {
    let db = fixture_with_docs(3).await;
    let collection = db.get_collection("Users").unwrap().unwrap();
    let closed = Arc::new(AtomicBool::new(false));

    let inner = LensedAutoCommitFetcher::new(db.clone())
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    let mut stream = wrapped_stream(db.clone(), collection, inner, closed.clone());

    stream.next().await.unwrap();
    stream.close().await.unwrap();

    assert!(closed.load(Ordering::SeqCst));
}

/// A `TransformStore` that always fails, so the persist-side re-transform
/// inside `persist_migrated_document_batch` fails deterministically.
#[derive(Default)]
struct AlwaysFailingTransformStore {
    transforms: RwLock<HashSet<TransformId>>,
}

#[async_trait]
impl TransformStore for AlwaysFailingTransformStore {
    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        let id = TransformId::new(format!("always-failing-transform-{}", config.lenses.len()));
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
        _id: &TransformId,
        _docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        Err(lens::Error::WasmExecution("boom-persist".to_string()))
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

/// A `DocStream` whose `close()` always fails, standing in for a storage
/// iterator that cannot be closed cleanly.
struct FailingCloseStream;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocStream for FailingCloseStream {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        Ok(None)
    }

    async fn close(&mut self) -> query::error::Result<()> {
        Err(query::error::QueryError::execution("boom-close"))
    }
}

/// When both the inner close and the write-back flush fail on the same
/// `close()` call, neither error may be dropped: the double-failure branch
/// must join them into one.
///
/// The write-back candidate is crafted directly (rather than produced by a
/// real read pass) with a `migration_generation` older than the collection's
/// current one - the same state a real candidate would be in if the
/// migration graph changed between the read and the flush. That mismatch is
/// what forces `persist_migrated_document_batch` down its real re-transform
/// path (`migration.rs:465-476`) instead of reusing the cached
/// `migrated_document`, so the persist-side failure below comes from genuine
/// production code, not a stub standing in for it.
#[tokio::test]
async fn close_joins_inner_close_error_with_persist_error() {
    let transform_store = Arc::new(AlwaysFailingTransformStore::default());
    let mut raw_db = DB::new(MemoryStore::new()).unwrap();
    raw_db.lens_store = transform_store;
    let db = Arc::new(raw_db);

    let mut migratable_schema = test_schema();
    migratable_schema.is_materialized = true;
    db.create_collection(migratable_schema).await.unwrap();
    let v1 = db
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let v2 = db
        .patch_collection(
            "Users",
            r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"verified","Kind":"Boolean"}}]"#,
            None,
        )
        .await
        .unwrap()
        .version_id;
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

    let collection = db.get_collection("Users").unwrap().unwrap();
    let index_manager = crate::index_manager::IndexManager::from_collection(
        collection.resolved_root_id(),
        collection.schema(),
    )
    .unwrap();

    let doc_id = DocID::new_v0_from_seed("close-joins-errors");
    let mut old_doc = Document::new();
    old_doc.set_id(doc_id.clone());
    old_doc.set("name", NormalValue::String("Stale".to_string()));
    old_doc.set_schema_version_id(&v1);

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
        .create_with_indexes(&datastore, &old_doc, doc_short_id, &index_manager)
        .await
        .unwrap();
    collection
        .save_with_datastore(&datastore, &old_doc, doc_short_id)
        .await
        .unwrap();
    drop(datastore);
    drop(systemstore);
    txn.commit().await.unwrap();

    let read_txn = db.new_txn(true).await.unwrap();
    let current_doc = collection
        .get_by_doc_id(
            &read_txn.datastore().unwrap(),
            &read_txn.systemstore().unwrap(),
            &doc_id,
        )
        .await
        .unwrap()
        .unwrap();
    read_txn.discard().unwrap();

    let stale_generation = db.migration_generation().wrapping_add(1);
    let write_backs = vec![MigrationWriteBack {
        source_document: current_doc.clone(),
        migrated_document: current_doc,
        migration_generation: stale_generation,
    }];

    let mut stream = LensedAutoCommitDocStream::<MemoryStore> {
        inner: Some(Box::new(FailingCloseStream)),
        txn: std::sync::Mutex::new(None),
        fetcher: LensedAutoCommitFetcher::new(db),
        collection,
        migration_generation: 0,
        has_migrations: false,
        preloaded_history: None,
        write_backs,
    };

    let err = stream.close().await.unwrap_err().to_string();
    assert!(err.contains("boom-close"), "missing close error: {err}");
    assert!(err.contains("boom-persist"), "missing persist error: {err}");
}
