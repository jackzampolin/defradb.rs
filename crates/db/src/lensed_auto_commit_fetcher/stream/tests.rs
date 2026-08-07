use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use document::{Document, NormalValue};
use query::doc_stream::DocStream;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

use super::LensedAutoCommitDocStream;
use crate::doc_mutator::DbDocMutator;
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
