use super::*;
use document::NormalValue;
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::atomic::{AtomicBool, Ordering};
use storage::backends::MemoryStore;

use crate::write::doc::DbDocMutator;

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

/// The stream must be observationally identical to the eager path.
#[tokio::test]
async fn stream_matches_get_all_with_deleted_ordering_and_content() {
    let db = fixture_with_docs(5).await;
    let fetcher = AutoCommitFetcher::new(db);

    let eager = fetcher.get_all_with_deleted("Users", false).await.unwrap();

    let mut streamed = Vec::new();
    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    while let Some(pair) = stream.next().await.unwrap() {
        streamed.push(pair);
    }

    assert_eq!(streamed.len(), eager.len());
    for (s, e) in streamed.iter().zip(eager.iter()) {
        assert_eq!(s.0.id(), e.0.id());
        assert_eq!(s.1, e.1);
    }
}

/// Partial consumption must not error and must not require draining -
/// `AutoCommitDocStream` owns its read transaction, unlike
/// `CollectionDocStream`'s own tests where the transaction outlives the
/// stream, so this exercises the `Drop` discard path specifically.
#[tokio::test]
async fn stream_may_be_dropped_after_partial_consumption() {
    let db = fixture_with_docs(20).await;
    let fetcher = AutoCommitFetcher::new(db);

    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(stream.next().await.unwrap().is_some());
    }
    drop(stream);
}

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

/// Draining a stream to exhaustion must close the storage iterator, not just
/// drop it: `release_read_txn` clears `inner`, so a later `ScanNode::close`
/// cannot reach it.
#[tokio::test]
async fn exhaustion_closes_the_inner_stream() {
    let db = fixture_with_docs(3).await;
    let closed = Arc::new(AtomicBool::new(false));

    let inner = AutoCommitFetcher::new(db.clone())
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    let mut stream = AutoCommitDocStream::<MemoryStore> {
        inner: Some(Box::new(RecordingStream {
            inner,
            closed: closed.clone(),
        })),
        txn: std::sync::Mutex::new(None),
    };

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
    let closed = Arc::new(AtomicBool::new(false));

    let inner = AutoCommitFetcher::new(db.clone())
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    let mut stream = AutoCommitDocStream::<MemoryStore> {
        inner: Some(Box::new(RecordingStream {
            inner,
            closed: closed.clone(),
        })),
        txn: std::sync::Mutex::new(None),
    };

    assert!(
        stream.next().await.unwrap().is_some(),
        "the stream must yield a document, or exhaustion closes it before the explicit close does"
    );
    stream.close().await.unwrap();

    assert!(closed.load(Ordering::SeqCst));
}

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

/// The read transaction is released even when closing the inner stream fails.
/// A leaked read transaction is worse than a surfaced cleanup error, so the
/// release is unconditional and the error is returned afterwards.
#[tokio::test]
async fn close_releases_the_txn_even_when_the_inner_close_fails() {
    let db = fixture_with_docs(1).await;
    let txn = db.new_txn(true).await.unwrap();

    let mut stream = AutoCommitDocStream {
        inner: Some(Box::new(FailingCloseStream)),
        txn: std::sync::Mutex::new(Some(txn)),
    };

    let err = stream.close().await.unwrap_err();
    assert!(format!("{err}").contains("boom-close"));
    assert!(
        stream.txn.lock().unwrap().is_none(),
        "the read transaction must be released even when the inner close fails"
    );
}
