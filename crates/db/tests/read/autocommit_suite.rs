use crate::common::fixture::fixture_with_docs;
use crate::common::stream::FailingCloseStream;
use crate::common::stream::RecordingStream;
use db::read::autocommit::*;
use query::doc_stream::DocStream;
use query::runner::DocFetcher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use storage::backends::MemoryStore;

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
    let mut stream = AutoCommitDocStream::<MemoryStore>::without_txn(Box::new(RecordingStream {
        inner,
        closed: closed.clone(),
    }));

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
    let mut stream = AutoCommitDocStream::<MemoryStore>::without_txn(Box::new(RecordingStream {
        inner,
        closed: closed.clone(),
    }));

    assert!(
        stream.next().await.unwrap().is_some(),
        "the stream must yield a document, or exhaustion closes it before the explicit close does"
    );
    stream.close().await.unwrap();

    assert!(closed.load(Ordering::SeqCst));
}

/// The read transaction is released even when closing the inner stream fails.
/// A leaked read transaction is worse than a surfaced cleanup error, so the
/// release is unconditional and the error is returned afterwards.
#[tokio::test]
async fn close_releases_the_txn_even_when_the_inner_close_fails() {
    let db = fixture_with_docs(1).await;
    let txn = db.new_txn(true).await.unwrap();

    let mut stream = AutoCommitDocStream::new(Box::new(FailingCloseStream), txn);

    let err = stream.close().await.unwrap_err();
    assert!(format!("{err}").contains("boom-close"));
    assert!(
        stream.txn_released(),
        "the read transaction must be released even when the inner close fails"
    );
}
