//! Pull-based document source for query execution.

use async_trait::async_trait;
use document::Document;
use query_types::error::Result;
use storage::corekv::MaybeSendSync;

/// A pull-based source of documents paired with their deletion status.
///
/// Returned by [`crate::fetcher::DocFetcher::stream_all_with_deleted`]. Each
/// call yields at most one document, so a consumer that stops pulling stops
/// the underlying storage work.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait DocStream: MaybeSendSync {
    /// Advance the stream. `Ok(None)` means exhausted; further calls keep
    /// returning `Ok(None)`.
    async fn next(&mut self) -> Result<Option<(Document, bool)>>;

    /// Finish with the stream, whether or not it was exhausted.
    ///
    /// A consumer that stops pulling early (a satisfied `LimitNode` above a
    /// `ScanNode`) must call this before dropping the stream: `Drop` cannot
    /// await, so this is the only point at which a stream can flush work it
    /// deferred while yielding documents. The default is a no-op, which is
    /// correct for streams that defer nothing.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A [`DocStream`] over an already-materialized vector.
///
/// This is the default fallback for fetchers that have no streaming
/// implementation: correct, but it does not avoid the eager fetch.
pub struct VecStream {
    pairs: std::vec::IntoIter<(Document, bool)>,
}

impl VecStream {
    /// Wrap pre-fetched documents as a stream.
    pub fn new(pairs: Vec<(Document, bool)>) -> Self {
        Self {
            pairs: pairs.into_iter(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocStream for VecStream {
    async fn next(&mut self) -> Result<Option<(Document, bool)>> {
        Ok(self.pairs.next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use document::Document;

    #[tokio::test]
    async fn vec_stream_yields_all_pairs_in_order_then_none() {
        let pairs = vec![
            (Document::new(), false),
            (Document::new(), true),
            (Document::new(), false),
        ];
        let mut stream = VecStream::new(pairs);

        assert_eq!(stream.next().await.unwrap().map(|(_, d)| d), Some(false));
        assert_eq!(stream.next().await.unwrap().map(|(_, d)| d), Some(true));
        assert_eq!(stream.next().await.unwrap().map(|(_, d)| d), Some(false));
        assert!(stream.next().await.unwrap().is_none());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn vec_stream_empty_yields_none_immediately() {
        let mut stream = VecStream::new(Vec::new());
        assert!(stream.next().await.unwrap().is_none());
    }
}
