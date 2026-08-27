use async_trait::async_trait;
use document::Document;
use query::doc_stream::DocStream;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct RecordingStream {
    pub inner: Box<dyn DocStream>,
    pub closed: Arc<AtomicBool>,
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

pub struct FailingCloseStream;

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
