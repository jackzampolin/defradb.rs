//! A pull-based document source for index backfill.
//!
//! Backfill used to take `&[(u64, Document)]`, which meant the caller loaded
//! the whole collection before indexing started. At 768-dimension embeddings
//! that is gigabytes for a large collection, so the source yields one document
//! at a time and nothing holds more than the one being indexed.

use async_trait::async_trait;
use defra_core::thread_bounds::MaybeSend;
use document::Document;

use crate::index::error::Result;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait DocumentSource: MaybeSend {
    /// The next `(doc_short_id, document)`, or `None` when exhausted.
    async fn next(&mut self) -> Result<Option<(u64, Document)>>;
}

/// Adapts an already-materialised slice, for callers that legitimately hold one
/// (a migration that just produced the documents, and tests).
pub struct SliceSource<'a> {
    documents: &'a [(u64, Document)],
    next: usize,
}

impl<'a> SliceSource<'a> {
    pub fn new(documents: &'a [(u64, Document)]) -> Self {
        Self { documents, next: 0 }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocumentSource for SliceSource<'_> {
    async fn next(&mut self) -> Result<Option<(u64, Document)>> {
        let item = self.documents.get(self.next);
        self.next += 1;
        Ok(item.map(|(id, doc)| (*id, doc.clone())))
    }
}
