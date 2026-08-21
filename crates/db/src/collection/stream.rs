//! Pull-based document source over a collection's blob prefix.

use async_lock::Mutex;
use async_trait::async_trait;
use datastore::NamespaceView;
use document::Document;
use query::doc_stream::DocStream;
use storage::corekv::Iterator as KvIterator;
use storage::keys::doc_id_index::decode_doc_short_id;

use crate::collection::Collection;

/// Streams a collection's documents, resolving each one's public DocID,
/// deletion status and schema version as it is yielded.
///
/// Owns its storage handles: the collection loader returns owned values and
/// `Box<dyn Iterator>` is `'static`, so nothing here borrows the transaction.
/// The iterator is wrapped in a `Mutex` purely so the (`Send`-but-not-`Sync`)
/// trait object doesn't stop `CollectionDocStream` satisfying `DocStream`'s
/// `MaybeSendSync` bound; every access goes through `&mut self`, so it never
/// actually contends.
pub struct CollectionDocStream {
    collection: Collection,
    datastore: NamespaceView,
    systemstore: NamespaceView,
    iter: Mutex<Box<dyn KvIterator>>,
    prefix_len: usize,
    show_deleted: bool,
    exhausted: bool,
    last_short_id: Option<u64>,
}

impl CollectionDocStream {
    /// Wrap an already-opened, prefix-scoped iterator over a collection's
    /// document blobs.
    pub(crate) fn new(
        collection: Collection,
        datastore: NamespaceView,
        systemstore: NamespaceView,
        iter: Box<dyn KvIterator>,
        prefix_len: usize,
        show_deleted: bool,
    ) -> Self {
        Self {
            collection,
            datastore,
            systemstore,
            iter: Mutex::new(iter),
            prefix_len,
            show_deleted,
            exhausted: false,
            last_short_id: None,
        }
    }

    /// The short id of the document `next` most recently yielded. Backfill
    /// needs it, and the `DocStream` contract has no room for it.
    pub(crate) fn last_short_id(&self) -> Option<u64> {
        self.last_short_id
    }

    /// Fuse the stream and wrap `e` as an execution error prefixed with
    /// `context`. Any resolution failure (decode, DocID lookup, deletion
    /// check, version lookup) must stop the stream the same way a raw
    /// iterator error does, so a caller that polls again after an error
    /// gets a consistent `Ok(None)` rather than silently resuming from the
    /// next raw entry.
    fn fuse_err(&mut self, context: &str, e: impl std::fmt::Display) -> query::error::QueryError {
        self.exhausted = true;
        query::error::QueryError::execution(format!("{}: {}", context, e))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocStream for CollectionDocStream {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            let pair = match self.iter.get_mut().next().await {
                Ok(Some(pair)) => pair,
                Ok(None) => {
                    self.exhausted = true;
                    return Ok(None);
                }
                Err(e) => {
                    self.exhausted = true;
                    return Err(query::error::QueryError::execution(format!(
                        "storage error: {}",
                        e
                    )));
                }
            };

            let Ok(doc_short_id) = decode_doc_short_id(&pair.key[self.prefix_len..]) else {
                continue;
            };

            let mut doc = Document::from_cbor(&pair.value)
                .map_err(|e| self.fuse_err("document decode error", e))?;

            let Some(doc_id_str) = crate::docid::map::get_doc_id(&self.systemstore, doc_short_id)
                .await
                .map_err(|e| self.fuse_err("storage error", e))?
            else {
                continue;
            };
            let Ok(doc_id) = doc_id_str.parse() else {
                continue;
            };
            doc.set_id(doc_id);

            let is_deleted = self
                .collection
                .is_deleted(&self.datastore, doc_short_id)
                .await
                .map_err(|e| self.fuse_err("storage error", e))?;
            if is_deleted && !self.show_deleted {
                continue;
            }

            if let Some(version) = self
                .collection
                .load_version(&self.datastore, doc_short_id)
                .await
                .map_err(|e| self.fuse_err("storage error", e))?
            {
                doc.set_schema_version_id(version);
            }

            self.last_short_id = Some(doc_short_id);
            return Ok(Some((doc, is_deleted)));
        }
    }

    async fn close(&mut self) -> query::error::Result<()> {
        self.exhausted = true;
        self.iter
            .get_mut()
            .close()
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))
    }
}

/// A [`DocStream`] over a known list of document short ids.
///
/// One point read per id, pulled as the consumer asks, so nothing here holds
/// more than a single document and a consumer that stops early stops the reads.
/// That is the difference between a query narrowed by an index costing what it
/// asked for and costing the size of the collection.
///
/// Ids that are absent are skipped: a caller holding an id from an index may
/// hold one whose document has since gone.
pub struct ShortIdDocStream {
    collection: Collection,
    datastore: NamespaceView,
    systemstore: NamespaceView,
    doc_short_ids: Vec<u64>,
    position: usize,
    show_deleted: bool,
}

impl ShortIdDocStream {
    pub fn new(
        collection: Collection,
        datastore: NamespaceView,
        systemstore: NamespaceView,
        doc_short_ids: Vec<u64>,
        show_deleted: bool,
    ) -> Self {
        Self {
            collection,
            datastore,
            systemstore,
            doc_short_ids,
            position: 0,
            show_deleted,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocStream for ShortIdDocStream {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        while self.position < self.doc_short_ids.len() {
            let doc_short_id = self.doc_short_ids[self.position];
            self.position += 1;

            let found = self
                .collection
                .get_by_short_ids(
                    &self.datastore,
                    &self.systemstore,
                    &[doc_short_id],
                    self.show_deleted,
                )
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!("storage error: {}", e))
                })?;

            if let Some((_, doc, deleted)) = found.into_iter().next() {
                return Ok(Some((doc, deleted)));
            }
        }
        Ok(None)
    }
}

/// Feeds index backfill from a collection without materialising it.
///
/// `IndexManager::bulk_index` used to take the whole collection as a slice; at
/// 768-dimension embeddings that is gigabytes before indexing starts.
pub struct BackfillSource {
    inner: CollectionDocStream,
}

impl BackfillSource {
    /// Opens a prefix scan over the collection's document blobs. Deleted
    /// documents are skipped, which is what backfill wants: they carry no
    /// index entries.
    pub async fn open(
        collection: Collection,
        datastore: NamespaceView,
        systemstore: NamespaceView,
    ) -> crate::error::Result<Self> {
        let prefix = collection.collection_key_prefix();
        let prefix_len = prefix.len();
        let iter = datastore
            .iterator(storage::corekv::IterOptions::new().with_prefix(prefix))
            .await
            .map_err(crate::error::Error::Storage)?;

        Ok(Self {
            inner: CollectionDocStream::new(
                collection,
                datastore,
                systemstore,
                iter,
                prefix_len,
                false,
            ),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl crate::index::manager::DocumentSource for BackfillSource {
    async fn next(&mut self) -> crate::index::error::Result<Option<(u64, Document)>> {
        use query::doc_stream::DocStream;

        let Some((doc, _)) = self
            .inner
            .next()
            .await
            .map_err(|e| crate::index::error::Error::Other(e.to_string()))?
        else {
            return Ok(None);
        };
        // Set immediately before the document was yielded.
        let short_id = self.inner.last_short_id().unwrap_or(0);
        Ok(Some((short_id, doc)))
    }
}
