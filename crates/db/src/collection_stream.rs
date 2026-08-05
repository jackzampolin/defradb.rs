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
pub(crate) struct CollectionDocStream {
    collection: Collection,
    datastore: NamespaceView,
    systemstore: NamespaceView,
    iter: Mutex<Box<dyn KvIterator>>,
    prefix_len: usize,
    show_deleted: bool,
    exhausted: bool,
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
        }
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

            let Some(doc_id_str) = crate::doc_id_map::get_doc_id(&self.systemstore, doc_short_id)
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

            return Ok(Some((doc, is_deleted)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use document::NormalValue;
    use query::mutator::DocMutator;
    use query::runner::DocFetcher;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use std::sync::Arc;
    use storage::backends::MemoryStore;

    use crate::database::DB;
    use crate::doc_fetcher::DbDocFetcher;
    use crate::doc_mutator::DbDocMutator;

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

    /// Create a DB with a `Users` collection and `n` committed documents,
    /// named `user-0`..`user-{n-1}` in insertion order.
    async fn fixture_with_docs(n: usize) -> (Arc<DB<MemoryStore>>, String) {
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

        (db, "Users".to_string())
    }

    async fn fetcher(db: &Arc<DB<MemoryStore>>) -> DbDocFetcher<MemoryStore> {
        DbDocFetcher::new(db.new_txn(true).await.unwrap())
    }

    /// Delete the `n`th document in insertion order.
    async fn delete_nth_document(db: &Arc<DB<MemoryStore>>, collection_name: &str, n: usize) {
        let doc_id = {
            let f = fetcher(db).await;
            let docs = f.get_all(collection_name).await.unwrap();
            docs[n].id().unwrap().clone()
        };

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(db.clone(), txn);
        mutator.delete(collection_name, &doc_id).await.unwrap();
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();
    }

    /// Overwrite the `n`th document's blob (in insertion order) with bytes
    /// that fail to decode as CBOR.
    async fn corrupt_nth_document(db: &Arc<DB<MemoryStore>>, collection_name: &str, n: usize) {
        let txn = db.new_txn(false).await.unwrap();
        let shared = Arc::new(Mutex::new(Some(txn)));
        {
            let (collection, datastore, systemstore) =
                crate::collection_loader::get_collection_with_lazy_load(&shared, collection_name)
                    .await
                    .unwrap();
            let entries = collection
                .get_all_with_datastore_short_ids(&datastore, &systemstore, false)
                .await
                .unwrap();
            let doc_short_id = entries[n].0;
            datastore
                .set(&collection.doc_key(doc_short_id), b"not valid cbor")
                .await
                .unwrap();
        }

        let txn = shared.lock().await.take().unwrap();
        txn.commit().await.unwrap();
    }

    /// The stream must be observationally identical to the eager path.
    #[tokio::test]
    async fn stream_matches_get_all_with_deleted_ordering_and_content() {
        let (db, collection_name) = fixture_with_docs(5).await;
        let fetcher = fetcher(&db).await;

        let eager = fetcher
            .get_all_with_deleted(&collection_name, false)
            .await
            .unwrap();

        let mut streamed = Vec::new();
        let mut stream = fetcher
            .stream_all_with_deleted(&collection_name, false)
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

    /// Deleted documents interleaved must be skipped identically.
    #[tokio::test]
    async fn stream_skips_deleted_when_not_showing_deleted() {
        let (db, collection_name) = fixture_with_docs(5).await;
        delete_nth_document(&db, &collection_name, 1).await;
        delete_nth_document(&db, &collection_name, 3).await;
        let fetcher = fetcher(&db).await;

        let mut streamed = Vec::new();
        let mut stream = fetcher
            .stream_all_with_deleted(&collection_name, false)
            .await
            .unwrap();
        while let Some(pair) = stream.next().await.unwrap() {
            streamed.push(pair);
        }

        assert_eq!(streamed.len(), 3);
        assert!(streamed.iter().all(|(_, deleted)| !deleted));
    }

    /// Partial consumption must not error and must not require draining.
    #[tokio::test]
    async fn stream_may_be_dropped_after_partial_consumption() {
        let (db, collection_name) = fixture_with_docs(100).await;
        let fetcher = fetcher(&db).await;

        let mut stream = fetcher
            .stream_all_with_deleted(&collection_name, false)
            .await
            .unwrap();
        for _ in 0..3 {
            assert!(stream.next().await.unwrap().is_some());
        }
        drop(stream);
    }

    /// A resolution error (not just a raw iterator error) must fuse the
    /// stream: polling again after the error must not silently resume from
    /// the next entry.
    #[tokio::test]
    async fn stream_fuses_after_resolution_error() {
        let (db, collection_name) = fixture_with_docs(3).await;
        corrupt_nth_document(&db, &collection_name, 0).await;
        let fetcher = fetcher(&db).await;

        let mut stream = fetcher
            .stream_all_with_deleted(&collection_name, false)
            .await
            .unwrap();

        assert!(stream.next().await.is_err());
        assert!(stream.next().await.unwrap().is_none());
    }
}
