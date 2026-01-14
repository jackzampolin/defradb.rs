/// Collection struct for DefraDB matching Go's internal/db/collection.go.
///
/// A Collection represents a set of documents that share the same schema.
/// It provides CRUD operations for documents.
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use document::{DocID, Document};
use schema::CollectionVersion;
use storage::corekv::{IterOptions, Store};

/// Key prefix for document data in datastore.
const DOC_KEY_PREFIX: &[u8] = b"/d/";

/// A collection of documents with a shared schema.
pub struct Collection {
    /// The collection schema definition.
    def: CollectionVersion,
}

impl Collection {
    /// Create a new collection with the given schema definition.
    pub fn new(def: CollectionVersion) -> Self {
        Self { def }
    }

    /// Get the collection name.
    pub fn name(&self) -> &str {
        &self.def.name
    }

    /// Get the collection ID.
    pub fn collection_id(&self) -> &str {
        &self.def.collection_id
    }

    /// Get the collection schema.
    pub fn schema(&self) -> &CollectionVersion {
        &self.def
    }

    /// Create a new document in this collection.
    pub async fn create<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<DocID> {
        // Generate document ID if not present
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        // Check if document already exists
        let key = self.doc_key(&doc_id);
        if txn
            .datastore()
            .has(&key)
            .await
            .map_err(Error::Storage)?
        {
            return Err(Error::InvalidDocument(format!(
                "Document with ID {} already exists",
                doc_id
            )));
        }

        // Serialize document to CBOR
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Store document
        txn.datastore()
            .set(&key, &data)
            .await
            .map_err(Error::Storage)?;

        Ok(doc_id)
    }

    /// Get a document by ID.
    pub async fn get<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<Option<Document>> {
        let key = self.doc_key(doc_id);
        let data = txn
            .datastore()
            .get(&key)
            .await
            .map_err(Error::Storage)?;

        match data {
            Some(bytes) => {
                let doc =
                    Document::from_cbor(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Update an existing document.
    pub async fn update<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<()> {
        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Check document exists
        if !txn
            .datastore()
            .has(&key)
            .await
            .map_err(Error::Storage)?
        {
            return Err(Error::DocumentNotFound(doc_id.to_string()));
        }

        // Serialize and store
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        txn.datastore()
            .set(&key, &data)
            .await
            .map_err(Error::Storage)?;

        Ok(())
    }

    /// Delete a document by ID.
    pub async fn delete<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<bool> {
        let key = self.doc_key(doc_id);

        // Check if document exists
        if !txn
            .datastore()
            .has(&key)
            .await
            .map_err(Error::Storage)?
        {
            return Ok(false);
        }

        txn.datastore()
            .delete(&key)
            .await
            .map_err(Error::Storage)?;

        Ok(true)
    }

    /// Check if a document exists.
    pub async fn exists<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<bool> {
        let key = self.doc_key(doc_id);
        txn.datastore()
            .has(&key)
            .await
            .map_err(Error::Storage)
    }

    /// Save a document (create or update).
    pub async fn save<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<DocID> {
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(&doc_id);

        // Serialize and store (upsert)
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        txn.datastore()
            .set(&key, &data)
            .await
            .map_err(Error::Storage)?;

        Ok(doc_id)
    }

    /// Iterate over all documents in the collection.
    pub async fn iterate<S: Store, F, Fut>(&self, txn: &DbTxn<S>, mut callback: F) -> Result<()>
    where
        F: FnMut(Document) -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        let prefix = self.collection_key_prefix();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = txn
            .datastore()
            .iterator(opts)
            .await
            .map_err(Error::Storage)?;

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let doc = Document::from_cbor(&pair.value)
                .map_err(|e| Error::Serialization(e.to_string()))?;

            // Callback returns true to continue, false to stop
            if !callback(doc).await? {
                break;
            }
        }

        iter.close().await.map_err(Error::Storage)?;
        Ok(())
    }

    /// Get all documents in the collection.
    pub async fn get_all<S: Store>(&self, txn: &DbTxn<S>) -> Result<Vec<Document>> {
        let mut docs = Vec::new();

        self.iterate(txn, |doc| {
            docs.push(doc);
            async { Ok(true) }
        })
        .await?;

        Ok(docs)
    }

    /// Generate the storage key for a document.
    fn doc_key(&self, doc_id: &DocID) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DOC_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(doc_id.to_string().as_bytes());
        key
    }

    /// Generate the key prefix for iterating collection documents.
    fn collection_key_prefix(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DOC_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DB;
    use document::NormalValue;
    use schema::CollectionVersion;
    use storage::backends::MemoryStore;

    fn test_collection() -> Collection {
        Collection::new(CollectionVersion::new("users", "v1", "col-1", vec![]))
    }

    #[tokio::test]
    async fn test_collection_name() {
        let col = test_collection();
        assert_eq!(col.name(), "users");
        assert_eq!(col.collection_id(), "col-1");
    }

    #[tokio::test]
    async fn test_collection_create_get() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let col = test_collection();

        let txn = db.new_txn(false).await.unwrap();

        // Create a document
        let doc = Document::from_json_str(r#"{"name": "Alice", "age": 30}"#).unwrap();
        doc.generate_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();

        col.create(&txn, &doc).await.unwrap();
        txn.commit().await.unwrap();

        // Read it back
        let txn = db.new_txn(true).await.unwrap();
        let retrieved = col.get(&txn, &doc_id).await.unwrap();
        assert!(retrieved.is_some());

        let retrieved_doc = retrieved.unwrap();
        assert_eq!(
            retrieved_doc.get("name").and_then(|v| v.as_str()),
            Some("Alice")
        );
    }

    #[tokio::test]
    async fn test_collection_delete() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let col = test_collection();

        // Create
        let txn = db.new_txn(false).await.unwrap();
        let doc = Document::from_json_str(r#"{"name": "Alice"}"#).unwrap();
        doc.generate_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        col.create(&txn, &doc).await.unwrap();
        txn.commit().await.unwrap();

        // Verify exists
        let txn = db.new_txn(true).await.unwrap();
        assert!(col.exists(&txn, &doc_id).await.unwrap());

        // Delete
        let txn = db.new_txn(false).await.unwrap();
        let deleted = col.delete(&txn, &doc_id).await.unwrap();
        assert!(deleted);
        txn.commit().await.unwrap();

        // Verify gone
        let txn = db.new_txn(true).await.unwrap();
        assert!(!col.exists(&txn, &doc_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_collection_exists_nonexistent() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let col = test_collection();

        // Create a document to get a valid DocID format, then check for non-existent
        let doc = Document::from_json_str(r#"{"name": "Test"}"#).unwrap();
        doc.generate_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();

        let txn = db.new_txn(true).await.unwrap();
        // Document was never saved, so it shouldn't exist
        assert!(!col.exists(&txn, &doc_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_collection_save_upsert() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let col = test_collection();

        // Save (create)
        let txn = db.new_txn(false).await.unwrap();
        let doc = Document::from_json_str(r#"{"name": "Bob"}"#).unwrap();
        doc.generate_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        col.save(&txn, &doc).await.unwrap();
        txn.commit().await.unwrap();

        // Save again (update) - keep the same doc_id
        let txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::with_id(doc_id.clone());
        doc.set("name", NormalValue::String("Robert".to_string()));
        col.save(&txn, &doc).await.unwrap();
        txn.commit().await.unwrap();

        // Verify
        let txn = db.new_txn(true).await.unwrap();
        let retrieved = col.get(&txn, &doc_id).await.unwrap().unwrap();
        assert_eq!(
            retrieved.get("name").and_then(|v| v.as_str()),
            Some("Robert")
        );
    }
}
