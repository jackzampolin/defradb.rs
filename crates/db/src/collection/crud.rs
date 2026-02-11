use super::*;

impl Collection {
    /// Create a new document in this collection.
    ///
    /// The document must have an ID set before calling this method.
    /// The document will be validated against the collection schema.
    /// The document's schema version will be set to the current collection version.
    pub async fn create<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        // Generate document ID if not present
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let datastore = txn.datastore()?;

        // Check if document already exists
        let key = self.doc_key(&doc_id);
        if datastore.has(&key).await.map_err(Error::Storage)? {
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
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version
        self.store_version(&datastore, &doc_id).await?;

        Ok(doc_id)
    }

    /// Get a document by ID.
    ///
    /// The returned document will have its schema version set if stored.
    pub async fn get<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<Option<Document>> {
        let datastore = txn.datastore()?;
        let key = self.doc_key(doc_id);
        let data = datastore.get(&key).await.map_err(Error::Storage)?;

        match data {
            Some(bytes) => {
                let mut doc =
                    Document::from_cbor(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
                // Set the document ID (stored as part of the key, not in the serialized document)
                doc.set_id(doc_id.clone());

                // Load and set the schema version
                if let Some(version) = self.load_version(&datastore, doc_id).await? {
                    doc.set_schema_version_id(version);
                }

                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Update an existing document.
    ///
    /// The document will be validated against the collection schema.
    pub async fn update<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Check document exists
        if !txn.datastore()?.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::DocumentNotFound(doc_id.to_string()));
        }

        // Serialize and store
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let datastore = txn.datastore()?;
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Update schema version to current collection version
        self.store_version(&datastore, doc_id).await?;

        Ok(())
    }

    /// Delete a document by ID.
    ///
    /// Also deletes the document's schema version.
    pub async fn delete<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<bool> {
        let datastore = txn.datastore()?;
        let key = self.doc_key(doc_id);

        // Check if document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }

        // Delete document
        datastore.delete(&key).await.map_err(Error::Storage)?;

        // Delete schema version
        self.delete_version(&datastore, doc_id).await?;

        Ok(true)
    }

    /// Check if a document exists.
    pub async fn exists<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<bool> {
        let key = self.doc_key(doc_id);
        txn.datastore()?.has(&key).await.map_err(Error::Storage)
    }

    /// Save a document (create or update).
    ///
    /// The document will be validated against the collection schema.
    pub async fn save<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(&doc_id);

        // Serialize and store (upsert)
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        txn.datastore()?
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
            .datastore()?
            .iterator(opts)
            .await
            .map_err(Error::Storage)?;

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let mut doc = Document::from_cbor(&pair.value).map_err(|e| {
                Error::Serialization(format!(
                    "failed to deserialize document at key {:?}: {}",
                    String::from_utf8_lossy(&pair.key),
                    e
                ))
            })?;

            // Extract doc_id from key (format: /d/<collection_id>/<doc_id>)
            // Find the last '/' and extract the doc_id string after it
            if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                let doc_id_str = String::from_utf8_lossy(&pair.key[pos + 1..]);
                if let Ok(doc_id) = doc_id_str.parse::<DocID>() {
                    doc.set_id(doc_id);
                }
            }

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
}
