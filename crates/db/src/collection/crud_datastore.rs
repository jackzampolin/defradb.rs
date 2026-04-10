use super::*;

impl Collection {
    /// Get a document by ID using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// The returned document will have its schema version set if stored.
    /// Get a document by ID, returning None if it doesn't exist or is deleted.
    pub async fn get_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<Option<Document>> {
        // Check if document is deleted
        let deleted_key = self.deleted_key(doc_id);
        if datastore.has(&deleted_key).await.map_err(Error::Storage)? {
            return Ok(None);
        }

        self.get_with_datastore_include_deleted(datastore, doc_id, false)
            .await
            .map(|opt| opt.map(|(doc, _)| doc))
    }

    /// Get a document by ID with its deletion status.
    ///
    /// Returns (Document, is_deleted) if document exists, None otherwise.
    pub async fn get_with_datastore_include_deleted(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
        check_deleted: bool,
    ) -> Result<Option<(Document, bool)>> {
        let key = self.doc_key(doc_id);
        let data = datastore.get(&key).await.map_err(Error::Storage)?;

        match data {
            Some(bytes) => {
                let mut doc = Document::from_cbor(&bytes)?;
                // Set the document ID (stored as part of the key, not in the serialized document)
                doc.set_id(doc_id.clone());

                // Load and set the schema version
                if let Some(version) = self.load_version(datastore, doc_id).await? {
                    doc.set_schema_version_id(version);
                }

                // Check deletion status if requested
                let is_deleted = if check_deleted {
                    let deleted_key = self.deleted_key(doc_id);
                    datastore.has(&deleted_key).await.map_err(Error::Storage)?
                } else {
                    false
                };

                Ok(Some((doc, is_deleted)))
            }
            None => Ok(None),
        }
    }

    /// Get all documents in the collection using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// Each returned document will have its schema version set if stored.
    /// Excludes deleted documents.
    pub async fn get_all_with_datastore(&self, datastore: &NamespaceView) -> Result<Vec<Document>> {
        let result = self
            .get_all_with_datastore_include_deleted(datastore, false)
            .await?;
        Ok(result.into_iter().map(|(doc, _)| doc).collect())
    }

    /// Get all documents in the collection with deletion status.
    ///
    /// If `show_deleted` is true, returns all documents including deleted ones.
    /// If `show_deleted` is false, returns only non-deleted documents.
    /// Returns tuples of (Document, is_deleted).
    pub async fn get_all_with_datastore_include_deleted(
        &self,
        datastore: &NamespaceView,
        show_deleted: bool,
    ) -> Result<Vec<(Document, bool)>> {
        let prefix = self.collection_key_prefix();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;

        let mut docs = Vec::new();
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            // Skip version keys (end with /v)
            if pair.key.ends_with(b"/v") {
                continue;
            }

            let mut doc = Document::from_cbor(&pair.value)
                .map_err(|e| Error::document_at_key(&pair.key, e))?;

            // Extract doc_id from key (format: /d/<collection_id>/<doc_id>)
            // Find the last '/' and extract the doc_id string after it
            if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                let doc_id_str = String::from_utf8_lossy(&pair.key[pos + 1..]);
                if let Ok(doc_id) = doc_id_str.parse::<DocID>() {
                    doc.set_id(doc_id.clone());

                    // Check if document is deleted
                    let is_deleted = self.is_deleted(datastore, &doc_id).await?;

                    // Skip deleted documents unless show_deleted is true
                    if is_deleted && !show_deleted {
                        continue;
                    }

                    // Load and set the schema version
                    if let Some(version) = self.load_version(datastore, &doc_id).await? {
                        doc.set_schema_version_id(version);
                    }

                    docs.push((doc, is_deleted));
                }
            }
        }

        iter.close().await.map_err(Error::Storage)?;
        Ok(docs)
    }

    /// Create a new document using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// The document's schema version will be set to the current collection version.
    pub async fn create_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
    ) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        // Require document ID
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        // Check if document already exists
        let key = self.doc_key(&doc_id);
        if datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::InvalidDocument(format!(
                "Document with ID {} already exists",
                doc_id
            )));
        }

        // Serialize document to CBOR
        let data = doc.to_cbor()?;

        // Store document
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version
        self.store_version(datastore, &doc_id).await?;

        Ok(doc_id)
    }

    /// Update an existing document using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    pub async fn update_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
    ) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Check document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::DocumentNotFound(doc_id.to_string()));
        }

        // Serialize and store
        let data = doc.to_cbor()?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Update schema version to current collection version
        self.store_version(datastore, doc_id).await?;

        Ok(())
    }

    /// Delete a document by ID using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// Also deletes the document's schema version.
    pub async fn delete_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);

        // Check if document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }

        // Delete document
        datastore.delete(&key).await.map_err(Error::Storage)?;

        // Delete schema version
        self.delete_version(datastore, doc_id).await?;

        Ok(true)
    }

    /// Check if a document exists using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// Check if a document exists and is not deleted.
    pub async fn exists_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }
        // Document exists, check if it's deleted
        let deleted_key = self.deleted_key(doc_id);
        let is_deleted = datastore.has(&deleted_key).await.map_err(Error::Storage)?;
        Ok(!is_deleted)
    }

    /// Check if a document exists (regardless of deletion status).
    pub async fn exists_with_datastore_include_deleted(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);
        datastore.has(&key).await.map_err(Error::Storage)
    }

    /// Save a document (create or update) using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    ///
    /// Unlike `create_with_datastore`, this performs an upsert - it will
    /// create the document if it doesn't exist, or update it if it does.
    ///
    /// Note: Validation is skipped for P2P-synced documents since they
    /// may have been created with a different schema version. The document's
    /// schema version is preserved if set, otherwise the collection's version is used.
    pub async fn save_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
    ) -> Result<DocID> {
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(&doc_id);

        // Serialize and store (upsert)
        let data = doc.to_cbor()?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version - preserve document's version if set, otherwise use collection's
        let version_key = self.version_key(&doc_id);
        let version = doc.schema_version_id().unwrap_or(&self.def.version_id);
        datastore
            .set(&version_key, version.as_bytes())
            .await
            .map_err(Error::Storage)?;

        Ok(doc_id)
    }
}
