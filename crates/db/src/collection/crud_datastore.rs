use storage::keys::doc_id_index::decode_doc_short_id;

use super::*;

impl Collection {
    /// Get a document by its public DocID, resolving the short ID through
    /// the systemstore mapping. Returns None for unknown or deleted docs.
    pub async fn get_by_doc_id(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<Option<Document>> {
        let Some((doc_short_id, canonical_doc_id)) =
            self.resolve_doc_identity(systemstore, doc_id).await?
        else {
            return Ok(None);
        };
        self.get_with_datastore(datastore, doc_short_id, &canonical_doc_id)
            .await
    }

    /// Check whether a document exists (and is not deleted) by its public
    /// DocID, resolving the short ID through the systemstore mapping.
    pub async fn exists_by_doc_id(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<bool> {
        let Some(doc_short_id) = self.resolve_doc_short_id(systemstore, doc_id).await? else {
            return Ok(false);
        };
        self.exists_with_datastore(datastore, doc_short_id).await
    }

    /// Get a document by short ID using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// The returned document carries `doc_id` and its schema version.
    /// Returns None if the document doesn't exist or is deleted.
    pub async fn get_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
        doc_id: &DocID,
    ) -> Result<Option<Document>> {
        // Check if document is deleted
        let deleted_key = self.deleted_key(doc_short_id);
        if datastore.has(&deleted_key).await.map_err(Error::Storage)? {
            return Ok(None);
        }

        self.get_with_datastore_include_deleted(datastore, doc_short_id, doc_id, false)
            .await
            .map(|opt| opt.map(|(doc, _)| doc))
    }

    /// Get a document by short ID with its deletion status.
    ///
    /// Returns (Document, is_deleted) if document exists, None otherwise.
    pub async fn get_with_datastore_include_deleted(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
        doc_id: &DocID,
        check_deleted: bool,
    ) -> Result<Option<(Document, bool)>> {
        let key = self.doc_key(doc_short_id);
        let data = datastore.get(&key).await.map_err(Error::Storage)?;

        match data {
            Some(bytes) => {
                let mut doc = Document::from_cbor(&bytes)?;
                // The blob stores values only; identity lives in the mapping.
                doc.set_id(doc_id.clone());

                // Load and set the schema version
                if let Some(version) = self.load_version(datastore, doc_short_id).await? {
                    doc.set_schema_version_id(version);
                }

                // Check deletion status if requested
                let is_deleted = if check_deleted {
                    let deleted_key = self.deleted_key(doc_short_id);
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
    pub async fn get_all_with_datastore(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
    ) -> Result<Vec<Document>> {
        let result = self
            .get_all_with_datastore_include_deleted(datastore, systemstore, false)
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
        systemstore: &NamespaceView,
        show_deleted: bool,
    ) -> Result<Vec<(Document, bool)>> {
        let entries = self
            .get_all_with_datastore_short_ids(datastore, systemstore, show_deleted)
            .await?;
        Ok(entries
            .into_iter()
            .map(|(_, doc, is_deleted)| (doc, is_deleted))
            .collect())
    }

    /// Get all documents in the collection with their doc short IDs and
    /// deletion status.
    ///
    /// Blobs are keyed by doc short ID, so iteration yields documents in
    /// allocation (insertion) order — matching Go v1.0.0's default result
    /// ordering. Public DocIDs are resolved through the systemstore mapping.
    ///
    /// Returns tuples of (doc_short_id, Document, is_deleted).
    pub async fn get_all_with_datastore_short_ids(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        show_deleted: bool,
    ) -> Result<Vec<(u64, Document, bool)>> {
        let prefix = self.collection_key_prefix();
        let prefix_len = prefix.len();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;

        let mut entries: Vec<(u64, Document)> = Vec::new();
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let Ok(doc_short_id) = decode_doc_short_id(&pair.key[prefix_len..]) else {
                continue;
            };

            let doc = Document::from_cbor(&pair.value)
                .map_err(|e| Error::document_at_key(&pair.key, e))?;
            entries.push((doc_short_id, doc));
        }
        iter.close().await.map_err(Error::Storage)?;

        let mut docs = Vec::new();
        for (doc_short_id, doc) in entries {
            if let Some(hydrated) = self
                .hydrate(datastore, systemstore, doc_short_id, doc, show_deleted)
                .await?
            {
                docs.push(hydrated);
            }
        }

        Ok(docs)
    }

    /// Reads the given documents by short id, without scanning the collection.
    ///
    /// One point read per id, so the cost is the number asked for rather than
    /// the size of the collection. That is what makes a query that already
    /// knows which documents it wants, such as one narrowed by a vector index,
    /// cheaper than the full scan it replaces.
    ///
    /// Ids that are absent are skipped rather than reported: a caller holding
    /// an id from an index may be holding one whose document has since gone.
    /// The result follows the order of `doc_short_ids`.
    pub async fn get_by_short_ids(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> Result<Vec<(u64, Document, bool)>> {
        let mut docs = Vec::with_capacity(doc_short_ids.len());
        for &doc_short_id in doc_short_ids {
            let Some(bytes) = datastore
                .get(&self.doc_key(doc_short_id))
                .await
                .map_err(Error::Storage)?
            else {
                continue;
            };
            let doc = Document::from_cbor(&bytes)
                .map_err(|e| Error::document_at_key(&self.doc_key(doc_short_id), e))?;
            if let Some(hydrated) = self
                .hydrate(datastore, systemstore, doc_short_id, doc, show_deleted)
                .await?
            {
                docs.push(hydrated);
            }
        }
        Ok(docs)
    }

    /// Completes a document read from its blob: its id, its deletion state and
    /// its schema version. `None` when it should not be returned at all.
    ///
    /// Shared by the scanning and the point-read paths so the two cannot
    /// disagree about what a loaded document looks like.
    async fn hydrate(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        doc_short_id: u64,
        mut doc: Document,
        show_deleted: bool,
    ) -> Result<Option<(u64, Document, bool)>> {
        let Some(doc_id_str) = crate::doc_id_map::get_doc_id(systemstore, doc_short_id).await?
        else {
            return Ok(None);
        };
        let Ok(doc_id) = doc_id_str.parse::<DocID>() else {
            return Ok(None);
        };
        doc.set_id(doc_id);

        let is_deleted = self.is_deleted(datastore, doc_short_id).await?;
        if is_deleted && !show_deleted {
            return Ok(None);
        }

        if let Some(version) = self.load_version(datastore, doc_short_id).await? {
            doc.set_schema_version_id(version);
        }

        Ok(Some((doc_short_id, doc, is_deleted)))
    }

    /// Create a new document blob using a NamespaceView directly.
    ///
    /// The document must already carry its (genesis-CID-derived) DocID and
    /// the caller supplies the allocated short ID. Duplicate detection is
    /// the caller's responsibility via the DocID mapping.
    pub async fn create_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        doc_short_id: u64,
    ) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        // Serialize document to CBOR
        let data = doc.to_cbor()?;

        // Store document
        let key = self.doc_key(doc_short_id);
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version
        self.store_version(datastore, doc_short_id).await?;

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
        doc_short_id: u64,
    ) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_short_id);

        let old_doc = match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let mut d = Document::from_cbor(&bytes)?;
                d.set_id(doc_id.clone());
                d
            }
            None => return Err(Error::DocumentNotFound(doc_id.to_string())),
        };

        self.validate_immutable_fields_unchanged(&old_doc, doc)?;

        // Serialize and store
        let data = doc.to_cbor()?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Update schema version to current collection version
        self.store_version(datastore, doc_short_id).await?;

        Ok(())
    }

    /// Delete a document blob using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// Also deletes the document's schema version.
    pub async fn delete_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
    ) -> Result<bool> {
        let key = self.doc_key(doc_short_id);

        // Check if document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }

        // Delete document
        datastore.delete(&key).await.map_err(Error::Storage)?;

        // Delete schema version
        self.delete_version(datastore, doc_short_id).await?;

        Ok(true)
    }

    /// Check if a document exists and is not deleted.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    pub async fn exists_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
    ) -> Result<bool> {
        let key = self.doc_key(doc_short_id);
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }
        // Document exists, check if it's deleted
        let deleted_key = self.deleted_key(doc_short_id);
        let is_deleted = datastore.has(&deleted_key).await.map_err(Error::Storage)?;
        Ok(!is_deleted)
    }

    /// Check if a document exists (regardless of deletion status).
    pub async fn exists_with_datastore_include_deleted(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
    ) -> Result<bool> {
        let key = self.doc_key(doc_short_id);
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
        doc_short_id: u64,
    ) -> Result<DocID> {
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_short_id);

        // Serialize and store (upsert)
        let data = doc.to_cbor()?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version - preserve document's version if set, otherwise use collection's
        let version_key = self.version_key(doc_short_id);
        let version = doc.schema_version_id().unwrap_or(&self.def.version_id);
        datastore
            .set(&version_key, version.as_bytes())
            .await
            .map_err(Error::Storage)?;

        Ok(doc_id)
    }
}
