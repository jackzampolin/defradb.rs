use super::*;

impl Collection {
    /// Create a new document and update all indexes.
    ///
    /// When `blind` is true, skips the document existence check. Use this when
    /// the document ID was just generated (content-addressed), guaranteeing uniqueness.
    pub async fn create_with_indexes(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        index_manager: &IndexManager,
        blind: bool,
    ) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        // Generate document ID if not present
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(&doc_id);

        // Skip existence check for blind creates (content-addressed IDs are unique by construction)
        if !blind && datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::InvalidDocument(format!(
                "Document with ID {} already exists",
                doc_id
            )));
        }

        // Serialize document to CBOR
        let data = doc.to_cbor()?;

        // Store document
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version for lens migration support
        self.store_version(datastore, &doc_id).await?;

        // Update indexes (skip unique constraint checks for blind creates)
        if blind {
            index_manager
                .on_document_create_blind(datastore, doc, &self.def)
                .await?;
        } else {
            index_manager
                .on_document_create(datastore, doc, &self.def)
                .await?;
        }

        Ok(doc_id)
    }

    /// Update an existing document and maintain all indexes.
    ///
    /// This method wraps the standard update operation with index maintenance.
    pub async fn update_with_indexes(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        index_manager: &IndexManager,
    ) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Get old document for index update
        let old_doc = match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let mut d = Document::from_cbor(&bytes)?;
                d.set_id(doc_id.clone());
                d
            }
            None => return Err(Error::DocumentNotFound(doc_id.to_string())),
        };

        // Serialize and store
        let data = doc.to_cbor()?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Update schema version to current collection version
        self.store_version(datastore, doc_id).await?;

        // Update indexes
        index_manager
            .on_document_update(datastore, &old_doc, doc, &self.def)
            .await?;

        Ok(())
    }

    /// Delete a document and update all indexes.
    ///
    /// This uses logical deletion: the document data remains in storage but a
    /// deletion marker is set. This allows `showDeleted: true` queries to still
    /// return the document with `_deleted: true`.
    pub async fn delete_with_indexes(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
        index_manager: &IndexManager,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);
        let deleted_key = self.deleted_key(doc_id);

        // Check if already deleted
        if datastore.has(&deleted_key).await.map_err(Error::Storage)? {
            return Ok(false); // Already deleted
        }

        // Get the document for index cleanup
        let doc = match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let mut d = Document::from_cbor(&bytes)?;
                d.set_id(doc_id.clone());
                d
            }
            None => return Ok(false),
        };

        // Set deletion marker (logical delete, keep document data)
        datastore
            .set(&deleted_key, &[DELETED_MARKER])
            .await
            .map_err(Error::Storage)?;

        // Update indexes (remove from indexes since doc is now "deleted")
        index_manager
            .on_document_delete(datastore, &doc, &self.def)
            .await?;

        Ok(true)
    }

    /// Check if a document is marked as deleted.
    pub async fn is_deleted(&self, datastore: &NamespaceView, doc_id: &DocID) -> Result<bool> {
        let deleted_key = self.deleted_key(doc_id);
        datastore.has(&deleted_key).await.map_err(Error::Storage)
    }
}
