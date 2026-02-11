use super::*;

/// Write document blocks to blockstore and heads to headstore.
///
/// This function creates CRDT blocks for a document and writes:
/// 1. LWW field blocks to blockstore
/// 2. Composite block to blockstore
/// 3. Head CIDs to headstore (for _commits queries)
///
/// This matches Go DefraDB's ProcessBlock → updateHeads flow.
///
/// # Arguments
///
/// * `blockstore` - Transaction blockstore view
/// * `headstore` - Transaction headstore view
/// * `doc` - The document to build blocks from
/// * `schema_version_id` - The schema version ID for CRDT deltas
/// * `modified_fields` - Optional set of field names that were modified.
///   If Some, only these fields will have new blocks created.
///   If None (for create operations), all fields get new blocks.
///
/// # Returns
///
/// Returns `BlockResult` containing the composite block and metadata.
pub async fn write_document_blocks(
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc: &Document,
    schema_version_id: &str,
    modified_fields: Option<&std::collections::HashSet<String>>,
    encryption_config: Option<&EncryptionConfig>,
    signing_config: Option<&SigningConfig>,
) -> Result<BlockResult, String> {
    let doc_id = doc
        .id()
        .ok_or_else(|| "Document must have an ID".to_string())?;
    let doc_id_str = doc_id.to_string();
    let doc_id_bytes = doc_id_str.as_bytes().to_vec();

    let mut field_links: Vec<DAGLink> = Vec::new();
    let mut field_cids: Vec<Cid> = Vec::new();

    // For creates (modified_fields=None), priority is always 1 and no heads exist.
    // For updates, scan headstore for max priority (matches Go behavior).
    let is_create = modified_fields.is_none();
    let priority: u64 = if is_create {
        1
    } else {
        get_max_priority(headstore, &doc_id_str).await? + 1
    };

    // Create LWW blocks for each field
    // For updates with modified_fields, only create blocks for changed fields.
    // For unchanged fields, use their existing head CID in the composite links.
    for (field_name, field_value) in doc.values() {
        // Skip _docID (stored as the block key, not a CRDT field).
        // Other _-prefixed fields like _ownerID (foreign keys) DO get blocks.
        if field_name == "_docID" {
            continue;
        }

        // Check if this field should have a new block created
        let should_create_block = match modified_fields {
            None => true, // Create operation: all fields get new blocks
            Some(fields) => fields.contains(field_name), // Update: only modified fields
        };

        // For creates, no heads exist yet — skip the headstore scan.
        let field_head_entries = if is_create {
            Vec::new()
        } else {
            get_all_field_heads(headstore, &doc_id_str, field_name).await?
        };

        if should_create_block {
            // Create new block for this field (LWW or Counter depending on CRDT type)
            let heads: Vec<Cid> = field_head_entries.iter().map(|h| h.cid).collect();

            // For counter fields during updates, use the raw increment delta
            // (not the accumulated value) to match Go behavior.
            let cbor_value = if let Some(delta) = doc.get_counter_delta(field_name) {
                delta
            } else {
                field_value.value()
            };

            // Encode the field value as CBOR
            let value_bytes = encode_value_as_cbor(cbor_value)?;

            // Encrypt delta and create Encryption metadata block if configured
            let (value_bytes, encryption_cid) = if let Some(enc) = encryption_config {
                if enc.should_encrypt_field(field_name) {
                    let key = enc.derive_key(field_name, &doc_id_str);
                    let encrypted = encrypt_delta(&value_bytes, &key)?;

                    tracing::debug!(
                        field = %field_name,
                        ciphertext_len = encrypted.len(),
                        "Encrypted field delta"
                    );

                    // Create Encryption metadata block (matches Go's block/encryption.go)
                    let is_field_level = enc.encrypt_fields.iter().any(|f| f == field_name);
                    let enc_block = Encryption {
                        doc_id: doc_id_bytes.clone(),
                        field_name: if is_field_level {
                            Some(field_name.clone())
                        } else {
                            None
                        },
                        key: key.to_vec(),
                    };
                    let enc_cid = enc_block
                        .generate_cid()
                        .map_err(|e| format!("Failed to generate encryption CID: {}", e))?;
                    let enc_bytes = enc_block
                        .to_dag_cbor()
                        .map_err(|e| format!("Failed to encode encryption block: {}", e))?;
                    blockstore
                        .set(&enc_cid.to_bytes(), &enc_bytes)
                        .await
                        .map_err(|e| format!("Failed to store encryption block: {}", e))?;

                    (encrypted, Some(enc_cid))
                } else {
                    (value_bytes, None)
                }
            } else {
                (value_bytes, None)
            };

            // Check if this field is a Counter type.
            // For documents loaded from storage, the CRDT type may default to LWW
            // since storage doesn't preserve schema annotations. Fall back to checking
            // whether a counter delta was set (only counter fields get counter deltas).
            let is_counter = doc
                .fields()
                .get(field_name)
                .map(|f| f.crdt_type().is_counter())
                .unwrap_or(false)
                || doc.get_counter_delta(field_name).is_some();

            // For Counter fields, use nonce=0 for initial creation (heads empty),
            // random nonce for updates (heads non-empty) - matches Go behavior
            let nonce: i64 = if is_counter && !heads.is_empty() {
                // Update operation: use random nonce
                rand::random()
            } else {
                // Initial creation: use deterministic nonce=0
                0
            };

            // Create the appropriate delta based on CRDT type
            let delta = if is_counter {
                CrdtDelta::Counter(CounterDeltaPayload {
                    doc_id: doc_id_bytes.clone(),
                    field_name: field_name.clone(),
                    priority,
                    nonce,
                    schema_version_id: schema_version_id.to_string(),
                    data: value_bytes,
                })
            } else {
                CrdtDelta::Lww(LwwDeltaPayload {
                    doc_id: doc_id_bytes.clone(),
                    field_name: field_name.clone(),
                    priority,
                    schema_version_id: schema_version_id.to_string(),
                    data: value_bytes,
                })
            };

            // Create the field block with heads linking to previous version
            // Include encryption CID if this field was encrypted
            let mut field_block =
                Block::new_with_options(delta, heads.clone(), vec![], encryption_cid, None);

            // Sign the block if a signer is available.
            // Go's signBlock: sign the block bytes (without signature), store
            // the Signature as a separate block, then set block.signature = sig_cid.
            if let Some(signer) = signing_config {
                tracing::debug!(field = %field_name, priority, "Signing field block");
                match sign_block(&field_block, signer, blockstore).await {
                    Ok(Some(sig_cid)) => {
                        tracing::debug!(field = %field_name, sig_cid = %sig_cid, "Field block signed");
                        field_block.signature = Some(sig_cid);
                    }
                    Ok(None) => {
                        tracing::debug!(field = %field_name, priority, "Skipped signing (priority > 1)");
                    }
                    Err(e) => {
                        tracing::debug!(field = %field_name, error = %e, "Failed to sign field block");
                        return Err(e);
                    }
                }
            } else {
                tracing::debug!(field = %field_name, "No signing config for field");
            }

            // Serialize and generate CID (includes signature CID if signed)
            let field_block_bytes = field_block
                .to_dag_cbor()
                .map_err(|e| format!("Failed to encode field block: {}", e))?;
            let field_cid = field_block
                .generate_cid()
                .map_err(|e| format!("Failed to generate field CID: {}", e))?;

            // Debug: verify roundtrip of signature through serialization
            tracing::debug!(
                field = %field_name,
                cid = %field_cid,
                bytes_len = field_block_bytes.len(),
                signature = ?field_block.signature,
                "Storing field block"
            );
            if tracing::enabled!(tracing::Level::DEBUG) {
                match Block::from_dag_cbor(&field_block_bytes) {
                    Ok(b) => tracing::debug!(signature = ?b.signature, "Signature roundtrip OK"),
                    Err(e) => tracing::debug!(error = %e, "Signature roundtrip failed"),
                }
            }

            // Store the field block in blockstore
            blockstore
                .set(&field_cid.to_bytes(), &field_block_bytes)
                .await
                .map_err(|e| format!("Failed to store field block: {}", e))?;

            // Delete all old head entries (replace all branches with single new head)
            for old_head in &field_head_entries {
                headstore
                    .delete(&old_head.key)
                    .await
                    .map_err(|e| format!("Failed to delete old field head: {}", e))?;
            }

            // Write new head CID to headstore: /d/{doc_id}/{field_name}/{cid} → priority
            let head_key = HeadstoreDocKey::new(&doc_id_str, field_name, field_cid);
            let priority_bytes = encode_priority_varint(priority);
            headstore
                .set(&head_key.bytes(), &priority_bytes)
                .await
                .map_err(|e| format!("Failed to write field head: {}", e))?;

            tracing::debug!(
                field_name = %field_name,
                cid = %field_cid,
                is_counter = is_counter,
                has_prev_head = !heads.is_empty(),
                "Stored field block and head"
            );

            // Add link to composite - only new blocks get linked
            field_links.push(DAGLink::new(field_name.clone(), field_cid));
            field_cids.push(field_cid);
        }
        // Note: Unchanged fields are NOT added to composite links.
        // Go only includes newly created field blocks in the composite's links array.
    }

    // For creates, no composite heads exist yet — skip the headstore scan.
    let (composite_head_entries, composite_heads) = if is_create {
        (Vec::new(), Vec::new())
    } else {
        let entries = get_all_field_heads(headstore, &doc_id_str, "C").await?;
        let heads: Vec<Cid> = entries.iter().map(|h| h.cid).collect();
        (entries, heads)
    };

    // Create the Composite delta payload
    let composite_payload = CompositeDeltaPayload {
        doc_id: doc_id_bytes,
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 1, // Active document
    };

    // For doc-level encryption, the composite block also carries an encryption
    // CID link (even though its data is not encrypted). Go's AddDelta always calls
    // determineBlockEncryption, which returns an encryption block for doc-level
    // encryption regardless of delta type. encryptBlock() then skips actual
    // encryption for composites but the Encryption link is still set on the block.
    let composite_encryption_cid = if let Some(enc) = encryption_config {
        if enc.encrypt_doc {
            let key = enc.derive_key("", &doc_id_str); // doc-level: empty field name
            let enc_block = Encryption {
                doc_id: doc_id_str.as_bytes().to_vec(),
                field_name: None,
                key: key.to_vec(),
            };
            let enc_cid = enc_block
                .generate_cid()
                .map_err(|e| format!("Failed to generate composite encryption CID: {}", e))?;
            let enc_bytes = enc_block
                .to_dag_cbor()
                .map_err(|e| format!("Failed to encode composite encryption block: {}", e))?;
            blockstore
                .set(&enc_cid.to_bytes(), &enc_bytes)
                .await
                .map_err(|e| format!("Failed to store composite encryption block: {}", e))?;
            Some(enc_cid)
        } else {
            None
        }
    } else {
        None
    };

    // Create the composite block with heads linking to previous version
    let mut composite_block = Block::new_with_options(
        CrdtDelta::Composite(composite_payload),
        composite_heads,
        field_links,
        composite_encryption_cid,
        None,
    );

    // Sign the composite block if a signer is available
    if let Some(signer) = signing_config {
        if let Some(sig_cid) = sign_block(&composite_block, signer, blockstore).await? {
            composite_block.signature = Some(sig_cid);
        }
    }

    // Serialize the composite block (includes signature CID if signed)
    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode composite block: {}", e))?;
    let composite_cid = composite_block
        .generate_cid()
        .map_err(|e| format!("Failed to generate composite CID: {}", e))?;

    // Store the composite block in blockstore
    blockstore
        .set(&composite_cid.to_bytes(), &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store composite block: {}", e))?;

    // Delete all old composite head entries (replace all branches with single new head)
    for old_head in &composite_head_entries {
        headstore
            .delete(&old_head.key)
            .await
            .map_err(|e| format!("Failed to delete old composite head: {}", e))?;
    }

    // Write new composite head to headstore: /d/{doc_id}/C/{cid} → priority
    let composite_head_key = HeadstoreDocKey::new(&doc_id_str, "C", composite_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore
        .set(&composite_head_key.bytes(), &priority_bytes)
        .await
        .map_err(|e| format!("Failed to write composite head: {}", e))?;

    tracing::info!(
        doc_id = %doc_id_str,
        cid = %composite_cid,
        field_count = field_cids.len(),
        prev_heads = composite_head_entries.len(),
        "Built composite block with field links and wrote heads"
    );

    Ok(BlockResult {
        cid: composite_cid,
        block: composite_bytes,
        doc_id: doc_id_str,
        field_cids,
    })
}

/// Write a delete block (composite with status=2) to blockstore and heads to headstore.
///
/// Delete operations in Go DefraDB create a composite block with status=2 (deleted),
/// no field-level blocks, and heads pointing to the previous composite head.
/// This matches Go's `crdt.DocComposite.DeleteDelta()` → `coreblock.AddDelta()` flow.
pub async fn write_delete_block(
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc_id: &str,
    schema_version_id: &str,
    signing_config: Option<&SigningConfig>,
) -> Result<BlockResult, String> {
    let doc_id_bytes = doc_id.as_bytes().to_vec();

    // Get max existing priority and increment by 1
    let max_priority = get_max_priority(headstore, doc_id).await?;
    let priority: u64 = max_priority + 1;

    // Get all existing composite heads to build proper DAG links
    let composite_head_entries = get_all_field_heads(headstore, doc_id, "C").await?;
    let composite_heads: Vec<Cid> = composite_head_entries.iter().map(|h| h.cid).collect();

    // Create the Composite delta payload with status=2 (deleted)
    let composite_payload = CompositeDeltaPayload {
        doc_id: doc_id_bytes,
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 2, // Deleted document
    };

    // Create the composite block with no field links (deletes have no field blocks)
    let mut composite_block = Block::new(
        CrdtDelta::Composite(composite_payload),
        composite_heads,
        vec![], // No field links for delete
    );

    // Sign the delete composite block if a signer is available
    if let Some(signer) = signing_config {
        if let Some(sig_cid) = sign_block(&composite_block, signer, blockstore).await? {
            composite_block.signature = Some(sig_cid);
        }
    }

    // Serialize the composite block
    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode delete composite block: {}", e))?;
    let composite_cid = composite_block
        .generate_cid()
        .map_err(|e| format!("Failed to generate delete composite CID: {}", e))?;

    // Store the composite block in blockstore
    blockstore
        .set(&composite_cid.to_bytes(), &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store delete composite block: {}", e))?;

    // Delete all old composite head entries (replace all branches with single new head)
    for old_head in &composite_head_entries {
        headstore
            .delete(&old_head.key)
            .await
            .map_err(|e| format!("Failed to delete old composite head: {}", e))?;
    }

    // Write new composite head to headstore: /d/{doc_id}/C/{cid} → priority
    let composite_head_key = HeadstoreDocKey::new(doc_id, "C", composite_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore
        .set(&composite_head_key.bytes(), &priority_bytes)
        .await
        .map_err(|e| format!("Failed to write delete composite head: {}", e))?;

    tracing::info!(
        doc_id = %doc_id,
        cid = %composite_cid,
        priority = priority,
        "Built delete composite block (status=2)"
    );

    Ok(BlockResult {
        cid: composite_cid,
        block: composite_bytes,
        doc_id: doc_id.to_string(),
        field_cids: vec![],
    })
}
