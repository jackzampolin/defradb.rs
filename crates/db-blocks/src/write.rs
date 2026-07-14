use super::*;

/// Write document blocks to blockstore and heads to headstore.
///
/// This function creates CRDT blocks for a document and writes:
/// 1. LWW field blocks to blockstore
/// 2. Composite block to blockstore
/// 3. Head CIDs to headstore (for _commits queries)
///
/// This matches Go DefraDB's ProcessBlock -> updateHeads flow.
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
#[allow(clippy::too_many_arguments)]
pub async fn write_document_blocks(
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc: &Document,
    schema_version_id: &str,
    identity: DocStorageIdentity,
    modified_fields: Option<&std::collections::HashSet<String>>,
    encryption_config: Option<&EncryptionConfig>,
    signing_config: Option<&SigningConfig>,
    kms: Option<&std::sync::Arc<dyn kms::KmsService>>,
) -> Result<BlockResult, String> {
    let doc_ref_bytes = identity.doc_ref_bytes();

    let mut field_links: Vec<DAGLink> = Vec::new();
    let mut field_cids: Vec<Cid> = Vec::new();

    let is_create = modified_fields.is_none();
    let snapshot = if is_create {
        None
    } else {
        Some(DocHeadsSnapshot::load(headstore, identity.doc_short_id).await?)
    };
    let priority: u64 = if is_create {
        1
    } else {
        snapshot
            .as_ref()
            .ok_or_else(|| "snapshot required for updates".to_string())?
            .max_priority()
            + 1
    };

    for (field_name, field_value) in doc.values() {
        if field_name == "_docID" {
            continue;
        }

        let should_create_block = match modified_fields {
            None => true,
            Some(fields) => fields.contains(field_name),
        };

        let field_head_entries = if is_create {
            Vec::new()
        } else {
            snapshot
                .as_ref()
                .ok_or_else(|| "snapshot required for updates".to_string())?
                .field_heads(field_name)
        };

        if should_create_block {
            let heads: Vec<Cid> = field_head_entries.iter().map(|h| h.cid).collect();

            let cbor_value = if let Some(delta) = doc.get_counter_delta(field_name) {
                delta
            } else {
                field_value.value()
            };

            let value_bytes = encode_value_as_cbor(cbor_value)?;

            let (value_bytes, encryption_cid) = if let Some(enc) = encryption_config {
                if enc.should_encrypt_field(field_name) {
                    let key_field_name = if enc.should_encrypt_individual_field(field_name) {
                        Some(field_name.as_str())
                    } else {
                        None
                    };

                    if let Some(kms_svc) = kms {
                        // KMS path: the KMS generates + persists the Encryption
                        // block (in its KeyStore) and returns the CID + plain key
                        // for us to encrypt the field delta with. The scope is
                        // keyed by the node-local DocRef: the public DocID is
                        // not yet known on the create path.
                        let scope = kms::KeyScope::Document {
                            doc_id: hex::encode(&doc_ref_bytes),
                            field: key_field_name.map(str::to_owned),
                        };
                        let ctx = kms::RequestContext::anonymous();
                        let (enc_cid, key) = kms_svc
                            .generate_key(&ctx, scope)
                            .await
                            .map_err(|e| format!("kms generate_key: {e}"))?;
                        let encrypted = encrypt_delta(&value_bytes, &key)?;
                        tracing::debug!(
                            field = %field_name,
                            ciphertext_len = encrypted.len(),
                            "Encrypted field delta via KMS"
                        );
                        (encrypted, Some(enc_cid))
                    } else {
                        // Legacy path (unchanged): inline key generation + direct block store.
                        let key = defra_core::encryption::generate_encryption_key_for(
                            &doc_ref_bytes,
                            key_field_name,
                        );
                        let encrypted = encrypt_delta(&value_bytes, &key)?;

                        tracing::debug!(
                            field = %field_name,
                            ciphertext_len = encrypted.len(),
                            "Encrypted field delta"
                        );

                        let enc_block = Encryption { key: key.to_vec() };
                        let enc_bytes = enc_block
                            .to_dag_cbor()
                            .map_err(|e| format!("Failed to encode encryption block: {}", e))?;
                        let enc_cid = generate_cid_from_bytes(&enc_bytes)
                            .map_err(|e| format!("Failed to generate encryption CID: {}", e))?;
                        blockstore
                            .set(&enc_cid.to_bytes(), &enc_bytes)
                            .await
                            .map_err(|e| format!("Failed to store encryption block: {}", e))?;

                        (encrypted, Some(enc_cid))
                    }
                } else {
                    (value_bytes, None)
                }
            } else {
                (value_bytes, None)
            };

            let is_counter = doc
                .fields()
                .get(field_name)
                .map(|f| f.crdt_type().is_counter())
                .unwrap_or(false)
                || doc.get_counter_delta(field_name).is_some();

            let nonce: i64 = if is_counter && !heads.is_empty() {
                rand::random()
            } else {
                0
            };

            let delta = if is_counter {
                CrdtDelta::Counter(CounterDeltaPayload {
                    field_name: field_name.clone(),
                    priority,
                    nonce,
                    schema_version_id: schema_version_id.to_string(),
                    data: value_bytes,
                })
            } else {
                CrdtDelta::Lww(LwwDeltaPayload {
                    field_name: field_name.clone(),
                    priority,
                    schema_version_id: schema_version_id.to_string(),
                    data: value_bytes,
                })
            };

            let mut field_block =
                Block::new_with_options(delta, heads.clone(), vec![], encryption_cid, None);

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

            let field_block_bytes = field_block
                .to_dag_cbor()
                .map_err(|e| format!("Failed to encode field block: {}", e))?;
            let field_cid = generate_cid_from_bytes(&field_block_bytes)
                .map_err(|e| format!("Failed to generate field CID: {}", e))?;

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

            blockstore
                .set(&field_cid.to_bytes(), &field_block_bytes)
                .await
                .map_err(|e| format!("Failed to store field block: {}", e))?;

            for old_head in &field_head_entries {
                headstore
                    .delete(&old_head.key)
                    .await
                    .map_err(|e| format!("Failed to delete old field head: {}", e))?;
            }

            let head_key = HeadstoreDocKey::new(identity.doc_short_id, field_name, field_cid);
            let priority_bytes = encode_priority_varint(priority);
            headstore
                .set(&head_key.bytes(), &priority_bytes)
                .await
                .map_err(|e| format!("Failed to write field head: {}", e))?;
            headstore
                .set(
                    &priority_index_key(identity.doc_short_id, priority, field_cid),
                    &[],
                )
                .await
                .map_err(|e| format!("Failed to write field priority index: {}", e))?;

            tracing::debug!(
                field_name = %field_name,
                cid = %field_cid,
                is_counter = is_counter,
                has_prev_head = !heads.is_empty(),
                "Stored field block and head"
            );

            field_links.push(DAGLink::new(field_name.clone(), field_cid));
            field_cids.push(field_cid);
        }
    }

    let (composite_head_entries, composite_heads) = if is_create {
        (Vec::new(), Vec::new())
    } else {
        let entries = snapshot
            .as_ref()
            .ok_or_else(|| "snapshot required for updates".to_string())?
            .field_heads("C");
        let heads: Vec<Cid> = entries.iter().map(|h| h.cid).collect();
        (entries, heads)
    };

    let composite_payload = CompositeDeltaPayload {
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 1,
    };

    let composite_encryption_cid = if let Some(enc) = encryption_config {
        if enc.encrypt_doc {
            let key = defra_core::encryption::generate_encryption_key_for(&doc_ref_bytes, None);
            let enc_block = Encryption { key: key.to_vec() };
            let enc_bytes = enc_block
                .to_dag_cbor()
                .map_err(|e| format!("Failed to encode composite encryption block: {}", e))?;
            let enc_cid = generate_cid_from_bytes(&enc_bytes)
                .map_err(|e| format!("Failed to generate composite encryption CID: {}", e))?;
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

    let mut composite_block = Block::new_with_options(
        CrdtDelta::Composite(composite_payload),
        composite_heads,
        field_links,
        composite_encryption_cid,
        None,
    );

    if let Some(signer) = signing_config {
        if let Some(sig_cid) = sign_block(&composite_block, signer, blockstore).await? {
            composite_block.signature = Some(sig_cid);
        }
    }

    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode composite block: {}", e))?;
    let composite_cid = generate_cid_from_bytes(&composite_bytes)
        .map_err(|e| format!("Failed to generate composite CID: {}", e))?;

    blockstore
        .set(&composite_cid.to_bytes(), &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store composite block: {}", e))?;

    for old_head in &composite_head_entries {
        headstore
            .delete(&old_head.key)
            .await
            .map_err(|e| format!("Failed to delete old composite head: {}", e))?;
    }

    let composite_head_key = HeadstoreDocKey::new(identity.doc_short_id, "C", composite_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore
        .set(&composite_head_key.bytes(), &priority_bytes)
        .await
        .map_err(|e| format!("Failed to write composite head: {}", e))?;
    headstore
        .set(
            &priority_index_key(identity.doc_short_id, priority, composite_cid),
            &[],
        )
        .await
        .map_err(|e| format!("Failed to write composite priority index: {}", e))?;

    let doc_id_str = if is_create {
        derive_doc_id(&composite_cid)
    } else {
        doc.id()
            .ok_or_else(|| "Document must have an ID for updates".to_string())?
            .to_string()
    };

    tracing::debug!(
        doc_id = %doc_id_str,
        cid = %composite_cid,
        field_count = field_cids.len(),
        prev_heads = composite_head_entries.len(),
        "Built composite block with field links and wrote heads"
    );

    if let Some(session_key) = defra_core::batch_signing::get_batch_session_key() {
        if priority == 1 {
            for fc in &field_cids {
                defra_core::batch_signing::batch_collect_cid(&session_key, *fc);
            }
        }
        defra_core::batch_signing::batch_collect_cid(&session_key, composite_cid);
    }

    Ok(BlockResult {
        cid: composite_cid,
        block: composite_bytes,
        doc_id: doc_id_str,
        field_cids,
    })
}

/// Write a delete block (composite with status=2) to blockstore and heads to headstore.
pub async fn write_delete_block(
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc_id: &str,
    doc_short_id: u64,
    schema_version_id: &str,
    signing_config: Option<&SigningConfig>,
) -> Result<BlockResult, String> {
    let snapshot = DocHeadsSnapshot::load(headstore, doc_short_id).await?;
    let priority: u64 = snapshot.max_priority() + 1;

    let composite_head_entries = snapshot.field_heads("C");
    let composite_heads: Vec<Cid> = composite_head_entries.iter().map(|h| h.cid).collect();

    let composite_payload = CompositeDeltaPayload {
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 2,
    };

    let mut composite_block = Block::new(
        CrdtDelta::Composite(composite_payload),
        composite_heads,
        vec![],
    );

    if let Some(signer) = signing_config {
        if let Some(sig_cid) = sign_block(&composite_block, signer, blockstore).await? {
            composite_block.signature = Some(sig_cid);
        }
    }

    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode delete composite block: {}", e))?;
    let composite_cid = generate_cid_from_bytes(&composite_bytes)
        .map_err(|e| format!("Failed to generate delete composite CID: {}", e))?;

    blockstore
        .set(&composite_cid.to_bytes(), &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store delete composite block: {}", e))?;

    for old_head in &composite_head_entries {
        headstore
            .delete(&old_head.key)
            .await
            .map_err(|e| format!("Failed to delete old composite head: {}", e))?;
    }

    let composite_head_key = HeadstoreDocKey::new(doc_short_id, "C", composite_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore
        .set(&composite_head_key.bytes(), &priority_bytes)
        .await
        .map_err(|e| format!("Failed to write delete composite head: {}", e))?;
    headstore
        .set(
            &priority_index_key(doc_short_id, priority, composite_cid),
            &[],
        )
        .await
        .map_err(|e| format!("Failed to write delete priority index: {}", e))?;

    tracing::debug!(
        doc_id = %doc_id,
        cid = %composite_cid,
        priority = priority,
        "Built delete composite block (status=2)"
    );

    if let Some(session_key) = defra_core::batch_signing::get_batch_session_key() {
        defra_core::batch_signing::batch_collect_cid(&session_key, composite_cid);
    }

    Ok(BlockResult {
        cid: composite_cid,
        block: composite_bytes,
        doc_id: doc_id.to_string(),
        field_cids: vec![],
    })
}

#[cfg(test)]
mod kms_write_tests {
    use super::*;
    use async_trait::async_trait;
    use datastore::{NamespaceView, SharedTxn};
    use defra_core::encryption::EncryptionConfig;
    use std::sync::Arc;
    use storage::backends::MemoryStore;
    use storage::corekv::Store;
    use storage::namespace::Namespace;

    /// Stub KMS that mirrors the real `MemoryKeyStore::generate` block shape so
    /// the returned CID matches what the legacy path would produce for the same
    /// scope. The key is fixed to make the assertion deterministic.
    struct StubKms;

    #[async_trait]
    impl kms::KmsService for StubKms {
        async fn get_keys(
            &self,
            _: &kms::RequestContext,
            _: &[kms::EncryptionCid],
        ) -> kms::Result<kms::KeyResults> {
            let (r, tx) = kms::KeyResults::new(1);
            drop(tx);
            Ok(r)
        }

        async fn generate_key(
            &self,
            _: &kms::RequestContext,
            scope: kms::KeyScope,
        ) -> kms::Result<(kms::EncryptionCid, [u8; 32])> {
            let (doc_id_bytes, field_name) = match scope {
                kms::KeyScope::Document { doc_id, field } => (doc_id.into_bytes(), field),
                kms::KeyScope::Collection { collection_id } => (Vec::new(), Some(collection_id)),
            };
            let _ = (doc_id_bytes, field_name);
            let block = defra_core::Encryption { key: vec![5u8; 32] };
            let bytes = block.to_dag_cbor().unwrap();
            let cid = defra_core::block::generate_cid_from_bytes(&bytes).unwrap();
            Ok((cid, [5u8; 32]))
        }

        async fn serve_request(
            &self,
            _: kms::PeerIdentity,
            _: kms::FetchEncryptionKeyRequest,
        ) -> kms::Result<kms::FetchEncryptionKeyReply> {
            Err(kms::Error::Unsupported("stub"))
        }
    }

    /// Recompute the CID the stub KMS would return, so the test asserts
    /// against the KMS-derived CID rather than a hardcoded value.
    fn expected_field_cid() -> Cid {
        let block = defra_core::Encryption { key: vec![5u8; 32] };
        let bytes = block.to_dag_cbor().unwrap();
        generate_cid_from_bytes(&bytes).unwrap()
    }

    #[tokio::test]
    async fn kms_write_path_links_field_block_to_kms_encryption_cid() {
        let store = MemoryStore::new();
        let txn = store.new_txn(false).await.unwrap();
        let shared = SharedTxn::new(txn);
        let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
        let headstore = NamespaceView::new(shared.clone(), Namespace::Headstore);

        let mut doc = Document::new();
        doc.set("secret", NormalValue::String("classified".to_string()));

        let enc = EncryptionConfig {
            encrypt_doc: false,
            encrypt_fields: vec!["secret".to_string()],
        };

        let kms: Arc<dyn kms::KmsService> = Arc::new(StubKms);

        let result = write_document_blocks(
            &blockstore,
            &headstore,
            &doc,
            "schema-v1",
            DocStorageIdentity::new(1, 1),
            None,
            Some(&enc),
            None,
            Some(&kms),
        )
        .await
        .expect("KMS write path should succeed");

        assert_eq!(result.field_cids.len(), 1);
        let field_cid = result.field_cids[0];
        let field_bytes = blockstore
            .get(&field_cid.to_bytes())
            .await
            .unwrap()
            .expect("field block stored");
        let field_block = Block::from_dag_cbor(&field_bytes).unwrap();

        let expected = expected_field_cid();
        assert_eq!(
            field_block.encryption,
            Some(expected),
            "field block must link to the KMS-generated encryption CID"
        );
    }
}
