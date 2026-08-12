use super::*;

/// The `Encryption` link a new block should carry forward, read off the
/// previous block, when the write brings no config of its own.
///
/// Mirrors the fallback half of Go's `determineBlockEncryption`
/// (internal/core/block/store.go): the DAG, not any process-local state, is
/// what records that a document is encrypted, and the existing link is reused
/// so no new encryption block is written.
///
/// A head whose block cannot be read is an error rather than a miss: guessing
/// "not encrypted" is exactly the silent downgrade this derivation exists to
/// prevent. Matches Go, which returns `ErrCouldNotFindBlock` here.
///
/// Heads with no encryption are skipped rather than ending the search, and an
/// empty `heads` yields `None`. A field with nothing of its own to inherit is
/// not thereby in the clear: `write_document_blocks` falls back to the
/// document-level policy the composite head records before writing plaintext.
async fn inherited_encryption_cid(
    blockstore: &NamespaceView,
    heads: &[Cid],
) -> Result<Option<Cid>, String> {
    for head in heads {
        let block_bytes = blockstore
            .get(&head.to_bytes())
            .await
            .map_err(|e| format!("Failed to read previous block {}: {}", head, e))?
            .ok_or_else(|| format!("Previous block {} not found", head))?;
        let prev = Block::from_dag_cbor(&block_bytes)
            .map_err(|e| format!("Failed to decode previous block {}: {}", head, e))?;

        if prev.encryption.is_some() {
            return Ok(prev.encryption);
        }
    }

    Ok(None)
}

/// Last resort for an `Encryption` block neither block namespace holds: ask the
/// configured KMS, which may keep it in a `KeyStore` of its own.
///
/// `DefraKms` serves what it holds locally before any peer fan-out
/// (`kms/src/defra_kms.rs`), so this stays a local read in the common case.
async fn kms_resolved_key(
    kms: Option<&std::sync::Arc<dyn kms::KmsService>>,
    enc_cid: Cid,
) -> Result<Option<([u8; 32], Cid)>, String> {
    let kms_svc = kms.ok_or_else(|| format!("Encryption block {} not found", enc_cid))?;
    let key = kms_svc
        .get_keys(&kms::RequestContext::anonymous(), &[enc_cid])
        .await
        .map_err(|e| format!("kms get_keys for {}: {}", enc_cid, e))?
        .wait_all()
        .await
        .map_err(|e| format!("kms get_keys for {}: {}", enc_cid, e))?
        .remove(&enc_cid)
        .ok_or_else(|| format!("Encryption block {} not found", enc_cid))?;

    Ok(Some((key, enc_cid)))
}

/// The inherited encryption link together with the key it points at, for the
/// field deltas that actually need to encrypt with it.
///
/// Reusing one key across a field's whole history is safe only because every
/// delta is sealed with a fresh random nonce (see `encrypt_delta`). Do not make
/// that nonce deterministic or counter-based without revisiting this.
async fn inherited_encryption(
    blockstore: &NamespaceView,
    heads: &[Cid],
    kms: Option<&std::sync::Arc<dyn kms::KmsService>>,
) -> Result<Option<([u8; 32], Cid)>, String> {
    let Some(enc_cid) = inherited_encryption_cid(blockstore, heads).await? else {
        return Ok(None);
    };

    // Encstore first, then blockstore: blocks arriving over P2P land in the
    // encstore while locally-written ones go to the blockstore. Mirrors the
    // merge decrypt path (`db-merge/src/merge_handler/encryption.rs`) and
    // `versioned_fetcher`.
    let enc_key = enc_cid.to_bytes();
    let enc_bytes = match blockstore
        .sibling(storage::namespace::Namespace::Encstore)
        .get(&enc_key)
        .await
        .map_err(|e| format!("Failed to read encryption block {}: {}", enc_cid, e))?
    {
        Some(bytes) => bytes,
        None => match blockstore
            .get(&enc_key)
            .await
            .map_err(|e| format!("Failed to read encryption block {}: {}", enc_cid, e))?
        {
            Some(bytes) => bytes,
            // A `KmsService` only promises that `generate_key` persisted the
            // block in its own `KeyStore`; nothing requires that store to be
            // either of these namespaces. Ask it for the key rather than
            // assuming the aliasing today's wiring happens to give us.
            None => return kms_resolved_key(kms, enc_cid).await,
        },
    };
    let enc_block = Encryption::from_dag_cbor(&enc_bytes)
        .map_err(|e| format!("Failed to decode encryption block {}: {}", enc_cid, e))?;
    let key: [u8; 32] = enc_block.key.as_slice().try_into().map_err(|_| {
        format!(
            "Encryption block {} has a {}-byte key, expected 32",
            enc_cid,
            enc_block.key.len()
        )
    })?;

    Ok(Some((key, enc_cid)))
}

/// Mint a fresh key for a field, seal its delta with it, and persist the
/// `Encryption` block the new block will link to.
///
/// Shared by the explicit-config path and the document-policy path so both
/// route through the KMS whenever one is configured.
async fn encrypt_with_new_key(
    blockstore: &NamespaceView,
    kms: Option<&std::sync::Arc<dyn kms::KmsService>>,
    doc_ref_bytes: &[u8],
    field_name: &str,
    key_field_name: Option<&str>,
    value_bytes: &[u8],
) -> Result<(Vec<u8>, Cid), String> {
    if let Some(kms_svc) = kms {
        // KMS path: the KMS generates + persists the Encryption block (in its
        // KeyStore) and returns the CID + plain key for us to encrypt the field
        // delta with. The scope is keyed by the node-local DocRef: the public
        // DocID is not yet known on the create path.
        let scope = kms::KeyScope::Document {
            doc_id: hex::encode(doc_ref_bytes),
            field: key_field_name.map(str::to_owned),
        };
        let ctx = kms::RequestContext::anonymous();
        let (enc_cid, key) = kms_svc
            .generate_key(&ctx, scope)
            .await
            .map_err(|e| format!("kms generate_key: {e}"))?;
        let encrypted = encrypt_delta(value_bytes, &key)?;
        tracing::debug!(
            field = %field_name,
            ciphertext_len = encrypted.len(),
            "Encrypted field delta via KMS"
        );
        return Ok((encrypted, enc_cid));
    }

    // Legacy path: inline key generation + direct block store.
    let key = defra_core::encryption::generate_encryption_key_for(doc_ref_bytes, key_field_name);
    let encrypted = encrypt_delta(value_bytes, &key)?;

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

    Ok((encrypted, enc_cid))
}

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
    let mut encryption_cids: Vec<Cid> = Vec::new();

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

    // The composite block's encryption link is set only for whole-document
    // encryption, so its presence is what records that policy in the DAG. Read
    // once, before the field loop, and reused for the composite block below.
    // Skipped when this write sets the policy itself, since every field then
    // takes the explicit path anyway.
    let explicit_doc_encryption = encryption_config.is_some_and(|enc| enc.encrypt_doc);
    let inherited_doc_encryption_cid = if explicit_doc_encryption {
        None
    } else {
        inherited_encryption_cid(blockstore, &composite_heads).await?
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

            let explicit = encryption_config.filter(|enc| enc.should_encrypt_field(field_name));
            let (value_bytes, encryption_cid) = if let Some(enc) = explicit {
                let key_field_name = if enc.should_encrypt_individual_field(field_name) {
                    Some(field_name.as_str())
                } else {
                    None
                };

                let (encrypted, enc_cid) = encrypt_with_new_key(
                    blockstore,
                    kms,
                    &doc_ref_bytes,
                    field_name,
                    key_field_name,
                    &value_bytes,
                )
                .await?;
                encryption_cids.push(enc_cid);
                (encrypted, Some(enc_cid))
            } else if let Some((key, enc_cid)) =
                inherited_encryption(blockstore, &heads, kms).await?
            {
                let encrypted = encrypt_delta(&value_bytes, &key)?;

                tracing::debug!(
                    field = %field_name,
                    ciphertext_len = encrypted.len(),
                    "Encrypted field delta with the previous block's key"
                );

                (encrypted, Some(enc_cid))
            } else if inherited_doc_encryption_cid.is_some() {
                // The document is encrypted as a whole but this field has no
                // history of its own to inherit from, so mint it a key under
                // the document-level policy rather than dropping to plaintext.
                let (encrypted, enc_cid) = encrypt_with_new_key(
                    blockstore,
                    kms,
                    &doc_ref_bytes,
                    field_name,
                    None,
                    &value_bytes,
                )
                .await?;
                encryption_cids.push(enc_cid);
                (encrypted, Some(enc_cid))
            } else {
                (value_bytes, None)
            };

            let is_counter = doc
                .fields()
                .get(field_name)
                .map(|f| f.crdt_type().is_counter())
                .unwrap_or(false)
                || doc.get_counter_delta(field_name).is_some();

            let nonce: i64 = if is_counter && !is_create {
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

    let composite_payload = CompositeDeltaPayload {
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 1,
    };

    let composite_encryption_cid = if explicit_doc_encryption {
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
        encryption_cids.push(enc_cid);
        Some(enc_cid)
    } else {
        // The composite payload is never encrypted with the key, only linked,
        // so the CID alone is enough here.
        inherited_doc_encryption_cid
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
        encryption_cids,
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

    let composite_encryption_cid = inherited_encryption_cid(blockstore, &composite_heads).await?;

    let mut composite_block = Block::new_with_options(
        CrdtDelta::Composite(composite_payload),
        composite_heads,
        vec![],
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
        encryption_cids: vec![],
    })
}

#[cfg(test)]
#[path = "write_encryption_tests.rs"]
mod encryption_derivation_tests;

#[cfg(test)]
#[path = "write_kms_tests.rs"]
mod kms_write_tests;

#[cfg(test)]
#[path = "write_counter_tests.rs"]
mod counter_write_tests;
