use super::*;

/// Pre-computed blocks ready for batch insertion into storage.
///
/// All (key, value) pairs are accumulated during pure computation (no storage
/// access). The caller inserts them into blockstore/headstore inside a transaction.
#[derive(Debug, Clone)]
pub struct ComputedBlocks {
    pub blockstore_entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub encstore_entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub headstore_entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub block_result: BlockResult,
}

/// Compute all document blocks without any storage access.
///
/// This is the CREATE-only path extracted from `write_document_blocks()`.
/// For creates: priority is always 1, no headstore reads, no prev heads.
/// The entire computation is a pure function of the inputs.
///
/// For each field: CBOR encode → optional encrypt → build Block → optional sign →
/// serialize → CID. Then builds composite block the same way.
/// Accumulates all (key, value) pairs instead of writing to storage.
pub fn compute_document_blocks(
    doc: &Document,
    schema_version_id: &str,
    encryption_config: Option<&EncryptionConfig>,
    signing_config: Option<&SigningConfig>,
) -> Result<ComputedBlocks, String> {
    let doc_id = doc
        .id()
        .ok_or_else(|| "Document must have an ID".to_string())?;
    let doc_id_str = doc_id.to_string();
    let doc_id_bytes = doc_id_str.as_bytes().to_vec();

    let mut blockstore_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut encstore_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut headstore_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut field_links: Vec<DAGLink> = Vec::new();
    let mut field_cids: Vec<Cid> = Vec::new();

    let priority: u64 = 1; // Always 1 for creates

    for (field_name, field_value) in doc.values() {
        if field_name == "_docID" {
            continue;
        }

        // For counter fields during creates, use the raw delta if set
        let cbor_value = if let Some(delta) = doc.get_counter_delta(field_name) {
            delta
        } else {
            field_value.value()
        };

        let value_bytes = encode_value_as_cbor(cbor_value)?;

        // Encrypt delta and create Encryption metadata block if configured
        let (value_bytes, encryption_cid) = if let Some(enc) = encryption_config {
            if enc.should_encrypt_field(field_name) {
                let key = enc.get_or_generate_key(field_name, &doc_id_str);
                let encrypted = encrypt_delta(&value_bytes, &key)?;

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
                let enc_bytes = enc_block
                    .to_dag_cbor()
                    .map_err(|e| format!("Failed to encode encryption block: {}", e))?;
                let enc_cid = generate_cid_from_bytes(&enc_bytes)
                    .map_err(|e| format!("Failed to generate encryption CID: {}", e))?;
                encstore_entries.push((enc_cid.to_bytes(), enc_bytes));

                (encrypted, Some(enc_cid))
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

        // For creates, heads are always empty → nonce is always 0
        let nonce: i64 = 0;

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

        // No prev heads for creates
        let mut field_block = Block::new_with_options(delta, vec![], vec![], encryption_cid, None);

        if let Some(signer) = signing_config {
            if let Some((sig_cid, sig_cbor)) = compute_signature(&field_block, signer)? {
                blockstore_entries.push((sig_cid.to_bytes(), sig_cbor));
                field_block.signature = Some(sig_cid);
            }
        }

        let field_block_bytes = field_block
            .to_dag_cbor()
            .map_err(|e| format!("Failed to encode field block: {}", e))?;
        let field_cid = generate_cid_from_bytes(&field_block_bytes)
            .map_err(|e| format!("Failed to generate field CID: {}", e))?;

        blockstore_entries.push((field_cid.to_bytes(), field_block_bytes));

        // Head entry: /d/{doc_id}/{field_name}/{cid} → priority
        let head_key = HeadstoreDocKey::new(&doc_id_str, field_name, field_cid);
        let priority_bytes = encode_priority_varint(priority);
        headstore_entries.push((head_key.bytes(), priority_bytes));
        headstore_entries.push((priority_index_key(&doc_id_str, priority, field_cid), vec![]));

        field_links.push(DAGLink::new(field_name.clone(), field_cid));
        field_cids.push(field_cid);
    }

    // Composite encryption CID for doc-level encryption
    let composite_encryption_cid = if let Some(enc) = encryption_config {
        if enc.encrypt_doc {
            let key = enc.get_or_generate_key("", &doc_id_str);
            let enc_block = Encryption {
                doc_id: doc_id_str.as_bytes().to_vec(),
                field_name: None,
                key: key.to_vec(),
            };
            let enc_bytes = enc_block
                .to_dag_cbor()
                .map_err(|e| format!("Failed to encode composite encryption block: {}", e))?;
            let enc_cid = generate_cid_from_bytes(&enc_bytes)
                .map_err(|e| format!("Failed to generate composite encryption CID: {}", e))?;
            encstore_entries.push((enc_cid.to_bytes(), enc_bytes));
            Some(enc_cid)
        } else {
            None
        }
    } else {
        None
    };

    let composite_payload = CompositeDeltaPayload {
        doc_id: doc_id_bytes,
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 1, // Active document
    };

    let mut composite_block = Block::new_with_options(
        CrdtDelta::Composite(composite_payload),
        vec![], // No prev heads for creates
        field_links,
        composite_encryption_cid,
        None,
    );

    if let Some(signer) = signing_config {
        if let Some((sig_cid, sig_cbor)) = compute_signature(&composite_block, signer)? {
            blockstore_entries.push((sig_cid.to_bytes(), sig_cbor));
            composite_block.signature = Some(sig_cid);
        }
    }

    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode composite block: {}", e))?;
    let composite_cid = generate_cid_from_bytes(&composite_bytes)
        .map_err(|e| format!("Failed to generate composite CID: {}", e))?;

    blockstore_entries.push((composite_cid.to_bytes(), composite_bytes.clone()));

    let composite_head_key = HeadstoreDocKey::new(&doc_id_str, "C", composite_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore_entries.push((composite_head_key.bytes(), priority_bytes));
    headstore_entries.push((
        priority_index_key(&doc_id_str, priority, composite_cid),
        vec![],
    ));

    Ok(ComputedBlocks {
        blockstore_entries,
        encstore_entries,
        headstore_entries,
        block_result: BlockResult {
            cid: composite_cid,
            block: composite_bytes,
            doc_id: doc_id_str,
            field_cids,
        },
    })
}

/// Batch-insert pre-computed blocks into blockstore and headstore.
pub async fn insert_computed_blocks(
    blockstore: &NamespaceView,
    encstore: &NamespaceView,
    headstore: &NamespaceView,
    blocks: &ComputedBlocks,
) -> Result<(), String> {
    for (key, value) in &blocks.blockstore_entries {
        blockstore
            .set(key, value)
            .await
            .map_err(|e| format!("Failed to store block: {}", e))?;
    }
    for (key, value) in &blocks.encstore_entries {
        encstore
            .set(key, value)
            .await
            .map_err(|e| format!("Failed to store encryption block: {}", e))?;
    }
    for (key, value) in &blocks.headstore_entries {
        headstore
            .set(key, value)
            .await
            .map_err(|e| format!("Failed to write head: {}", e))?;
    }
    Ok(())
}
