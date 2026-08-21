use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use cid::Cid;
use defra_core::browser_sync::{
    BrowserSyncDocument, MAX_SYNC_BLOCKS_PER_DOCUMENT, MAX_SYNC_BLOCK_BYTES, MAX_SYNC_ID_BYTES,
    MAX_SYNC_PAYLOAD_BYTES, MAX_SYNC_ROOTS_PER_DOCUMENT,
};
use defra_core::{Block, CrdtDelta, Signature, DAG_CBOR_CODEC};
use storage::corekv::Store;

use super::{BrowserSyncEngine, BrowserSyncError, ValidatedBrowserSyncDocument};

enum DecodedSyncBlock {
    Delta(Box<Block>),
    Signature,
}

#[derive(Clone, Copy)]
enum ExpectedSyncBlock {
    Delta,
    Signature,
}

impl<S: Store + 'static> BrowserSyncEngine<S> {
    pub fn validate_document(
        &self,
        document: &BrowserSyncDocument,
    ) -> Result<ValidatedBrowserSyncDocument, BrowserSyncError> {
        validate_id("doc_id", &document.doc_id)?;
        validate_id("collection_id", &document.collection_id)?;
        if document.roots.is_empty() {
            return Err(BrowserSyncError::Invalid(format!(
                "document {} has invalid root count {}",
                document.doc_id,
                document.roots.len()
            )));
        }
        if document.roots.len() > MAX_SYNC_ROOTS_PER_DOCUMENT {
            return Err(BrowserSyncError::TooLarge(format!(
                "document {} root count {} exceeds {}",
                document.doc_id,
                document.roots.len(),
                MAX_SYNC_ROOTS_PER_DOCUMENT
            )));
        }
        if document.blocks.is_empty() {
            return Err(BrowserSyncError::Invalid(format!(
                "document {} has invalid block count {}",
                document.doc_id,
                document.blocks.len()
            )));
        }
        if document.blocks.len() > MAX_SYNC_BLOCKS_PER_DOCUMENT {
            return Err(BrowserSyncError::TooLarge(format!(
                "document {} block count {} exceeds {}",
                document.doc_id,
                document.blocks.len(),
                MAX_SYNC_BLOCKS_PER_DOCUMENT
            )));
        }

        let mut total_bytes = 0usize;
        let mut blocks = Vec::with_capacity(document.blocks.len());
        let mut decoded_blocks = HashMap::with_capacity(document.blocks.len());
        for block in &document.blocks {
            validate_id("block CID", &block.cid)?;
            let cid = Cid::from_str(&block.cid).map_err(|error| {
                BrowserSyncError::Invalid(format!("invalid block CID '{}': {error}", block.cid))
            })?;
            if cid.codec() != DAG_CBOR_CODEC {
                return Err(BrowserSyncError::Invalid(format!(
                    "block {cid} is not DAG-CBOR"
                )));
            }
            let data = hex::decode(&block.data).map_err(|error| {
                BrowserSyncError::Invalid(format!("invalid block data for {cid}: {error}"))
            })?;
            if data.len() > MAX_SYNC_BLOCK_BYTES {
                return Err(BrowserSyncError::TooLarge(format!(
                    "block {cid} exceeds {} bytes",
                    MAX_SYNC_BLOCK_BYTES
                )));
            }
            total_bytes = total_bytes.saturating_add(data.len());
            if total_bytes > MAX_SYNC_PAYLOAD_BYTES {
                return Err(BrowserSyncError::TooLarge(format!(
                    "document {} exceeds {} bytes",
                    document.doc_id, MAX_SYNC_PAYLOAD_BYTES
                )));
            }
            blockstore::verify_block_cid(&cid, &data)
                .map_err(|error| BrowserSyncError::Invalid(error.to_string()))?;
            let decoded = match Block::from_dag_cbor(&data) {
                Ok(block) => DecodedSyncBlock::Delta(Box::new(block)),
                Err(_) if Signature::from_dag_cbor(&data).is_ok() => DecodedSyncBlock::Signature,
                Err(block_error) => {
                    return Err(BrowserSyncError::Invalid(format!(
                        "invalid document or signature block {cid}: {block_error}"
                    )))
                }
            };
            if decoded_blocks.insert(cid, decoded).is_some() {
                return Err(BrowserSyncError::Invalid(format!("duplicate block {cid}")));
            }
            blocks.push((cid, data));
        }

        let roots = document
            .roots
            .iter()
            .map(|root| {
                validate_id("root CID", root)?;
                Cid::from_str(root).map_err(|error| {
                    BrowserSyncError::Invalid(format!("invalid root CID '{root}': {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if roots.iter().collect::<HashSet<_>>().len() != roots.len() {
            return Err(BrowserSyncError::Invalid(format!(
                "document {} contains duplicate roots",
                document.doc_id
            )));
        }

        let mut reachable = HashSet::new();
        let mut genesis_doc_ids = HashMap::new();
        let mut stack = roots
            .iter()
            .copied()
            .map(|cid| (cid, ExpectedSyncBlock::Delta))
            .collect::<Vec<_>>();
        while let Some((cid, expected)) = stack.pop() {
            let decoded = decoded_blocks.get(&cid).ok_or_else(|| {
                BrowserSyncError::Invalid(format!("referenced block {cid} is missing"))
            })?;
            let block = match (expected, decoded) {
                (ExpectedSyncBlock::Delta, DecodedSyncBlock::Delta(block)) => Some(block),
                (ExpectedSyncBlock::Signature, DecodedSyncBlock::Signature) => None,
                (ExpectedSyncBlock::Delta, DecodedSyncBlock::Signature) => {
                    return Err(BrowserSyncError::Invalid(format!(
                        "document link {cid} points to a signature block"
                    )))
                }
                (ExpectedSyncBlock::Signature, DecodedSyncBlock::Delta(_)) => {
                    return Err(BrowserSyncError::Invalid(format!(
                        "signature link {cid} points to a document block"
                    )))
                }
            };
            if !reachable.insert(cid) {
                continue;
            }
            let Some(block) = block else { continue };
            if block.heads.as_deref().is_none_or(<[Cid]>::is_empty)
                && matches!(block.delta, CrdtDelta::Composite(_))
            {
                genesis_doc_ids.insert(crate::block::builder::derive_doc_id(&cid), cid);
            }
            stack.extend(
                block
                    .heads
                    .iter()
                    .flatten()
                    .copied()
                    .map(|cid| (cid, ExpectedSyncBlock::Delta)),
            );
            stack.extend(
                block
                    .links
                    .iter()
                    .flatten()
                    .map(|link| (link.link, ExpectedSyncBlock::Delta)),
            );
            stack.extend(
                block
                    .signature
                    .map(|cid| (cid, ExpectedSyncBlock::Signature)),
            );
        }
        if reachable.len() != decoded_blocks.len() {
            return Err(BrowserSyncError::Invalid(
                "document contains blocks outside its root DAG".into(),
            ));
        }

        for (cid, decoded) in &decoded_blocks {
            let DecodedSyncBlock::Delta(block) = decoded else {
                continue;
            };
            let schema_version_id = match &block.delta {
                CrdtDelta::Composite(payload) => &payload.schema_version_id,
                CrdtDelta::Lww(payload) => &payload.schema_version_id,
                CrdtDelta::Counter(payload) => &payload.schema_version_id,
                _ => {
                    return Err(BrowserSyncError::Invalid(format!(
                        "block {cid} is not a document delta"
                    )))
                }
            };
            let collection = self
                .db
                .get_collection_by_version_id(schema_version_id)
                .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
                .or_else(|| {
                    self.db
                        .find_collection_by_id(schema_version_id)
                        .ok()
                        .flatten()
                })
                .ok_or_else(|| {
                    BrowserSyncError::Invalid(format!(
                        "schema version {schema_version_id} is not registered"
                    ))
                })?;
            if collection.collection_id() != document.collection_id {
                return Err(BrowserSyncError::Invalid(format!(
                    "block {cid} belongs to collection {}, not {}",
                    collection.collection_id(),
                    document.collection_id
                )));
            }

            match &block.delta {
                CrdtDelta::Composite(_) => {
                    for head in block.heads.iter().flatten() {
                        if !matches!(
                            decoded_blocks.get(head),
                            Some(DecodedSyncBlock::Delta(block))
                                if matches!(block.delta, CrdtDelta::Composite(_))
                        ) {
                            return Err(BrowserSyncError::Invalid(format!(
                                "composite block {cid} has a non-composite head {head}"
                            )));
                        }
                    }

                    let mut linked_fields = HashSet::new();
                    for link in block.links.iter().flatten() {
                        if !linked_fields.insert(link.name.as_str()) {
                            return Err(BrowserSyncError::Invalid(format!(
                                "composite block {cid} links field '{}' more than once",
                                link.name
                            )));
                        }
                        let linked_field = decoded_blocks
                            .get(&link.link)
                            .and_then(|block| match block {
                                DecodedSyncBlock::Delta(block) => field_delta_name(&block.delta),
                                DecodedSyncBlock::Signature => None,
                            })
                            .ok_or_else(|| {
                                BrowserSyncError::Invalid(format!(
                                    "composite block {cid} links non-field block {}",
                                    link.link
                                ))
                            })?;
                        if linked_field != link.name {
                            return Err(BrowserSyncError::Invalid(format!(
                                "composite block {cid} links field '{}' as '{}'",
                                linked_field, link.name
                            )));
                        }
                    }
                }
                CrdtDelta::Lww(payload) => {
                    validate_field_block(cid, block, &payload.field_name, &decoded_blocks)?;
                }
                CrdtDelta::Counter(payload) => {
                    validate_field_block(cid, block, &payload.field_name, &decoded_blocks)?;
                }
                _ => {
                    return Err(BrowserSyncError::Invalid(format!(
                        "block {cid} is not a document delta"
                    )))
                }
            }
        }
        let Some(genesis_cid) = (genesis_doc_ids.len() == 1)
            .then(|| genesis_doc_ids.get(&document.doc_id).copied())
            .flatten()
        else {
            return Err(BrowserSyncError::Invalid(format!(
                "document ID {} does not match its genesis block",
                document.doc_id
            )));
        };

        // Ownership registration must be anchored to the document's
        // cryptographically verified author, never the transport caller
        // (mirrors the replication path's `effective_creator()` convention).
        // An unsigned genesis yields no verified creator; an invalid
        // signature rejects the push outright.
        let verified_genesis_creator = match decoded_blocks.get(&genesis_cid) {
            Some(DecodedSyncBlock::Delta(block)) => match block.signature {
                Some(sig_cid) => {
                    let sig_data = blocks
                        .iter()
                        .find_map(|(cid, data)| (*cid == sig_cid).then_some(data.as_slice()))
                        .ok_or_else(|| {
                            BrowserSyncError::Invalid(format!(
                                "genesis signature block {sig_cid} is missing"
                            ))
                        })?;
                    Some(
                        crate::merge::merge_handler::verify_signature_data(
                            &genesis_cid,
                            block,
                            sig_data,
                        )
                        .map_err(|error| BrowserSyncError::Invalid(error.to_string()))?,
                    )
                }
                None => None,
            },
            _ => None,
        };

        for root in &roots {
            let Some(DecodedSyncBlock::Delta(block)) = decoded_blocks.get(root) else {
                return Err(BrowserSyncError::Invalid(format!(
                    "root {root} is not a composite block"
                )));
            };
            if !matches!(&block.delta, CrdtDelta::Composite(_)) {
                return Err(BrowserSyncError::Invalid(format!(
                    "root {root} is not a composite block"
                )));
            }
        }

        Ok(ValidatedBrowserSyncDocument {
            doc_id: document.doc_id.clone(),
            collection_id: document.collection_id.clone(),
            roots,
            blocks,
            verified_genesis_creator,
        })
    }
}

fn field_delta_name(delta: &CrdtDelta) -> Option<&str> {
    match delta {
        CrdtDelta::Lww(payload) => Some(&payload.field_name),
        CrdtDelta::Counter(payload) => Some(&payload.field_name),
        _ => None,
    }
}

fn validate_field_block(
    cid: &Cid,
    block: &Block,
    field_name: &str,
    decoded_blocks: &HashMap<Cid, DecodedSyncBlock>,
) -> Result<(), BrowserSyncError> {
    if block.links.as_ref().is_some_and(|links| !links.is_empty()) {
        return Err(BrowserSyncError::Invalid(format!(
            "field block {cid} contains document links"
        )));
    }
    for head in block.heads.iter().flatten() {
        let head_field = decoded_blocks
            .get(head)
            .and_then(|block| match block {
                DecodedSyncBlock::Delta(block) => field_delta_name(&block.delta),
                DecodedSyncBlock::Signature => None,
            })
            .ok_or_else(|| {
                BrowserSyncError::Invalid(format!("field block {cid} has a non-field head {head}"))
            })?;
        if head_field != field_name {
            return Err(BrowserSyncError::Invalid(format!(
                "field block {cid} has head {head} for field '{head_field}'"
            )));
        }
    }
    Ok(())
}

fn validate_id(name: &str, value: &str) -> Result<(), BrowserSyncError> {
    if value.is_empty() || value.len() > MAX_SYNC_ID_BYTES {
        return Err(BrowserSyncError::Invalid(format!(
            "{name} has invalid length {}",
            value.len()
        )));
    }
    Ok(())
}
