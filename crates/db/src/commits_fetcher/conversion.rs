//! Block-to-document conversion and nested link/head building.

use std::collections::HashMap;

use cid::Cid;
use defra_core::block::{Block, Signature};
use document::Document;
use serde_json::{json, Value as JsonValue};
use storage::corekv::Store;

use crate::error::{Error, Result};
use crate::txn::DbTxn;

use super::CommitsFetcher;

impl<S: Store> CommitsFetcher<S> {
    /// Load a block from blockstore by CID
    pub(super) async fn load_block(&self, txn: &mut DbTxn<S>, cid: &Cid) -> Result<Block> {
        let blockstore = txn.blockstore()?;

        let key = cid.to_bytes();
        let data = blockstore
            .get(&key)
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            })?;

        let block = Block::from_dag_cbor(&data)
            .map_err(|e| Error::Serialization(format!("Failed to decode block: {}", e)))?;
        tracing::debug!(
            cid = %cid,
            data_len = data.len(),
            signature = ?block.signature,
            "Loaded block from blockstore"
        );
        Ok(block)
    }

    /// Load a signature block from blockstore by CID
    pub(super) async fn load_signature_block(
        &self,
        txn: &mut DbTxn<S>,
        cid: &Cid,
    ) -> Result<Signature> {
        let blockstore = txn.blockstore()?;

        let key = cid.to_bytes();
        let data = blockstore
            .get(&key)
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| Error::Serialization("signature block not found".to_string()))?;

        Signature::from_dag_cbor(&data)
            .map_err(|e| Error::Serialization(format!("Failed to decode signature block: {}", e)))
    }

    /// Convert a block to a commit document.
    ///
    /// Loads linked blocks and head blocks to populate the `height` field
    /// in nested link/head objects, matching Go DefraDB behavior.
    pub(super) async fn block_to_commit_doc(
        &self,
        txn: &mut DbTxn<S>,
        cid: &Cid,
        block: &Block,
    ) -> Result<Document> {
        tracing::debug!(
            cid = %cid,
            signature = ?block.signature,
            encryption = ?block.encryption,
            "Converting block to commit document"
        );
        let mut map = HashMap::new();

        map.insert("cid".to_string(), json!(cid.to_string()));
        map.insert("height".to_string(), json!(block.delta.priority() as i64));

        let field_name = self.get_field_name(&block.delta);
        map.insert(
            "fieldName".to_string(),
            field_name.map(|s| json!(s)).unwrap_or(JsonValue::Null),
        );

        let doc_id = self.get_doc_id(&block.delta);
        map.insert(
            "docID".to_string(),
            doc_id.map(|s| json!(s)).unwrap_or(JsonValue::Null),
        );

        let delta_data = self.get_delta_data(&block.delta);
        if let Some(data) = delta_data {
            use base64::Engine;
            map.insert(
                "delta".to_string(),
                json!(base64::engine::general_purpose::STANDARD.encode(&data)),
            );
        } else {
            map.insert("delta".to_string(), JsonValue::Null);
        }

        let schema_version_id = self.get_schema_version_id(&block.delta);
        map.insert(
            "collectionVersionId".to_string(),
            schema_version_id
                .map(|s| json!(s))
                .unwrap_or(JsonValue::Null),
        );

        // links
        let mut links: Vec<JsonValue> = Vec::new();
        if let Some(block_links) = &block.links {
            for link in block_links {
                match self.load_block(txn, &link.link).await {
                    Ok(linked_block) => {
                        let height = linked_block.delta.priority() as i64;
                        let fn_from_delta = self.get_field_name(&linked_block.delta);
                        let field_name = if link.name.is_empty() {
                            fn_from_delta.clone().unwrap_or_default()
                        } else {
                            link.name.clone()
                        };
                        let doc_id = self.get_doc_id(&linked_block.delta);

                        let nested_links = self.build_simple_links(txn, &linked_block).await;
                        let nested_heads = self.build_simple_heads(txn, &linked_block).await;

                        links.push(json!({
                            "cid": link.link.to_string(),
                            "fieldName": field_name,
                            "height": height,
                            "docID": doc_id,
                            "links": nested_links,
                            "heads": nested_heads,
                        }));
                    }
                    Err(_) => {
                        links.push(json!({
                            "cid": link.link.to_string(),
                            "fieldName": link.name,
                            "height": JsonValue::Null,
                        }));
                    }
                };
            }
        }
        map.insert("links".to_string(), json!(links));

        // heads
        let mut heads: Vec<JsonValue> = Vec::new();
        if let Some(block_heads) = &block.heads {
            for head_cid in block_heads {
                match self.load_block(txn, head_cid).await {
                    Ok(head_block) => {
                        let height = head_block.delta.priority() as i64;
                        let field_name = self.get_field_name(&head_block.delta);
                        let doc_id = self.get_doc_id(&head_block.delta);

                        let nested_links = self.build_simple_links(txn, &head_block).await;
                        let nested_heads = self.build_simple_heads(txn, &head_block).await;

                        heads.push(json!({
                            "cid": head_cid.to_string(),
                            "height": height,
                            "fieldName": field_name,
                            "docID": doc_id,
                            "links": nested_links,
                            "heads": nested_heads,
                        }));
                    }
                    Err(_) => {
                        heads.push(json!({
                            "cid": head_cid.to_string(),
                            "height": JsonValue::Null,
                        }));
                    }
                };
            }
        }
        map.insert("heads".to_string(), json!(heads));

        // signature
        let sig_value = if let Some(sig_cid) = &block.signature {
            tracing::debug!(sig_cid = %sig_cid, "Loading signature block");
            match self.load_signature_block(txn, sig_cid).await {
                Ok(sig) => {
                    let sig_type = match sig.header.sig_type {
                        defra_core::block::SignatureType::ES256K => "ES256K",
                        defra_core::block::SignatureType::EdDSA => "EdDSA",
                        defra_core::block::SignatureType::BLS => "BLS",
                    };
                    let sig_json = json!({
                        "type": sig_type,
                        "identity": String::from_utf8_lossy(&sig.header.identity).to_string(),
                        "value": hex::encode(&sig.value),
                    });
                    tracing::debug!(sig_type, "Signature loaded successfully");
                    sig_json
                }
                Err(e) => {
                    tracing::debug!(sig_cid = %sig_cid, error = %e, "Failed to load signature block");
                    JsonValue::Null
                }
            }
        } else {
            tracing::debug!(cid = %cid, "Block has no signature CID");
            JsonValue::Null
        };
        map.insert("signature".to_string(), sig_value);

        Document::from_map(map)
            .map_err(|e| Error::Serialization(format!("Failed to create document: {}", e)))
    }

    /// Build simple links array (without recursive nesting) for nested queries.
    /// Returns array of {cid, fieldName, height} for each link.
    pub(super) async fn build_simple_links(
        &self,
        txn: &mut DbTxn<S>,
        block: &Block,
    ) -> Vec<JsonValue> {
        let mut links = Vec::new();
        if let Some(block_links) = &block.links {
            for link in block_links {
                let (height, field_name) = match self.load_block(txn, &link.link).await {
                    Ok(linked_block) => {
                        let h = linked_block.delta.priority() as i64;
                        let fn_from_delta = self.get_field_name(&linked_block.delta);
                        let fn_val = if link.name.is_empty() {
                            fn_from_delta.unwrap_or_default()
                        } else {
                            link.name.clone()
                        };
                        (json!(h), fn_val)
                    }
                    Err(_) => (JsonValue::Null, link.name.clone()),
                };
                links.push(json!({
                    "cid": link.link.to_string(),
                    "fieldName": field_name,
                    "height": height,
                }));
            }
        }
        links
    }

    /// Build simple heads array (without recursive nesting) for nested queries.
    /// Returns array of {cid, fieldName, height} for each head.
    pub(super) async fn build_simple_heads(
        &self,
        txn: &mut DbTxn<S>,
        block: &Block,
    ) -> Vec<JsonValue> {
        let mut heads = Vec::new();
        if let Some(block_heads) = &block.heads {
            for head_cid in block_heads {
                match self.load_block(txn, head_cid).await {
                    Ok(head_block) => {
                        let height = head_block.delta.priority() as i64;
                        let field_name = self.get_field_name(&head_block.delta);
                        heads.push(json!({
                            "cid": head_cid.to_string(),
                            "height": height,
                            "fieldName": field_name,
                        }));
                    }
                    Err(_) => {
                        heads.push(json!({
                            "cid": head_cid.to_string(),
                            "height": JsonValue::Null,
                        }));
                    }
                };
            }
        }
        heads
    }
}
