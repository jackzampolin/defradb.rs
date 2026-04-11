//! SE artifact receiver.
//!
//! Deserializes, validates, and stores incoming SE artifacts from P2P
//! replication. This is the consumer side of the SE producer-consumer model.
//!
//! Artifacts arrive as CBOR-encoded PushSEArtifactsRequest messages via
//! the SE request protocol. The receiver converts them to crypto::se::Artifact
//! structs, validates structure, and writes to the datastore.

use crypto::se::Artifact;
use storage::corekv::Writer;

use super::storage::store_artifacts;
use super::validate::validate_artifact;

/// Deserialize SE artifacts from a CBOR-encoded PushSEArtifactsRequest.
///
/// Returns the collection_id and a vec of crypto::se::Artifact structs.
/// Uses serde_cbor for deserialization (same format used in P2P messaging).
pub fn deserialize_artifacts(data: &[u8]) -> std::result::Result<ReceivedBatch, DeserializeError> {
    #[derive(serde::Deserialize)]
    struct RawArtifact {
        #[serde(rename = "DocID")]
        doc_id: String,
        #[serde(rename = "IndexID")]
        index_id: String,
        #[serde(rename = "SearchTag", with = "serde_bytes")]
        search_tag: Vec<u8>,
    }

    #[derive(serde::Deserialize)]
    struct RawRequest {
        #[serde(rename = "CollectionID")]
        collection_id: String,
        #[serde(rename = "Artifacts")]
        artifacts: Vec<RawArtifact>,
    }

    let raw: RawRequest =
        serde_cbor::from_slice(data).map_err(|e| DeserializeError(e.to_string()))?;

    let artifacts = raw
        .artifacts
        .into_iter()
        .map(|a| Artifact::new(&raw.collection_id, a.doc_id, a.index_id, a.search_tag))
        .collect();

    Ok(ReceivedBatch {
        collection_id: raw.collection_id,
        artifacts,
    })
}

/// A deserialized batch of SE artifacts.
#[derive(Debug)]
pub struct ReceivedBatch {
    pub collection_id: String,
    pub artifacts: Vec<Artifact>,
}

/// Error from CBOR deserialization of SE artifacts.
#[derive(Debug, thiserror::Error)]
#[error("SE artifact deserialization failed: {0}")]
pub struct DeserializeError(String);

/// Receive, validate, and store SE artifacts.
///
/// This is the main entry point for the receiver side. It:
/// 1. Deserializes from CBOR
/// 2. Validates each artifact
/// 3. Stores valid artifacts in the datastore
///
/// Invalid artifacts are logged and skipped (not fatal).
pub async fn receive_and_store<S: Writer>(
    store: &mut S,
    data: &[u8],
) -> std::result::Result<ReceiveResult, DeserializeError> {
    let batch = deserialize_artifacts(data)?;

    let mut stored = 0usize;
    let mut rejected = 0usize;

    let valid_artifacts: Vec<Artifact> = batch
        .artifacts
        .into_iter()
        .filter(|a| match validate_artifact(a) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(
                    collection_id = %batch.collection_id,
                    doc_id = %a.doc_id,
                    error = %e,
                    "Rejected invalid SE artifact"
                );
                rejected += 1;
                false
            }
        })
        .collect();

    if !valid_artifacts.is_empty() {
        store_artifacts(store, &valid_artifacts)
            .await
            .map_err(|e| DeserializeError(format!("storage failed: {}", e)))?;
        stored = valid_artifacts.len();
    }

    Ok(ReceiveResult {
        collection_id: batch.collection_id,
        stored,
        rejected,
    })
}

/// Result of receiving and storing SE artifacts.
#[derive(Debug)]
pub struct ReceiveResult {
    pub collection_id: String,
    pub stored: usize,
    pub rejected: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::se::SEARCH_TAG_SIZE;

    fn build_cbor_request(collection_id: &str, artifacts: Vec<(&str, &str, Vec<u8>)>) -> Vec<u8> {
        use serde::Serialize;

        #[derive(Serialize)]
        struct Art {
            #[serde(rename = "DocID")]
            doc_id: String,
            #[serde(rename = "IndexID")]
            index_id: String,
            #[serde(rename = "SearchTag", with = "serde_bytes")]
            search_tag: Vec<u8>,
        }

        #[derive(Serialize)]
        struct Req {
            #[serde(rename = "CollectionID")]
            collection_id: String,
            #[serde(rename = "Artifacts")]
            artifacts: Vec<Art>,
        }

        let req = Req {
            collection_id: collection_id.to_string(),
            artifacts: artifacts
                .into_iter()
                .map(|(doc, idx, tag)| Art {
                    doc_id: doc.to_string(),
                    index_id: idx.to_string(),
                    search_tag: tag,
                })
                .collect(),
        };

        let mut bytes = Vec::new();
        ciborium::into_writer(&req, &mut bytes).unwrap();
        bytes
    }

    #[test]
    fn test_deserialize_valid() {
        let tag = vec![0xABu8; SEARCH_TAG_SIZE];
        let data = build_cbor_request("col_v1", vec![("doc1", "age", tag.clone())]);

        let batch = deserialize_artifacts(&data).unwrap();
        assert_eq!(batch.collection_id, "col_v1");
        assert_eq!(batch.artifacts.len(), 1);
        assert_eq!(batch.artifacts[0].doc_id, "doc1");
        assert_eq!(batch.artifacts[0].index_id, "age");
        assert_eq!(batch.artifacts[0].search_tag, tag);
        assert_eq!(batch.artifacts[0].collection_id, "col_v1");
    }

    #[test]
    fn test_deserialize_invalid_cbor() {
        let result = deserialize_artifacts(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_multiple_artifacts() {
        let tag = vec![0u8; SEARCH_TAG_SIZE];
        let data = build_cbor_request(
            "users",
            vec![("doc1", "age", tag.clone()), ("doc2", "name", tag.clone())],
        );

        let batch = deserialize_artifacts(&data).unwrap();
        assert_eq!(batch.artifacts.len(), 2);
    }
}
