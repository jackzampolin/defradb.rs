use crypto::se::SEARCH_TAG_SIZE;
use db::merge::se::receiver::*;
use serde::Serialize;
use storage::Store;

fn build_cbor_request(collection_id: &str, artifacts: Vec<(&str, &str, Vec<u8>)>) -> Vec<u8> {
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

#[tokio::test]
async fn test_receive_and_store_reports_stored_doc_ids() {
    let tag = vec![0u8; SEARCH_TAG_SIZE];
    let data = build_cbor_request(
        "users",
        vec![("doc2", "age", tag.clone()), ("doc1", "age", tag.clone())],
    );
    let store = storage::MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let result = receive_and_store(&mut txn, &data).await.unwrap();
    txn.commit().await.unwrap();

    assert_eq!(result.collection_id, "users");
    assert_eq!(result.stored, 2);
    assert_eq!(result.rejected, 0);
    assert_eq!(result.doc_ids, vec!["doc1".to_string(), "doc2".to_string()]);
}
