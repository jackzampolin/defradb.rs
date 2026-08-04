#[cfg(test)]
mod unit_tests {
    use super::super::*;
    use document::NormalValue;
    use storage::backends::memory::MemoryStore;

    fn commit(doc_id: &str, field_name: &str) -> Document {
        let mut commit = Document::new();
        commit.set("docID", NormalValue::String(doc_id.to_string()));
        commit.set("fieldName", NormalValue::String(field_name.to_string()));
        commit
    }

    #[test]
    fn test_commits_query_options_default() {
        let opts = CommitsQueryOptions::default();
        assert!(opts.doc_id.is_none());
        assert!(opts.cid.is_none());
        assert!(opts.depth.is_none());
        assert!(opts.height_start.is_none());
        assert!(opts.height_end.is_none());
        assert!(opts.field_name.is_none());
    }

    #[test]
    fn sort_commits_preserves_document_discovery_order() {
        let fetcher = CommitsFetcher::<MemoryStore>::new(Arc::new(TokioMutex::new(None)));
        let mut commits = vec![
            commit("z-first", "_C"),
            commit("z-first", "name"),
            commit("a-second", "_C"),
            commit("a-second", "age"),
        ];

        fetcher.sort_commits_go_order(&mut commits);

        let actual: Vec<_> = commits
            .iter()
            .map(|commit| {
                (
                    commit.get("docID").and_then(|value| value.as_str()),
                    commit.get("fieldName").and_then(|value| value.as_str()),
                )
            })
            .collect();
        assert_eq!(
            actual,
            vec![
                (Some("z-first"), Some("name")),
                (Some("z-first"), Some("_C")),
                (Some("a-second"), Some("age")),
                (Some("a-second"), Some("_C")),
            ]
        );
    }
}

#[cfg(test)]
mod additional_tests {
    #[test]
    fn test_looks_like_cidv1() {
        use crate::commits_fetcher::CommitsFetcher;
        use storage::backends::memory::MemoryStore;

        assert!(CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist"
        ));
        assert!(CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q"
        ));

        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "fhbnjfahfhfhanfhga"
        ));
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1("short"));
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "randomtext"
        ));
    }
}

#[cfg(test)]
mod shared_owner_tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use async_lock::Mutex;
    use defra_core::{Block, CrdtDelta, LwwDeltaPayload};
    use document::{DocID, NormalValue};
    use storage::backends::MemoryStore;

    use crate::commits_fetcher::{CommitsFetcher, CommitsQueryOptions};
    use crate::{VersionedFetcher, DB};

    #[tokio::test]
    async fn shared_field_cid_fans_out_to_every_owner() {
        let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
        let mut data = Vec::new();
        ciborium::into_writer(&NormalValue::String("shared".to_string()), &mut data).unwrap();
        let block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority: 1,
                data,
            }),
            vec![],
            vec![],
        );
        let cid = block.generate_cid().unwrap();
        let owners = [
            DocID::new_v0(defra_core::block::generate_cid_from_bytes(b"owner-a").unwrap())
                .to_string(),
            DocID::new_v0(defra_core::block::generate_cid_from_bytes(b"owner-b").unwrap())
                .to_string(),
        ];

        let txn = db.new_txn(false).await.unwrap();
        {
            let blockstore = txn.blockstore().unwrap();
            let systemstore = txn.systemstore().unwrap();
            blockstore
                .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
                .await
                .unwrap();
            for owner in &owners {
                crate::doc_id_map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), owner)
                    .await
                    .unwrap();
            }
        }
        txn.commit().await.unwrap();

        let commits_txn = db.new_txn(true).await.unwrap();
        let commits = CommitsFetcher::new(Arc::new(Mutex::new(Some(commits_txn))))
            .fetch_commits(&CommitsQueryOptions {
                cid: Some(cid.to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let commit_owners: HashSet<_> = commits
            .iter()
            .filter_map(|commit| commit.get("docID").and_then(|value| value.as_str()))
            .collect();
        assert_eq!(commit_owners, owners.iter().map(String::as_str).collect());

        let version_txn = db.new_txn(true).await.unwrap();
        let documents = VersionedFetcher::new(Arc::new(Mutex::new(Some(version_txn))))
            .get_documents_at_cid(&cid.to_string(), None)
            .await
            .unwrap();
        let document_owners: HashSet<_> = documents
            .iter()
            .filter_map(|document| document.id().map(ToString::to_string))
            .collect();
        assert_eq!(document_owners, owners.into_iter().collect());
    }
}
