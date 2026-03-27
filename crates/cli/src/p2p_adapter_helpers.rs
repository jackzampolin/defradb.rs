//! Helper functions for P2P adapter replay logic.

use std::collections::HashSet;

pub(crate) fn collections_requiring_replay(
    effective_collections: &[String],
    collection_cids: &[String],
    existing_collection_ids: &HashSet<String>,
    collections_with_changed_capabilities: &HashSet<String>,
) -> Vec<String> {
    effective_collections
        .iter()
        .zip(collection_cids.iter())
        .filter(|(_, cid)| {
            !existing_collection_ids.contains(*cid)
                || collections_with_changed_capabilities.contains(*cid)
        })
        .map(|(name, _)| name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::collections_requiring_replay;

    #[test]
    fn collections_requiring_replay_replays_existing_collection_when_capability_changes() {
        let effective_collections = vec!["User".to_string()];
        let collection_cids = vec!["cid-user".to_string()];
        let existing_collection_ids = HashSet::from(["cid-user".to_string()]);
        let changed_capabilities = HashSet::from(["cid-user".to_string()]);

        let replay_collections = collections_requiring_replay(
            &effective_collections,
            &collection_cids,
            &existing_collection_ids,
            &changed_capabilities,
        );

        assert_eq!(replay_collections, vec!["User".to_string()]);
    }

    #[test]
    fn collections_requiring_replay_skips_existing_collection_when_capability_matches() {
        let effective_collections = vec!["User".to_string()];
        let collection_cids = vec!["cid-user".to_string()];
        let existing_collection_ids = HashSet::from(["cid-user".to_string()]);
        let changed_capabilities = HashSet::new();

        let replay_collections = collections_requiring_replay(
            &effective_collections,
            &collection_cids,
            &existing_collection_ids,
            &changed_capabilities,
        );

        assert!(replay_collections.is_empty());
    }
}
