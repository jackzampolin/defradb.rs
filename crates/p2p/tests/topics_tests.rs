use p2p::DefraTopic;

#[test]
fn system_topics_have_stable_names() {
    assert_eq!(DefraTopic::DocSync.topic_string(), "doc-sync");
    assert_eq!(DefraTopic::SyncBranchable.topic_string(), "sync-branchable");
    assert_eq!(DefraTopic::Encryption.topic_string(), "encryption");
}

#[test]
fn collection_and_document_topics_preserve_ids() {
    let collection_id = "bafkreih3x2qgxr4gpx7qd5kqj7gg6ukipvxc32e3ihdpkwmv5fvnz6wuui";
    let document_id = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";

    assert_eq!(
        DefraTopic::collection(collection_id).topic_string(),
        collection_id
    );
    assert_eq!(
        DefraTopic::document(document_id).topic_string(),
        document_id
    );
}

#[test]
fn known_topic_names_parse_to_their_variants() {
    assert_eq!(DefraTopic::from("doc-sync"), DefraTopic::DocSync);
    assert_eq!(
        DefraTopic::from("sync-branchable"),
        DefraTopic::SyncBranchable
    );
    assert_eq!(DefraTopic::from("encryption"), DefraTopic::Encryption);
    assert_eq!(
        DefraTopic::from("custom-topic"),
        DefraTopic::Custom("custom-topic".to_string())
    );
}

#[test]
fn display_uses_the_wire_topic_name() {
    assert_eq!(DefraTopic::DocSync.to_string(), "doc-sync");
    assert_eq!(DefraTopic::SyncBranchable.to_string(), "sync-branchable");
}

#[cfg(feature = "libp2p-transport")]
#[test]
fn libp2p_topic_has_a_hash() {
    assert!(!DefraTopic::DocSync
        .to_ident_topic()
        .hash()
        .to_string()
        .is_empty());
}
