/// Key hierarchy for DefraDB storage
///
/// This module provides 33 key types organized across 6 stores:
/// - Datastore (9 keys): Document, collection, and CRDT data
/// - Headstore (6 keys): Merkle tree heads, definitions, and priority indexes
/// - Systemstore (11 keys): Metadata and configuration
/// - Peerstore (4 keys): Peer and replication metadata
/// - Blockstore (2 keys): IPLD blocks and merge tracking
/// - Encstore (1 key): Encrypted blocks (shares implementation with Blockstore)
///
/// Each key type implements the Key trait, providing:
/// - bytes(): Serialize to byte representation for storage
/// - to_string(): Human-readable string for debugging/logging
///
/// # Key Encoding
///
/// Keys use various encoding strategies:
/// - CockroachDB-style varint encoding for integers (maintains sort order)
/// - Raw CID bytes for content-addressed storage
/// - String concatenation for metadata keys
/// - Type-specific encoding for field values
///
/// # Example
///
/// ```ignore
/// use storage::keys::datastore::DataStoreKey;
/// use storage::keys::utils::InstanceType;
/// use storage::corekv::Key;
///
/// let key = DataStoreKey::new(
///     1,
///     InstanceType::Value,
///     42,
///     "fieldname",
/// );
///
/// let bytes = key.bytes(); // Serialize for storage
/// let string = key.to_string(); // Debug representation
/// ```
pub mod blockstore;
pub mod crdt;
pub mod datastore;
pub mod doc_id_index;
pub mod document;
pub mod encstore;
pub mod headstore;
pub mod peerstore;
pub mod systemstore;
pub mod utils;

// Re-export commonly used types
pub use blockstore::{BlockstoreKey, ToMergeIndexKey, MERGE_PREFIX};
pub use crdt::{CRDTNonceKey, CRDTNoncePrefix, CRDTPriorityKey, CRDTValueKey};
pub use datastore::{
    DataStoreKey, DatastoreSE, IndexDataStoreKey, IndexedField, PrimaryDataStoreKey, ViewCacheKey,
    DATASTORE_DOC_VERSION_FIELD_ID,
};
pub use doc_id_index::{
    decode_doc_short_id, decode_doc_short_id_prefix, encode_doc_short_id, BlockCIDToDocIDKey,
    DocIDToDocRefKey, DocRef, DocShortIDSequenceKey, DocShortIDToDocIDAliasKey,
    DocShortIDToDocIDKey, DOC_ID_INDEX, DOC_SHORT_ID_SEQ,
};
pub use document::{deleted_doc_key, doc_key, DELETED_KEY_PREFIX, DOC_KEY_PREFIX};
pub use encstore::EncstoreKey;
pub use headstore::{
    HeadstoreColKey, HeadstoreCollectionDefinition, HeadstoreCollectionSetDefinition,
    HeadstoreDocKey, HeadstoreFieldDefinition, HeadstorePriorityKey,
};
pub use peerstore::{
    PeerstoreSERetry, ReplicatorKey, ReplicatorRetryCommitKey, ReplicatorRetryDocIDKey,
    ReplicatorRetryIDKey,
};
pub use systemstore::{
    CollectionID, CollectionIDSequenceKey, CollectionKey, CollectionNameKey, CollectionVersionKey,
    FieldID, FieldIDSequenceKey, IndexIDSequenceKey, NodeACPKey, P2PCollectionKey, P2PDocumentKey,
    P2PPendingDagKey, P2PQuarantinedDagKey,
};
pub use utils::{InstanceType, SEPARATOR};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corekv::Key;
    use cid::Cid;
    use std::str::FromStr;

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    #[test]
    fn test_all_key_types_implement_key_trait() {
        // Datastore keys
        let _: Box<dyn Key> = Box::new(DataStoreKey::new(1, InstanceType::Value, 42, "field"));
        let _: Box<dyn Key> = Box::new(PrimaryDataStoreKey::new(1, 42));
        let _: Box<dyn Key> = Box::new(IndexDataStoreKey::new(1, 2, vec![]));
        let _: Box<dyn Key> = Box::new(DatastoreSE::new("col", "idx", vec![], "doc"));
        let _: Box<dyn Key> = Box::new(ViewCacheKey::new(1, 5));
        let _: Box<dyn Key> = Box::new(CRDTValueKey::new("schema", b"doc".to_vec(), "field"));
        let _: Box<dyn Key> = Box::new(CRDTPriorityKey::new("schema", b"doc".to_vec(), "field"));
        let _: Box<dyn Key> = Box::new(CRDTNoncePrefix::new("schema", b"doc".to_vec(), "field"));
        let _: Box<dyn Key> =
            Box::new(CRDTNoncePrefix::new("schema", b"doc".to_vec(), "field").nonce_key(1));

        // Headstore keys
        let cid = test_cid();
        let _: Box<dyn Key> = Box::new(HeadstoreDocKey::new(42, "field", cid));
        let _: Box<dyn Key> = Box::new(HeadstoreColKey::new(1, cid));
        let _: Box<dyn Key> = Box::new(HeadstoreFieldDefinition::new("col", "field", cid));
        let _: Box<dyn Key> = Box::new(HeadstoreCollectionDefinition::new("col", cid));
        let _: Box<dyn Key> = Box::new(HeadstoreCollectionSetDefinition::new("col", cid));
        let _: Box<dyn Key> = Box::new(HeadstorePriorityKey::new(42, 1, cid));

        // Systemstore keys
        let _: Box<dyn Key> = Box::new(CollectionKey::new("col"));
        let _: Box<dyn Key> = Box::new(CollectionID::new("col"));
        let _: Box<dyn Key> = Box::new(CollectionNameKey::new("name"));
        let _: Box<dyn Key> = Box::new(CollectionVersionKey::new("col", "v1"));
        let _: Box<dyn Key> = Box::new(FieldID::new(1, "field"));
        let _: Box<dyn Key> = Box::new(NodeACPKey::new());
        let _: Box<dyn Key> = Box::new(P2PCollectionKey::new("col"));
        let _: Box<dyn Key> = Box::new(P2PDocumentKey::new("doc"));
        let _: Box<dyn Key> = Box::new(CollectionIDSequenceKey::new());
        let _: Box<dyn Key> = Box::new(FieldIDSequenceKey::new(1));
        let _: Box<dyn Key> = Box::new(IndexIDSequenceKey::new("col"));

        // Peerstore keys
        let _: Box<dyn Key> = Box::new(ReplicatorKey::new("rep"));
        let _: Box<dyn Key> = Box::new(ReplicatorRetryIDKey::new("peer"));
        let _: Box<dyn Key> = Box::new(ReplicatorRetryDocIDKey::new("peer", "doc"));
        let _: Box<dyn Key> = Box::new(PeerstoreSERetry::new("peer", "col", "doc"));

        // Blockstore keys
        let _: Box<dyn Key> = Box::new(BlockstoreKey::new(cid));
        let _: Box<dyn Key> = Box::new(ToMergeIndexKey::new(cid));

        // Encstore keys
        let _: Box<dyn Key> = Box::new(EncstoreKey::new(cid));
    }

    #[test]
    fn test_key_count() {
        // Verify we have all active key types.
        // This is a compile-time check that all types exist

        // Datastore: 5 keys
        let _d1 = DataStoreKey::new(1, InstanceType::Value, 42, "f");
        let _d2 = PrimaryDataStoreKey::new(1, 42);
        let _d3 = IndexDataStoreKey::new(1, 2, vec![]);
        let _d4 = DatastoreSE::new("c", "i", vec![], "d");
        let _d5 = ViewCacheKey::new(1, 5);
        let _d6 = CRDTValueKey::new("s", b"d".to_vec(), "f");
        let _d7 = CRDTPriorityKey::new("s", b"d".to_vec(), "f");
        let _d8 = CRDTNoncePrefix::new("s", b"d".to_vec(), "f");
        let _d9 = CRDTNoncePrefix::new("s", b"d".to_vec(), "f").nonce_key(1);

        // Headstore: 6 keys
        let cid = test_cid();
        let _h1 = HeadstoreDocKey::new(42, "f", cid);
        let _h2 = HeadstoreColKey::new(1, cid);
        let _h3 = HeadstoreFieldDefinition::new("c", "f", cid);
        let _h4 = HeadstoreCollectionDefinition::new("c", cid);
        let _h5 = HeadstoreCollectionSetDefinition::new("c", cid);
        let _h6 = HeadstorePriorityKey::new(42, 1, cid);

        // Systemstore: 11 keys
        let _s1 = CollectionKey::new("c");
        let _s2 = CollectionID::new("c");
        let _s3 = CollectionNameKey::new("n");
        let _s4 = CollectionVersionKey::new("c", "v");
        let _s5 = FieldID::new(1, "f");
        let _s6 = NodeACPKey::new();
        let _s7 = P2PCollectionKey::new("c");
        let _s8 = P2PDocumentKey::new("d");
        let _s9 = CollectionIDSequenceKey::new();
        let _s10 = FieldIDSequenceKey::new(1);
        let _s11 = IndexIDSequenceKey::new("c");

        // Peerstore: 4 keys
        let _p1 = ReplicatorKey::new("r");
        let _p2 = ReplicatorRetryIDKey::new("p");
        let _p3 = ReplicatorRetryDocIDKey::new("p", "d");
        let _p4 = PeerstoreSERetry::new("p", "c", "d");

        // Blockstore: 2 keys
        let _b1 = BlockstoreKey::new(cid);
        let _b2 = ToMergeIndexKey::new(cid);

        // Encstore: 1 key
        let _e1 = EncstoreKey::new(cid);

        // Total: 9 + 6 + 11 + 4 + 2 + 1 = 33 active key types.
    }
}
