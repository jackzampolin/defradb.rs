/// Store implementations for DefraDB
///
/// This module provides 7 specialized stores with namespace isolation:
/// - RootStore: Foundation store without namespacing
/// - Datastore: Document and collection data with chunking support
/// - Blockstore: IPLD blocks with merge tracking for CRDTs
/// - Headstore: Merkle tree heads and schema definitions
/// - Systemstore: Metadata, configuration, and sequence counters
/// - Peerstore: Peer and replication metadata
/// - Encstore: Encrypted blocks (uses blockstore implementation)
///
/// The Multistore coordinator provides unified access to all stores.
pub mod blockstore;
pub mod datastore;
pub mod headstore;
pub mod multistore;
pub mod peerstore;
pub mod retry_info;
pub mod rootstore;
pub mod systemstore;

// Re-export commonly used types
pub use blockstore::Blockstore;
pub use datastore::{Datastore, CHUNK_SIZE};
pub use headstore::Headstore;
pub use multistore::Multistore;
pub use peerstore::Peerstore;
pub use retry_info::{PersistedPushRetry, RetryInfo};
pub use rootstore::RootStore;
pub use systemstore::Systemstore;

#[cfg(not(target_arch = "wasm32"))]
pub use multistore::MemoryMultistore;

#[cfg(all(feature = "redb", not(target_arch = "wasm32")))]
pub use multistore::RedbMultistore;
