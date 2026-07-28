//! Database merge handler for processing incoming P2P blocks.
//!
//! This module implements the `MergeHandler` trait from the P2P layer,
//! bridging incoming blocks to the CRDT system for document merging.

mod authorization;
mod batch;
mod collection;
mod composite;
mod composite_fields;
mod composite_heads;
mod composite_persist;
mod counter;
mod definition;
mod dispatch;
mod doc_identity;
mod encryption;
pub(crate) mod error;
pub(crate) mod hook;
mod lww;
mod recovery;
pub(crate) mod se_merge;
mod signature;

pub use error::MergeError;
pub(crate) use error::{CounterMergeResult, LwwMergeResult};
pub(crate) use signature::verify_signature_data;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cid::Cid;
use crdt::traits::{Context, ReplicatedData, ValueReader};
use crdt::{Counter, CounterDelta, Lww, LwwDelta, NumericKind};
use datastore::NamespaceView;
use defra_core::block::{
    Block, CollectionDefinitionDeltaPayload, CrdtDelta, FieldDefinitionDeltaPayload,
};
#[cfg(test)]
use defra_core::merge::MergeBlock;
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use defra_core::types::DocId;
use document::{DocID, Document, NormalValue};
use events::{MergeCompleteData, Message, Update};
use schema::{
    self, CType, CollectionSource, CollectionVersion, FieldDescription, FieldKind, QuerySource,
    ScalarKind,
};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};
use zeroize::Zeroizing;

use db::collection::Collection;
use db::database::DB;
use db::index_manager::IndexManager;
use hook::CompositeMergeHook;

/// Maximum parent-chain depth for merge operations.
///
/// Bounds the work and heap used while traversing a malicious or corrupt DAG.
pub(crate) const MAX_MERGE_DEPTH: usize = 1024;

/// Encode a priority value as a varint (matches Go's binary.PutUvarint).
pub(crate) fn encode_priority_varint(priority: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    let mut n = priority;
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
    buf
}

/// Database merge handler that processes incoming P2P blocks.
///
/// This handler decodes IPLD blocks, extracts CRDT deltas, and applies
/// them to the database using the appropriate CRDT type.
pub struct DbMergeHandler<S: Store, B: blockstore::Blockstore> {
    /// Reference to the database for creating transactions.
    pub(crate) db: Arc<DB<S>>,
    /// Reference to the blockstore for loading linked blocks.
    pub(crate) blockstore: Arc<B>,
    /// Optional merge hook for policy-specific behavior around composite merges.
    composite_merge_hook: std::sync::OnceLock<Arc<dyn CompositeMergeHook>>,
    /// Tracks composite CIDs that have already been merged, preventing
    /// duplicate processing from concurrent dual-broadcast paths (doc topic
    /// + collection topic). Matches Go's `loadComposites` dedup guard.
    pub(crate) merged_composites: std::sync::Mutex<HashSet<Cid>>,
    /// Tracks collection CIDs that have already been merged, preventing
    /// replayed collection blocks from re-adding obsolete collection heads.
    pub(crate) merged_collections: std::sync::Mutex<HashSet<Cid>>,
    /// Optional SE encryption key for generating search artifacts on replicated documents.
    /// When set, the merge handler generates SE artifacts after merging documents
    /// that belong to collections with encrypted indexes.
    se_enc_key: std::sync::OnceLock<Zeroizing<Vec<u8>>>,
    /// Optional KMS service. When set, `decrypt_block_data` routes DEK
    /// retrieval through the KMS (NAC/DAC-gated, cross-peer fetch) instead
    /// of reading the raw key directly from the Encryption block.
    kms: std::sync::OnceLock<Arc<dyn kms::KmsService>>,
    /// Per-document write serialization queue, shared with the DB so that local
    /// writes and P2P merges that touch the same document are mutually
    /// serialized. Ensures concurrent merges (and concurrent local writes) for
    /// the same document are processed one at a time, preventing read-modify-write
    /// races on the CRDT accumulation store (#1021).
    pub(crate) merge_queue: Arc<db::DocWriteQueue>,
    /// Encryption CIDs with a background DEK prefetch currently in flight, so
    /// repeated deliveries of the same deferred field block
    /// (pushlog + gossip + retries) don't fan out duplicate cross-peer
    /// fetches.
    prefetched_dek_cids: Arc<std::sync::Mutex<HashSet<Cid>>>,
}

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    /// Create a new database merge handler.
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        let merge_queue = db.doc_write_queue();
        Self {
            db,
            blockstore,
            composite_merge_hook: std::sync::OnceLock::new(),
            merged_composites: std::sync::Mutex::new(HashSet::new()),
            merged_collections: std::sync::Mutex::new(HashSet::new()),
            se_enc_key: std::sync::OnceLock::new(),
            kms: std::sync::OnceLock::new(),
            merge_queue,
            prefetched_dek_cids: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Set the composite merge hook after construction.
    pub(crate) fn set_composite_merge_hook(&self, hook: Arc<dyn CompositeMergeHook>) {
        let _ = self.composite_merge_hook.set(hook);
    }

    pub(crate) fn composite_merge_hook(&self) -> Option<&Arc<dyn CompositeMergeHook>> {
        self.composite_merge_hook.get()
    }

    /// Set the SE encryption key for generating artifacts on replicated documents.
    pub fn set_se_enc_key(&self, key: Vec<u8>) {
        let _ = self.se_enc_key.set(Zeroizing::new(key));
    }

    /// Get the SE encryption key, if configured.
    pub(crate) fn se_enc_key(&self) -> Option<&[u8]> {
        self.se_enc_key.get().map(|k| k.as_slice())
    }

    /// Set the KMS service. Routes `decrypt_block_data` through the KMS once set.
    pub fn set_kms(&self, kms: Arc<dyn kms::KmsService>) {
        let _ = self.kms.set(kms);
    }

    /// Get the KMS service, if configured.
    pub(crate) fn kms(&self) -> Option<Arc<dyn kms::KmsService>> {
        self.kms.get().cloned()
    }

    /// Get reference to blockstore.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
    }
}

#[cfg(test)]
mod tests;
