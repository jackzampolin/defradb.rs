use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use defra_http::P2PResult;
use p2p::message::DocSyncRequest;

#[cfg(feature = "iroh")]
use p2p::transport::PeerId;

#[cfg(feature = "iroh")]
use crate::transport_doc_pusher::TransportDocPusher;

#[cfg(feature = "libp2p")]
use crate::libp2p_doc_pusher::DocPusher;
#[cfg(feature = "libp2p")]
use p2p::P2PHostHandle;

use super::dispatch::DocSyncDispatch;
use crate::{P2PError, P2PErrorExt as _};

/// Satisfies `TransportDocPusher` (iroh) and `DocPusher` (libp2p) for doc-sync
/// tests. `sync_documents` calls only `validate_collection_exists`; every
/// other method is unreachable from these tests and panics if that
/// assumption ever breaks.
pub(crate) struct StubPusher;

#[cfg(feature = "iroh")]
impl StubPusher {
    pub(crate) fn arc() -> Arc<dyn TransportDocPusher> {
        Arc::new(Self)
    }
}

#[cfg(feature = "libp2p")]
impl StubPusher {
    pub(crate) fn arc_doc_pusher() -> Arc<dyn DocPusher> {
        Arc::new(Self)
    }
}

#[cfg(feature = "iroh")]
#[async_trait]
impl TransportDocPusher for StubPusher {
    async fn push_existing_docs(
        &self,
        _peer_id: &PeerId,
        _collections: &[String],
        _filters: &p2p::ReplicationFilters,
        _se_key: Option<&[u8]>,
    ) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn retry_doc(
        &self,
        _peer_id: &PeerId,
        _doc_id: &str,
        _collection_id: &str,
    ) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn retry_collection_commit(
        &self,
        _peer_id: &PeerId,
        _collection_id: &str,
        _cid: &Cid,
    ) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn load_document_head_blocks(&self, _doc_id: &str) -> P2PResult<Vec<(Cid, Vec<u8>)>> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn load_doc_creator_did(
        &self,
        _collection_name: &str,
        _doc_id: &str,
    ) -> P2PResult<Option<String>> {
        unimplemented!("not reached by doc-sync tests")
    }

    fn get_collection_id(&self, _name: &str) -> Option<String> {
        unimplemented!("not reached by doc-sync tests")
    }

    fn list_collections(&self) -> P2PResult<Vec<String>> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn persist_replicator(&self, _peer_id: &str, _collections: &[String]) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn delete_persisted_replicator(&self, _peer_id: &str) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn persist_p2p_documents(&self, _doc_ids: &[String]) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn load_p2p_documents(&self) -> P2PResult<Vec<String>> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn persist_p2p_collections(&self, _collections: &[String]) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    fn validate_collection_exists(&self, _name: &str) -> P2PResult<()> {
        Ok(())
    }

    fn validate_branchable_collection(&self, _collection_id: &str) -> P2PResult<()> {
        Ok(())
    }
}

#[cfg(feature = "libp2p")]
#[async_trait]
impl DocPusher for StubPusher {
    async fn push_existing_docs(
        &self,
        _handle: &P2PHostHandle,
        _peer_id: libp2p::PeerId,
        _collections: &[String],
        _filters: &p2p::ReplicationFilters,
        _se_key: Option<&[u8]>,
        _se_identity_pubkey: Option<&[u8]>,
    ) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    fn get_collection_id(&self, _name: &str) -> Option<String> {
        unimplemented!("not reached by doc-sync tests")
    }

    fn list_collections(&self) -> P2PResult<Vec<String>> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn persist_replicator(&self, _peer_id: &str, _collections: &[String]) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn delete_persisted_replicator(&self, _peer_id: &str) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn persist_p2p_documents(&self, _doc_ids: &[String]) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn load_p2p_documents(&self) -> P2PResult<Vec<String>> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn persist_p2p_collections(&self, _collections: &[String]) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    fn validate_collection_exists(&self, _name: &str) -> P2PResult<()> {
        Ok(())
    }

    fn validate_branchable_collection(&self, _collection_id: &str) -> P2PResult<()> {
        Ok(())
    }

    async fn retry_doc(
        &self,
        _handle: &P2PHostHandle,
        _peer_id: libp2p::PeerId,
        _doc_id: &str,
        _collection_id: &str,
    ) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn retry_collection_commit(
        &self,
        _handle: &P2PHostHandle,
        _peer_id: libp2p::PeerId,
        _collection_id: &str,
        _cid: &Cid,
    ) -> P2PResult<()> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn load_document_head_blocks(&self, _doc_id: &str) -> P2PResult<Vec<(Cid, Vec<u8>)>> {
        unimplemented!("not reached by doc-sync tests")
    }

    async fn load_doc_creator_did(
        &self,
        _collection_name: &str,
        _doc_id: &str,
    ) -> P2PResult<Option<String>> {
        unimplemented!("not reached by doc-sync tests")
    }
}

/// Reports `peer_count` connected peers and fails every send, so
/// `sync_documents`'s `!any_sent` branch is reachable without a live
/// transport or a real timeout.
pub(crate) struct FailingDispatch {
    peer_count: usize,
    pub(crate) send_attempts: AtomicUsize,
}

impl FailingDispatch {
    pub(crate) fn with_peers(peer_count: usize) -> Self {
        Self {
            peer_count,
            send_attempts: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl DocSyncDispatch for FailingDispatch {
    type Peer = String;

    async fn connected_peers(&self) -> P2PResult<Vec<Self::Peer>> {
        Ok((0..self.peer_count).map(|i| format!("peer-{i}")).collect())
    }

    fn sign_request(&self, _request: &mut DocSyncRequest) -> P2PResult<()> {
        Ok(())
    }

    async fn send_doc_sync_request(
        &self,
        _peer: &Self::Peer,
        _request: DocSyncRequest,
    ) -> P2PResult<()> {
        self.send_attempts.fetch_add(1, Ordering::SeqCst);
        Err(P2PError::transport("send failed"))
    }
}
