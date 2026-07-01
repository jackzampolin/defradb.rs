//! Shared serve-boundary read gate primitives for Bitswap and CAR.

use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use defra_core::{is_lens_block, Block as DefraBlock, Signature};

use crate::peer_identity::PeerIdentityResolver;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockAcpMeta {
    pub collection_id: String,
    pub is_branchable: bool,
    pub policy: Option<(String, String)>,
    pub doc_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockClass {
    Allow,
    Data(BlockAcpMeta),
    Deny,
}

#[async_trait]
pub trait BlockClassifier: Send + Sync {
    async fn classify(&self, cid: &Cid, data: &[u8]) -> BlockClass;
}

#[async_trait]
pub trait BlockReadGate: Send + Sync {
    async fn may_read(&self, identity: &acp::Identity, meta: &BlockAcpMeta) -> bool;
}

pub struct ServeAcp {
    pub resolver: Arc<dyn PeerIdentityResolver>,
    pub gate: Arc<dyn BlockReadGate>,
}

#[derive(Default)]
pub struct LateBoundServeAcp(std::sync::OnceLock<ServeAcp>);

impl LateBoundServeAcp {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, serve: ServeAcp) {
        let _ = self.0.set(serve);
    }

    pub fn get(&self) -> Option<&ServeAcp> {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultBlockClassifier;

#[async_trait]
impl BlockClassifier for DefaultBlockClassifier {
    async fn classify(&self, cid: &Cid, data: &[u8]) -> BlockClass {
        match defra_core::block::generate_cid_from_bytes(data) {
            Ok(actual) if &actual == cid => {}
            _ => return BlockClass::Deny,
        }

        if Signature::from_dag_cbor(data).is_ok() {
            return BlockClass::Allow;
        }

        match DefraBlock::from_dag_cbor(data) {
            Ok(block) => {
                if block.delta.is_definition() {
                    return BlockClass::Allow;
                }
                BlockClass::Deny
            }
            Err(_) if is_lens_block(data) => BlockClass::Allow,
            Err(_) => BlockClass::Deny,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllBlockReadGate;

#[async_trait]
impl BlockReadGate for AllowAllBlockReadGate {
    async fn may_read(&self, _identity: &acp::Identity, _meta: &BlockAcpMeta) -> bool {
        true
    }
}
