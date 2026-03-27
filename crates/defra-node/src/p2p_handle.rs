//! Concrete P2P handle implementation (type-erased behind P2POps trait).

use std::sync::Arc;

use p2p::P2PTransport;

use crate::CollectionLookup;
use crate::P2POps;

pub(crate) struct P2PHandleImpl<B: blockstore::Blockstore + Send + Sync + 'static> {
    pub(crate) transport: p2p::iroh::IrohTransport,
    pub(crate) coordinator: Arc<p2p::sync::SyncCoordinator<B, p2p::iroh::IrohTransport>>,
    pub(crate) collection_lookup: Arc<dyn CollectionLookup>,
}

#[async_trait::async_trait]
impl<B: blockstore::Blockstore + Send + Sync + 'static> P2POps for P2PHandleImpl<B> {
    async fn local_peer_id(&self) -> String {
        self.transport.local_peer_id().to_string()
    }

    async fn listen_addresses(&self) -> Vec<String> {
        let raw_addrs = self.transport.listen_addresses().await.unwrap_or_default();
        p2p::iroh::format_public_listen_addrs(self.transport.local_peer_id(), &raw_addrs)
    }

    async fn connected_peers(&self) -> anyhow::Result<Vec<String>> {
        self.transport
            .connected_peers()
            .await
            .map(|peers| peers.into_iter().map(|peer| peer.to_string()).collect())
            .map_err(|e| anyhow::anyhow!("connected peers failed: {}", e))
    }

    async fn connect_peer(&self, addr: &str) -> anyhow::Result<()> {
        let (peer_id, addrs) = p2p::iroh::parse_public_peer_addr(addr)?;
        self.transport
            .dial(&peer_id, addrs)
            .await
            .map_err(|e| anyhow::anyhow!("dial failed: {}", e))
    }

    async fn notify_network_change(&self) -> anyhow::Result<()> {
        self.transport
            .network_change()
            .await
            .map_err(|e| anyhow::anyhow!("network_change failed: {}", e))
    }

    async fn subscribe_collection(&self, name: &str) -> anyhow::Result<()> {
        // Resolve collection name -> CID (gossip topics use CIDs, not names)
        let collection_id = self
            .collection_lookup
            .get_collection_id(name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "collection '{}' not found -- add schema before subscribing to P2P",
                    name
                )
            })?;
        tracing::debug!(
            collection_name = %name,
            collection_id = %collection_id,
            "resolved collection name to CID for P2P subscription"
        );
        self.coordinator
            .subscribe_collection(&collection_id)
            .await
            .map_err(|e| anyhow::anyhow!("subscribe collection failed: {}", e))?;
        Ok(())
    }

    async fn set_replicator(
        &self,
        peer_addr: &str,
        collections: Vec<String>,
    ) -> anyhow::Result<()> {
        let (peer_id, addrs) = p2p::iroh::parse_public_peer_addr(peer_addr)?;
        if !addrs.is_empty() || p2p::iroh::is_ticket_string(peer_addr) {
            self.connect_peer(peer_addr).await?;
        }

        // Resolve collection names -> CIDs
        let mut collection_cids = Vec::with_capacity(collections.len());
        for name in &collections {
            let cid = self
                .collection_lookup
                .get_collection_id(name)
                .ok_or_else(|| {
                    anyhow::anyhow!("collection '{}' not found for replicator setup", name)
                })?;
            tracing::debug!(
                collection_name = %name,
                collection_id = %cid,
                "resolved collection name to CID for replicator"
            );
            collection_cids.push(cid);
        }

        self.coordinator
            .create_replicator(&peer_id, collection_cids, true)
            .await
            .map_err(|e| anyhow::anyhow!("set replicator failed: {}", e))?;

        tracing::info!(
            peer_id = %peer_id,
            collections = ?collections,
            "configured live replicator; skipping eager backfill"
        );
        Ok(())
    }
}
