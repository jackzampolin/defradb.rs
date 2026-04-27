//! Accessor methods for the sync coordinator.

use std::sync::Arc;

use acp::{DocumentACP, ReplicatedDocActorRelationships};
use blockstore::Blockstore;

use super::{SyncCoordinator, SyncShutdownHandle};
use crate::bitswap::ReplicatorRegistry;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::manager::SyncManager;
use crate::sync::peer_state::PeerStateTracker;
use crate::transport::P2PTransport;

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Get the replicator registry.
    pub fn replicators(&self) -> &Arc<ReplicatorRegistry> {
        &self.access.replicators
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        self.manager.blockstore()
    }

    /// Get the broadcaster reference.
    pub fn broadcaster(&self) -> &Broadcaster<T> {
        &self.runtime.broadcaster
    }

    /// Get the local peer ID.
    pub fn local_peer_id(&self) -> &str {
        &self.access.local_peer_id
    }

    /// Get the peer state tracker reference.
    pub fn peer_state(&self) -> &PeerStateTracker {
        &self.access.peer_state
    }

    /// Get the transport reference.
    pub fn transport(&self) -> &T {
        &self.runtime.transport
    }

    /// Get the shutdown handle for coordinator-owned background tasks.
    pub fn background_shutdown_handle(&self) -> SyncShutdownHandle {
        self.runtime.shutdown.clone()
    }

    /// Get the sync manager reference.
    pub fn manager(&self) -> &SyncManager<B> {
        &self.manager
    }

    /// Wire document ACP into the coordinator for local ACP relationship replay.
    pub fn set_document_acp(&self, acp: Arc<dyn DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }

    pub(crate) async fn apply_replicated_actor_relationships(
        &self,
        doc_id: &str,
        creator: Option<&str>,
        snapshot: Option<&ReplicatedDocActorRelationships>,
    ) -> crate::Result<()> {
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let Some(acp) = self.document_acp.get() else {
            return Ok(());
        };

        if let Some(creator) = creator {
            let creator_did = identity::Did::new(creator).map_err(|error| {
                crate::Error::Behaviour(format!(
                    "failed to parse replicated ACP creator DID '{creator}': {error}"
                ))
            })?;
            let is_registered = acp
                .is_doc_registered(&snapshot.policy_id, &snapshot.resource_name, doc_id)
                .await
                .map_err(|error| {
                    crate::Error::Behaviour(format!(
                        "failed to check replicated ACP registration: {error}"
                    ))
                })?;

            if !is_registered {
                acp.register_doc_object(
                    &creator_did,
                    &snapshot.policy_id,
                    &snapshot.resource_name,
                    doc_id,
                )
                .await
                .map_err(|error| {
                    crate::Error::Behaviour(format!(
                        "failed to register replicated ACP document owner: {error}"
                    ))
                })?;
            }
        }

        acp.replace_actor_relationships(
            &snapshot.policy_id,
            &snapshot.resource_name,
            doc_id,
            &snapshot.relationships,
        )
        .await
        .map_err(|error| {
            crate::Error::Behaviour(format!(
                "failed to apply replicated ACP relationships: {error}"
            ))
        })
    }
}
