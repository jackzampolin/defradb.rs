//! Dispatcher for [`TransportEvent::GossipRawMessage`] that routes
//! pubsub_rpc gossipsub payloads to the appropriate `TopicHandler`.

use blockstore::Blockstore;
use tracing::debug;
#[cfg(feature = "libp2p-transport")]
use tracing::warn;

use super::super::SyncCoordinator;
use crate::error::Result;
#[cfg(feature = "libp2p-transport")]
use crate::pubsub_rpc::DeliveryOutcome;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    #[cfg(feature = "libp2p-transport")]
    pub(super) async fn handle_gossip_raw_message(
        &self,
        propagation_source: PeerId,
        topic: String,
        data: Vec<u8>,
    ) -> Result<()> {
        let Some(services) = self.pubsub_services.as_ref() else {
            // Services are only present on libp2p; the iroh transport never
            // emits GossipRawMessage so this branch should be unreachable
            // outside of tests. Log and drop for defensive hygiene.
            debug!(
                topic = %topic,
                "GossipRawMessage received but pubsub services not started; dropping"
            );
            return Ok(());
        };

        let Some(handler) = services.handler_for_topic(&topic) else {
            debug!(topic = %topic, "GossipRawMessage topic not owned by any TopicHandler");
            return Ok(());
        };

        let from_libp2p: libp2p::PeerId = match propagation_source.as_str().parse() {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    peer_id = %propagation_source,
                    topic = %topic,
                    error = %e,
                    "GossipRawMessage: source peer id does not parse as libp2p::PeerId"
                );
                return Ok(());
            }
        };

        let outcome = handler
            .deliver_gossip_message(&topic, from_libp2p, data)
            .await;

        match outcome {
            DeliveryOutcome::Forwarded => Ok(()),
            DeliveryOutcome::Ignored => Ok(()),
            DeliveryOutcome::Respond(response) => {
                // Publish the encoded InternalResponse envelope back on the
                // caller's <base>/<caller>/_response sub-topic.
                if let Err(e) = self
                    .runtime
                    .transport
                    .publish_raw(response.topic.clone(), response.bytes)
                    .await
                {
                    warn!(
                        topic = %response.topic,
                        error = %e,
                        "pubsub_rpc: failed to publish response envelope"
                    );
                }
                Ok(())
            }
        }
    }

    #[cfg(not(feature = "libp2p-transport"))]
    pub(super) async fn handle_gossip_raw_message(
        &self,
        _propagation_source: PeerId,
        topic: String,
        _data: Vec<u8>,
    ) -> Result<()> {
        debug!(
            topic = %topic,
            "GossipRawMessage received without libp2p transport support; dropping"
        );
        Ok(())
    }
}
