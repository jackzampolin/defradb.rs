//! Dispatcher for [`TransportEvent::GossipRawMessage`] that routes the KMS
//! encryption topic to the KMS transport and pubsub_rpc gossipsub payloads to
//! the appropriate `TopicHandler`.

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
    pub(super) async fn handle_gossip_raw_message(
        &self,
        propagation_source: PeerId,
        topic: String,
        data: Vec<u8>,
    ) -> Result<()> {
        // KMS short-circuit — backend-agnostic. Wire compat with Go requires
        // bare CBOR on the encryption topic (no InternalResponse envelope), so
        // route to the KMS transport's dispatcher before the pubsub_rpc path.
        if topic == crate::topics::ENCRYPTION_TOPIC {
            if let Some(transport) = self.kms_transport.get() {
                transport
                    .dispatch_incoming(
                        kms::PeerIdentity {
                            peer_id: propagation_source.to_string(),
                        },
                        data,
                    )
                    .await;
            } else {
                debug!(
                    topic = %topic,
                    "KMS message arrived but no KMS transport installed; dropping"
                );
            }
            return Ok(());
        }

        // pubsub_rpc path (libp2p only).
        #[cfg(feature = "libp2p-transport")]
        {
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
        {
            let _ = (propagation_source, data);
            debug!(
                topic = %topic,
                "GossipRawMessage for non-KMS topic without libp2p transport; dropping"
            );
            Ok(())
        }
    }
}
