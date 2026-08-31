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
        // KMS short-circuit, on any transport that emits raw gossip. Go layers
        // the go-libp2p-pubsub-rpc protocol on the `encryption` topic: bare-CBOR
        // requests on `encryption`, and reply envelopes on the per-peer
        // `encryption/<caller>/_response` sub-topic. Both must reach the KMS
        // transport's dispatcher (not the doc-sync/branchable pubsub_rpc path),
        // so match the base topic and its `_response` sub-topics here.
        #[cfg(feature = "kms")]
        {
            let is_kms_base = topic == crate::topics::ENCRYPTION_TOPIC;
            let is_kms_response = topic
                .starts_with(&format!("{}/", crate::topics::ENCRYPTION_TOPIC))
                && topic.ends_with("/_response");
            if is_kms_base || is_kms_response {
                if let Some(transport) = self.kms_transport.get() {
                    transport
                        .dispatch_incoming(propagation_source.to_string(), topic, data)
                        .await;
                } else {
                    debug!(
                        topic = %topic,
                        "KMS message arrived but no KMS transport installed; dropping"
                    );
                }
                return Ok(());
            }
        }

        // pubsub_rpc path (libp2p only).
        #[cfg(feature = "libp2p-transport")]
        {
            let Some(services) = self.pubsub_services.as_ref() else {
                // Services are started only by the libp2p runtime; a raw
                // message on a pubsub_rpc topic that arrives before they
                // exist has no owner. Log and drop for defensive hygiene.
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
