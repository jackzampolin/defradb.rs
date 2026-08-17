//! Protocol-specific event handlers (Identify, PushLog, GossipSub, Bitswap, Kademlia).

use iroh_bitswap::{BitswapEvent, Store};
use libp2p::{gossipsub, kad, kad::store::RecordStore, request_response, PeerId};
use tracing::{debug, error, warn};

use crate::behaviour::{DefraKademliaEvent, KademliaNetwork};
use crate::error::Error;
use crate::message::{PushLogBroadcast, PushLogReply, PushLogRequest};

use super::P2PHost;
use crate::host::event::HostEvent;
use crate::host::ResponseChannel;

impl<S: Store> P2PHost<S> {
    /// Handle identify protocol events.
    pub(super) async fn handle_identify_event(&mut self, event: libp2p::identify::Event) {
        match event {
            libp2p::identify::Event::Received { peer_id, info, .. } => {
                // Update stored address with the peer's first listen address.
                // This corrects the ephemeral send_back_addr for incoming connections.
                if let Some(listen_addr) = info.listen_addrs.first() {
                    self.peer_addrs.insert(peer_id, listen_addr.clone());
                }

                debug!(
                    "Identified peer {}: {} with {} addresses, {} protocols",
                    peer_id,
                    info.agent_version,
                    info.listen_addrs.len(),
                    info.protocols.len()
                );

                // Inform Bitswap about the peer's supported protocols
                // This is critical for Bitswap to know this peer can serve blocks
                let protocols: Vec<String> = info.protocols.iter().map(|p| p.to_string()).collect();
                debug!(
                    peer_id = %peer_id,
                    protocols = ?protocols,
                    "Informing Bitswap of peer protocols"
                );
                self.swarm.behaviour().on_identify(&peer_id, &protocols);

                // Store the peer's listen addresses in Kademlia for routing.
                // Do NOT call add_external_address — those are the REMOTE peer's
                // addresses, not ours. Adding them as local external addresses
                // causes address cross-contamination between peers.
                for addr in &info.listen_addrs {
                    debug!(peer_id = %peer_id, address = %addr, "Adding peer address to Kademlia");
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr.clone());
                }
            }
            libp2p::identify::Event::Sent { peer_id, .. } => {
                debug!("Sent identify info to {}", peer_id);
            }
            libp2p::identify::Event::Pushed { peer_id, .. } => {
                debug!("Pushed identify info to {}", peer_id);
            }
            libp2p::identify::Event::Error { peer_id, error, .. } => {
                warn!("Identify error with {}: {}", peer_id, error);
            }
        }
    }

    /// Handle PushLog request-response events.
    pub(super) async fn handle_pushlog_event(
        &mut self,
        event: request_response::Event<PushLogRequest, PushLogReply>,
    ) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    debug!("Received PushLog request from {}", peer);
                    if self
                        .event_tx
                        .send(HostEvent::PushLogRequest {
                            peer_id: peer,
                            request,
                            channel: ResponseChannel::new(channel),
                        })
                        .await
                        .is_err()
                    {
                        error!(peer_id = %peer, "Failed to send PushLogRequest event - receiver dropped, request will not be processed");
                    }
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    debug!("Received PushLog response for request {:?}", request_id);
                    if let Some(sender) = self.pending_requests.remove(&request_id) {
                        if sender.send(Ok(response)).is_err() {
                            debug!(request_id = ?request_id, "PushLog response dropped - caller cancelled");
                        }
                    }
                }
            },

            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                error!("Outbound request {:?} failed: {:?}", request_id, error);
                if let Some(sender) = self.pending_requests.remove(&request_id) {
                    if sender
                        .send(Err(Error::Transport(format!("{:?}", error))))
                        .is_err()
                    {
                        debug!(request_id = ?request_id, "PushLog error response dropped - caller cancelled");
                    }
                }
            }

            request_response::Event::InboundFailure { peer, error, .. } => {
                warn!("Inbound request from {} failed: {:?}", peer, error);
            }

            request_response::Event::ResponseSent { peer, .. } => {
                debug!("Response sent to {}", peer);
            }
        }
    }

    /// Handle GossipSub events.
    pub(super) async fn handle_gossipsub_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                let topic = message.topic.to_string();
                debug!(
                    "Received gossipsub message {} on topic {} from {}",
                    message_id, topic, propagation_source
                );

                // Topics registered via HostCommand::RegisterPubsubRpcTopic
                // skip the PushLog decoder — their payloads are opaque CBOR
                // handled by pubsub_rpc::TopicHandler in the coordinator
                // (#828). Response sub-topics follow the pattern
                // `<base>/<peer>/_response`; match them by suffix so the
                // coordinator doesn't need to register every response
                // sub-topic explicitly.
                if self.pubsub_rpc_topics.contains(&topic)
                    || (topic.ends_with("/_response")
                        && self
                            .pubsub_rpc_topics
                            .iter()
                            .any(|base| topic.starts_with(&format!("{base}/"))))
                {
                    if self
                        .event_tx
                        .send(HostEvent::GossipRawMessage {
                            propagation_source,
                            message_id: message_id.clone(),
                            topic: topic.clone(),
                            data: message.data,
                        })
                        .await
                        .is_err()
                    {
                        error!(
                            peer_id = %propagation_source,
                            message_id = ?message_id,
                            topic = %topic,
                            "Failed to send GossipRawMessage event - receiver dropped"
                        );
                    }
                    return;
                }

                match PushLogBroadcast::decode_gossip_payload(&message.data) {
                    Ok((mut broadcast, encoding)) => {
                        // libp2p signs the original author into Message::source.
                        // Preserve it independently from the authenticated
                        // propagation hop. Durable recovery may use only that
                        // author, and only when a live route to it exists.
                        if let Some(source) = message.source {
                            let source = source.to_string();
                            broadcast.source_peer_id = Some(source.clone());
                            broadcast.authenticate_origin_peer(source);
                            broadcast.authenticate_source_peer(propagation_source.to_string());
                        }
                        if encoding != crate::message::PushLogGossipPayloadEncoding::CborBroadcast
                            && encoding != crate::message::PushLogGossipPayloadEncoding::CborRequest
                        {
                            debug!(
                                peer_id = %propagation_source,
                                topic = %topic,
                                message_size = message.data.len(),
                                ?encoding,
                                "Decoded libp2p gossip payload via compatibility fallback"
                            );
                        }
                        if self
                            .event_tx
                            .send(HostEvent::GossipMessage {
                                propagation_source,
                                message_id: message_id.clone(),
                                topic: topic.clone(),
                                message: broadcast,
                            })
                            .await
                            .is_err()
                        {
                            error!(
                                peer_id = %propagation_source,
                                message_id = ?message_id,
                                topic = %topic,
                                "Failed to send GossipMessage event - receiver dropped, message will not be processed"
                            );
                        }
                    }
                    Err(e) => {
                        let payload_info =
                            crate::message::PushLogBroadcast::inspect_gossip_payload(&message.data);
                        let sample = crate::sync::GossipDecodeFailureSample {
                            transport: crate::sync::GossipTransport::Libp2p,
                            peer_id: propagation_source.to_string(),
                            topic: topic.clone(),
                            message_size: message.data.len(),
                            error: e.clone(),
                            payload_fingerprint: payload_info.payload_fingerprint,
                            payload_shape_hint: payload_info.payload_shape_hint,
                            occurrences: 0,
                        };
                        // Exponential-backoff sampling: warn on the
                        // 1st, 2nd, 4th, 8th... occurrence; remainder at
                        // debug. Shared process-global counter with the
                        // iroh transport (issue #858).
                        let count =
                            crate::sync::record_gossip_decode_failure_sample(sample.clone());
                        if count == 1 || count.is_power_of_two() {
                            warn!(
                                peer_id = %propagation_source,
                                topic = %topic,
                                message_size = message.data.len(),
                                total_failures = count,
                                error = %e,
                                payload_fingerprint = %sample.payload_fingerprint,
                                payload_shape = %sample.payload_shape_hint,
                                "Failed to decode gossipsub message as PushLogBroadcast or PushLogRequest"
                            );
                        } else {
                            debug!(
                                peer_id = %propagation_source,
                                topic = %topic,
                                message_size = message.data.len(),
                                total_failures = count,
                                error = %e,
                                payload_fingerprint = %sample.payload_fingerprint,
                                payload_shape = %sample.payload_shape_hint,
                                "Failed to decode gossipsub message"
                            );
                        }
                    }
                }
            }

            gossipsub::Event::Subscribed { peer_id, topic } => {
                debug!("Peer {} subscribed to {}", peer_id, topic);
                if self
                    .event_tx
                    .send(HostEvent::PeerSubscribed {
                        peer_id,
                        topic: topic.to_string(),
                    })
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, topic = %topic, "Failed to send PeerSubscribed event - receiver dropped");
                }
            }

            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                debug!("Peer {} unsubscribed from {}", peer_id, topic);
                if self
                    .event_tx
                    .send(HostEvent::PeerUnsubscribed {
                        peer_id,
                        topic: topic.to_string(),
                    })
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, topic = %topic, "Failed to send PeerUnsubscribed event - receiver dropped");
                }
            }

            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                debug!("Peer {} does not support gossipsub", peer_id);
            }

            gossipsub::Event::SlowPeer {
                peer_id,
                failed_messages,
            } => {
                warn!(
                    peer_id = %peer_id,
                    ?failed_messages,
                    "Peer is not consuming gossipsub messages fast enough"
                );
            }
        }
    }

    /// Handle Bitswap events.
    pub(super) async fn handle_bitswap_event(&mut self, event: BitswapEvent) {
        // iroh-bitswap events are for higher-level coordination
        // Block exchange happens transparently through the Client
        match event {
            BitswapEvent::Provide { key } => {
                debug!(cid = %key, "Bitswap requests to provide block");
                // Could integrate with Kademlia DHT to provide this key
            }
            BitswapEvent::FindProviders {
                key,
                response,
                limit,
            } => {
                debug!(cid = %key, limit = limit, "Bitswap requests to find providers");
                // Return all connected peers as potential providers. In a small
                // DefraDB network, any connected peer may have the requested block.
                // This complements session.add_provider() which may not take effect
                // if processed before get_blocks() adds CIDs to the want list.
                let providers: std::collections::HashSet<PeerId> =
                    self.peer_addrs.keys().copied().collect();
                debug!(cid = %key, count = providers.len(), "Returning connected peers as Bitswap providers");
                let _ = response.send(Ok(providers)).await;
            }
            BitswapEvent::Ping { peer, response } => {
                debug!(peer_id = %peer, "Bitswap ping request");
                // Could implement ping latency measurement
                let _ = response.send(None);
            }
        }
    }

    /// Handle Kademlia DHT events.
    pub(super) async fn handle_kademlia_event(&mut self, event: DefraKademliaEvent) {
        let (network, event) = event.split();

        match event {
            kad::Event::RoutingUpdated {
                peer, addresses, ..
            } => {
                debug!(
                    peer_id = %peer,
                    addresses = ?addresses,
                    dht = network.as_str(),
                    "Kademlia routing table updated"
                );
            }

            kad::Event::OutboundQueryProgressed { id, result, .. } => {
                debug!(query_id = ?id, dht = network.as_str(), "Kademlia query progressed: {:?}", result);
            }

            kad::Event::InboundRequest { request } => {
                self.handle_kademlia_inbound_request(network, request);
            }

            kad::Event::RoutablePeer { peer, address } => {
                debug!(
                    peer_id = %peer,
                    address = %address,
                    dht = network.as_str(),
                    "Found routable peer via Kademlia"
                );
            }

            kad::Event::PendingRoutablePeer { peer, address } => {
                debug!(
                    peer_id = %peer,
                    address = %address,
                    dht = network.as_str(),
                    "Found pending routable peer via Kademlia"
                );
            }

            kad::Event::UnroutablePeer { peer } => {
                debug!(peer_id = %peer, dht = network.as_str(), "Peer is unroutable via Kademlia");
            }

            kad::Event::ModeChanged { new_mode } => {
                debug!(mode = ?new_mode, dht = network.as_str(), "Kademlia mode changed");
            }
        }
    }

    fn handle_kademlia_inbound_request(
        &mut self,
        network: KademliaNetwork,
        request: kad::InboundRequest,
    ) {
        match request {
            kad::InboundRequest::PutRecord { source, record, .. } => {
                let Some(record) = record else {
                    debug!(
                        peer_id = %source,
                        dht = network.as_str(),
                        "Kademlia inbound put-record request"
                    );
                    return;
                };

                // Only /pk/ has a Rust-side validator today. Other namespaces
                // are stored as an intentional divergence from Go's full
                // NamespacedValidator registry.
                if let Err(error) = crate::behaviour::validate_pk_namespaced_record(&record) {
                    warn!(
                        peer_id = %source,
                        dht = network.as_str(),
                        error = %error,
                        "Rejected invalid Kademlia pk record"
                    );
                    return;
                }

                let key = record.key.clone();
                match self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut(network)
                    .put(record)
                {
                    Ok(()) => {
                        debug!(
                            peer_id = %source,
                            dht = network.as_str(),
                            record = ?key,
                            "Stored validated Kademlia record"
                        );
                    }
                    Err(error) => {
                        warn!(
                            peer_id = %source,
                            dht = network.as_str(),
                            record = ?key,
                            error = %error,
                            "Failed to store validated Kademlia record"
                        );
                    }
                }
            }
            kad::InboundRequest::AddProvider { record } => {
                let Some(record) = record else {
                    debug!(
                        dht = network.as_str(),
                        "Kademlia inbound add-provider request"
                    );
                    return;
                };

                let key = record.key.clone();
                let provider = record.provider;
                match self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut(network)
                    .add_provider(record)
                {
                    Ok(()) => {
                        debug!(
                            peer_id = %provider,
                            dht = network.as_str(),
                            provider_key = ?key,
                            "Stored Kademlia provider record"
                        );
                    }
                    Err(error) => {
                        warn!(
                            peer_id = %provider,
                            dht = network.as_str(),
                            provider_key = ?key,
                            error = %error,
                            "Failed to store Kademlia provider record"
                        );
                    }
                }
            }
            request => {
                debug!(
                    dht = network.as_str(),
                    "Kademlia inbound request: {:?}", request
                );
            }
        }
    }
}
