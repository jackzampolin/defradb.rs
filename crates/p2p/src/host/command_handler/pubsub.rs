//! PubSub commands: Subscribe, Unsubscribe, Publish, SubscribedTopics.

use iroh_bitswap::Store;
use tracing::debug;

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;
use crate::topics::DefraTopic;

use super::super::p2p_host::P2PHost;

impl<S: Store> P2PHost<S> {
    pub(super) fn handle_subscribe(
        &mut self,
        topic: DefraTopic,
        response: tokio::sync::oneshot::Sender<Result<bool>>,
    ) {
        let ident_topic = topic.to_ident_topic();
        let result = self
            .swarm
            .behaviour_mut()
            .subscribe(&ident_topic)
            .map_err(|e| Error::GossipSubSubscription(e.to_string()));
        if response.send(result).is_err() {
            debug!(topic = ?topic, "Subscribe command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_unsubscribe(
        &mut self,
        topic: DefraTopic,
        response: tokio::sync::oneshot::Sender<Result<bool>>,
    ) {
        let ident_topic = topic.to_ident_topic();
        let result = self
            .swarm
            .behaviour_mut()
            .unsubscribe(&ident_topic)
            .map_err(|e| Error::GossipSubUnsubscribe(e.to_string()));
        if response.send(result).is_err() {
            debug!(topic = ?topic, "Unsubscribe command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_publish(
        &mut self,
        topic: DefraTopic,
        message: PushLogBroadcast,
        response: tokio::sync::oneshot::Sender<Result<libp2p::gossipsub::MessageId>>,
    ) {
        let ident_topic = topic.to_ident_topic();
        let result = serde_cbor::to_vec(&message)
            .map_err(|e| Error::CborSerialization(e.to_string()))
            .and_then(|data| {
                self.swarm
                    .behaviour_mut()
                    .publish(ident_topic, data)
                    .map_err(|e| Error::GossipSubPublish(e.to_string()))
            });
        if response.send(result).is_err() {
            debug!(topic = ?topic, "Publish command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_publish_raw(
        &mut self,
        topic: String,
        data: Vec<u8>,
        response: tokio::sync::oneshot::Sender<Result<libp2p::gossipsub::MessageId>>,
    ) {
        let ident_topic = libp2p::gossipsub::IdentTopic::new(&topic);
        let result = self
            .swarm
            .behaviour_mut()
            .publish(ident_topic, data)
            .map_err(|e| Error::GossipSubPublish(e.to_string()));
        if response.send(result).is_err() {
            debug!(topic = %topic, "PublishRaw command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_subscribed_topics(
        &self,
        response: tokio::sync::oneshot::Sender<Vec<String>>,
    ) {
        let topics: Vec<String> = self
            .swarm
            .behaviour()
            .subscribed_topics()
            .map(|t| t.to_string())
            .collect();
        if response.send(topics).is_err() {
            debug!("SubscribedTopics command response dropped - caller cancelled");
        }
    }
}
