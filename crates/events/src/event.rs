//! Event types for the DefraDB event bus.

use bytes::Bytes;
use cid::Cid;
use defra_core::ActionExecution;

/// Event names that can be subscribed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EventName {
    /// Subscribe to all events.
    WildCard,
    /// Document update event (create, update, delete).
    Update,
    /// P2P merge started event.
    Merge,
    /// P2P merge completed event.
    MergeComplete,
    /// Replicator configuration completed (initial docs pushed).
    ReplicatorCompleted,
    /// GossipSub peer joined/left a topic.
    TopicPeerEvent,
    /// SE artifact received after merge (encrypted index document).
    SEArtifactReceived,
    /// ACP light client advanced to a new finalized header.
    AcpHeightAdvanced,
    /// ACP light client invalidated cached entries after a root change.
    AcpCacheInvalidated,
    /// A pending-DAG root was quarantined after a deterministic merge
    /// rejection (#1128) — the operator-facing forensics signal.
    PendingDagQuarantined,
    /// A long-running database action changed state.
    ActionExecution,
}

impl EventName {
    /// Check if this event name matches another (considering wildcards).
    pub fn matches(&self, other: &EventName) -> bool {
        match (self, other) {
            (EventName::WildCard, _) | (_, EventName::WildCard) => true,
            _ => self == other,
        }
    }
}

impl std::fmt::Display for EventName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventName::WildCard => write!(f, "*"),
            EventName::Update => write!(f, "update"),
            EventName::Merge => write!(f, "merge"),
            EventName::MergeComplete => write!(f, "merge-complete"),
            EventName::ReplicatorCompleted => write!(f, "replicator-completed"),
            EventName::TopicPeerEvent => write!(f, "topic-peer-event"),
            EventName::SEArtifactReceived => write!(f, "se-artifact-received"),
            EventName::AcpHeightAdvanced => write!(f, "acp-height-advanced"),
            EventName::AcpCacheInvalidated => write!(f, "acp-cache-invalidated"),
            EventName::PendingDagQuarantined => write!(f, "pending-dag-quarantined"),
            EventName::ActionExecution => write!(f, "action-execution"),
        }
    }
}

/// P2P merge complete event data.
#[derive(Debug, Clone)]
pub struct MergeCompleteData {
    /// Document ID that was merged.
    pub doc_id: String,
    /// Document ID used for authorization when this is a collection-level event.
    pub subject_doc_id: Option<String>,
    /// CID of the merged block.
    pub cid: Cid,
    /// Collection ID the document belongs to.
    pub collection_id: String,
    /// Peer ID that sent this block.
    pub by_peer: String,
}

/// GossipSub topic peer event data.
#[derive(Debug, Clone)]
pub struct TopicPeerEventData {
    /// The peer ID that joined or left.
    pub peer_id: String,
    /// The topic name.
    pub topic: String,
    /// "JOINED" or "LEFT".
    pub event_type: String,
}

/// SE artifact received event data.
#[derive(Debug, Clone)]
pub struct SEArtifactReceivedData {
    /// Document ID the artifact is for.
    pub doc_id: String,
}

/// Pending-DAG quarantine event data (#1128).
///
/// Emitted when a pending-DAG root is quarantined after a deterministic
/// merge rejection: the block is left unmerged and will not be re-driven
/// locally. This is distinct from `MergeComplete` — the document did NOT
/// merge — so fleet alerting can distinguish "caught up" from "stuck
/// forever on bad content" without misreading a quarantine as a success.
#[derive(Debug, Clone)]
pub struct PendingDagQuarantinedData {
    /// CID of the quarantined root block.
    pub cid: Cid,
    /// Document ID, if known.
    pub doc_id: String,
    /// Collection ID, if known.
    pub collection_id: String,
    /// Human-readable rejection reason (e.g. unique-index violation).
    pub reason: String,
}

/// ACP light client height advancement event data.
#[derive(Debug, Clone)]
pub struct AcpHeightAdvancedData {
    /// Latest finalized height observed by the ACP light client.
    pub height: u64,
    /// Latest finalized ACP module state root.
    pub module_state_root: String,
}

/// ACP light client cache invalidation event data.
#[derive(Debug, Clone)]
pub struct AcpCacheInvalidatedData {
    /// Height at which the new module state root was observed.
    pub height: u64,
    /// New ACP module state root.
    pub module_state_root: String,
    /// Previous ACP module state root.
    pub previous_root: String,
    /// Number of cache entries invalidated for the old root.
    pub entries_invalidated: usize,
}

/// Document update event data.
#[derive(Debug, Clone)]
pub struct Update {
    /// Document ID that was updated.
    pub doc_id: String,
    /// Document ID used for authorization when this is a collection-level event.
    pub subject_doc_id: Option<String>,
    /// CID of the update block.
    pub cid: Cid,
    /// Collection ID (schema version ID) the document belongs to.
    pub collection_id: String,
    /// Serialized block data.
    pub block: Bytes,
    /// Whether this is a retry of a previously failed operation.
    pub is_retry: bool,
    /// Whether this update came from P2P relay (vs local mutation).
    pub is_relay: bool,
}

impl Update {
    /// Create a new Update event.
    pub fn new(
        doc_id: String,
        cid: Cid,
        collection_id: String,
        block: impl Into<Bytes>,
        is_retry: bool,
        is_relay: bool,
    ) -> Self {
        Self {
            doc_id,
            subject_doc_id: None,
            cid,
            collection_id,
            block: block.into(),
            is_retry,
            is_relay,
        }
    }

    /// Create a new Update event with an internal authorization subject document ID.
    pub fn new_with_subject_doc_id(
        doc_id: String,
        subject_doc_id: String,
        cid: Cid,
        collection_id: String,
        block: impl Into<Bytes>,
        is_retry: bool,
        is_relay: bool,
    ) -> Self {
        Self {
            doc_id,
            subject_doc_id: Some(subject_doc_id),
            cid,
            collection_id,
            block: block.into(),
            is_retry,
            is_relay,
        }
    }
}

/// Message wrapper for events.
#[derive(Debug, Clone)]
pub struct Message {
    /// The event name/type.
    pub name: EventName,
    /// The event data (if any).
    pub data: MessageData,
}

/// Event data variants.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MessageData {
    /// No data (for simple signals).
    None,
    /// Document update data.
    Update(Update),
    /// P2P merge complete data.
    MergeComplete(MergeCompleteData),
    /// Replicator completed signal (initial docs pushed).
    ReplicatorCompleted,
    /// GossipSub topic peer event.
    TopicPeerEvent(TopicPeerEventData),
    /// SE artifact received after merge.
    SEArtifactReceived(SEArtifactReceivedData),
    /// ACP light client finalized height advanced.
    AcpHeightAdvanced(AcpHeightAdvancedData),
    /// ACP light client cache invalidated after a root change.
    AcpCacheInvalidated(AcpCacheInvalidatedData),
    /// Pending-DAG root quarantined after a deterministic merge rejection.
    PendingDagQuarantined(PendingDagQuarantinedData),
    /// Long-running database action changed state.
    ActionExecution(ActionExecution),
}

impl Message {
    /// Create a new Update message.
    pub fn update(update: Update) -> Self {
        Self {
            name: EventName::Update,
            data: MessageData::Update(update),
        }
    }

    /// Create a new Merge message (signal only, no data).
    pub fn merge() -> Self {
        Self {
            name: EventName::Merge,
            data: MessageData::None,
        }
    }

    /// Create a new MergeComplete message with data.
    pub fn merge_complete(data: MergeCompleteData) -> Self {
        Self {
            name: EventName::MergeComplete,
            data: MessageData::MergeComplete(data),
        }
    }

    /// Create a new ReplicatorCompleted message (signal).
    pub fn replicator_completed() -> Self {
        Self {
            name: EventName::ReplicatorCompleted,
            data: MessageData::ReplicatorCompleted,
        }
    }

    /// Create a new TopicPeerEvent message.
    pub fn topic_peer_event(data: TopicPeerEventData) -> Self {
        Self {
            name: EventName::TopicPeerEvent,
            data: MessageData::TopicPeerEvent(data),
        }
    }

    /// Create a new SEArtifactReceived message.
    pub fn se_artifact_received(data: SEArtifactReceivedData) -> Self {
        Self {
            name: EventName::SEArtifactReceived,
            data: MessageData::SEArtifactReceived(data),
        }
    }

    /// Create a new ACP height advanced message.
    pub fn acp_height_advanced(data: AcpHeightAdvancedData) -> Self {
        Self {
            name: EventName::AcpHeightAdvanced,
            data: MessageData::AcpHeightAdvanced(data),
        }
    }

    /// Create a new ACP cache invalidated message.
    pub fn acp_cache_invalidated(data: AcpCacheInvalidatedData) -> Self {
        Self {
            name: EventName::AcpCacheInvalidated,
            data: MessageData::AcpCacheInvalidated(data),
        }
    }

    /// Create a new PendingDagQuarantined message.
    pub fn pending_dag_quarantined(data: PendingDagQuarantinedData) -> Self {
        Self {
            name: EventName::PendingDagQuarantined,
            data: MessageData::PendingDagQuarantined(data),
        }
    }

    /// Create a new ActionExecution message.
    pub fn action_execution(data: ActionExecution) -> Self {
        Self {
            name: EventName::ActionExecution,
            data: MessageData::ActionExecution(data),
        }
    }

    /// Get the SEArtifactReceivedData if this is an SEArtifactReceived message.
    pub fn as_se_artifact_received(&self) -> Option<&SEArtifactReceivedData> {
        match &self.data {
            MessageData::SEArtifactReceived(d) => Some(d),
            _ => None,
        }
    }

    /// Get the TopicPeerEventData if this is a TopicPeerEvent message.
    pub fn as_topic_peer_event(&self) -> Option<&TopicPeerEventData> {
        match &self.data {
            MessageData::TopicPeerEvent(d) => Some(d),
            _ => None,
        }
    }

    /// Get the Update data if this is an Update message.
    pub fn as_update(&self) -> Option<&Update> {
        match &self.data {
            MessageData::Update(u) => Some(u),
            _ => None,
        }
    }

    /// Get the MergeCompleteData if this is a MergeComplete message.
    pub fn as_merge_complete(&self) -> Option<&MergeCompleteData> {
        match &self.data {
            MessageData::MergeComplete(d) => Some(d),
            _ => None,
        }
    }

    /// Get the AcpHeightAdvancedData if this is an ACP height advanced message.
    pub fn as_acp_height_advanced(&self) -> Option<&AcpHeightAdvancedData> {
        match &self.data {
            MessageData::AcpHeightAdvanced(d) => Some(d),
            _ => None,
        }
    }

    /// Get the AcpCacheInvalidatedData if this is an ACP cache invalidated message.
    pub fn as_acp_cache_invalidated(&self) -> Option<&AcpCacheInvalidatedData> {
        match &self.data {
            MessageData::AcpCacheInvalidated(d) => Some(d),
            _ => None,
        }
    }

    /// Get the PendingDagQuarantinedData if this is a PendingDagQuarantined message.
    pub fn as_pending_dag_quarantined(&self) -> Option<&PendingDagQuarantinedData> {
        match &self.data {
            MessageData::PendingDagQuarantined(d) => Some(d),
            _ => None,
        }
    }

    /// Get the ActionExecution data if this is an ActionExecution message.
    pub fn as_action_execution(&self) -> Option<&ActionExecution> {
        match &self.data {
            MessageData::ActionExecution(data) => Some(data),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_name_matches() {
        assert!(EventName::WildCard.matches(&EventName::Update));
        assert!(EventName::Update.matches(&EventName::WildCard));
        assert!(EventName::Update.matches(&EventName::Update));
        assert!(!EventName::Update.matches(&EventName::Merge));
    }

    #[test]
    fn test_message_creation() {
        let cid = Cid::default();
        let update = Update::new(
            "doc-123".to_string(),
            cid,
            "col-456".to_string(),
            vec![1, 2, 3],
            false,
            false,
        );

        let msg = Message::update(update);
        assert_eq!(msg.name, EventName::Update);
        assert!(msg.as_update().is_some());

        let merge_msg = Message::merge();
        assert_eq!(merge_msg.name, EventName::Merge);
        assert!(merge_msg.as_update().is_none());

        let mc_data = MergeCompleteData {
            doc_id: "doc-789".to_string(),
            subject_doc_id: None,
            cid: Cid::default(),
            collection_id: "col-abc".to_string(),
            by_peer: "peer-xyz".to_string(),
        };
        let mc_msg = Message::merge_complete(mc_data);
        assert_eq!(mc_msg.name, EventName::MergeComplete);
        assert!(mc_msg.as_merge_complete().is_some());
        assert_eq!(mc_msg.as_merge_complete().unwrap().doc_id, "doc-789");
    }

    #[test]
    fn test_pending_dag_quarantined_message() {
        let data = PendingDagQuarantinedData {
            cid: Cid::default(),
            doc_id: "doc-1".to_string(),
            collection_id: "col-1".to_string(),
            reason: "unique constraint violation".to_string(),
        };

        let msg = Message::pending_dag_quarantined(data);
        assert_eq!(msg.name, EventName::PendingDagQuarantined);
        assert!(msg.as_merge_complete().is_none());
        let quarantined = msg
            .as_pending_dag_quarantined()
            .expect("message should carry PendingDagQuarantinedData");
        assert_eq!(quarantined.doc_id, "doc-1");
        assert_eq!(quarantined.collection_id, "col-1");
        assert_eq!(quarantined.reason, "unique constraint violation");
    }
}
