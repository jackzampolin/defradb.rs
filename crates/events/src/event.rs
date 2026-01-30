//! Event types for the DefraDB event bus.

use cid::Cid;

/// Event names that can be subscribed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventName {
    /// Subscribe to all events.
    WildCard,
    /// Document update event (create, update, delete).
    Update,
    /// P2P merge started event.
    Merge,
    /// P2P merge completed event.
    MergeComplete,
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
        }
    }
}

/// P2P merge complete event data.
#[derive(Debug, Clone)]
pub struct MergeCompleteData {
    /// Document ID that was merged.
    pub doc_id: String,
    /// CID of the merged block.
    pub cid: Cid,
    /// Collection ID the document belongs to.
    pub collection_id: String,
    /// Peer ID that sent this block.
    pub by_peer: String,
}

/// Document update event data.
#[derive(Debug, Clone)]
pub struct Update {
    /// Document ID that was updated.
    pub doc_id: String,
    /// CID of the update block.
    pub cid: Cid,
    /// Collection ID (schema version ID) the document belongs to.
    pub collection_id: String,
    /// Serialized block data.
    pub block: Vec<u8>,
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
        block: Vec<u8>,
        is_retry: bool,
        is_relay: bool,
    ) -> Self {
        Self {
            doc_id,
            cid,
            collection_id,
            block,
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
pub enum MessageData {
    /// No data (for simple signals).
    None,
    /// Document update data.
    Update(Update),
    /// P2P merge complete data.
    MergeComplete(MergeCompleteData),
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
            cid: Cid::default(),
            collection_id: "col-abc".to_string(),
            by_peer: "peer-xyz".to_string(),
        };
        let mc_msg = Message::merge_complete(mc_data);
        assert_eq!(mc_msg.name, EventName::MergeComplete);
        assert!(mc_msg.as_merge_complete().is_some());
        assert_eq!(mc_msg.as_merge_complete().unwrap().doc_id, "doc-789");
    }
}
