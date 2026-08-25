//! Integration tests for event types and messages

use cid::Cid;
use events::{EventName, MergeCompleteData, Message, PendingDagQuarantinedData, Update};

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
