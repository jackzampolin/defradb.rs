use p2p::message::QuerySEArtifactsReply;
use p2p::SeQueryCorrelator;

fn reply(message_id: &str, doc_ids: Vec<&str>) -> QuerySEArtifactsReply {
    QuerySEArtifactsReply::success(
        message_id,
        doc_ids.into_iter().map(|s| s.to_string()).collect(),
    )
}

#[tokio::test]
async fn register_then_deliver_routes_reply() {
    let c = SeQueryCorrelator::new();
    let mut pending = c.register("msg-1".to_string());
    assert_eq!(c.in_flight(), 1);

    assert!(c.deliver(reply("msg-1", vec!["bae-a", "bae-b"])));
    assert_eq!(c.in_flight(), 0, "deliver must remove the slot");

    let got = pending.recv().await.expect("reply");
    assert_eq!(got.doc_ids, vec!["bae-a".to_string(), "bae-b".to_string()]);
}

#[tokio::test]
async fn stale_reply_returns_false() {
    let c = SeQueryCorrelator::new();
    assert!(!c.deliver(reply("never-registered", vec![])));
    assert_eq!(c.in_flight(), 0);
}

#[tokio::test]
async fn dropping_guard_cancels_slot() {
    let c = SeQueryCorrelator::new();
    let pending = c.register("msg-2".to_string());
    assert_eq!(c.in_flight(), 1);
    drop(pending);
    assert_eq!(c.in_flight(), 0, "Drop must release the slot");
    // A reply arriving after the requester gave up is stale, not delivered.
    assert!(!c.deliver(reply("msg-2", vec!["x"])));
}

#[tokio::test]
async fn cancel_removes_slot() {
    let c = SeQueryCorrelator::new();
    let _pending = c.register("msg-3".to_string());
    c.cancel("msg-3");
    assert_eq!(c.in_flight(), 0);
}

#[tokio::test]
async fn second_register_overwrites_first() {
    let c = SeQueryCorrelator::new();
    let mut first = c.register("dup".to_string());
    let mut second = c.register("dup".to_string());
    assert_eq!(c.in_flight(), 1);

    assert!(c.deliver(reply("dup", vec!["only"])));
    // The second registration's receiver wins; the first is dropped.
    assert!(first.recv().await.is_err());
    assert_eq!(second.recv().await.expect("reply").doc_ids, vec!["only"]);
}
