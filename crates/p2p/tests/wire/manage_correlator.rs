use p2p::message::{ManageQueryReply, ManageQueryResult, ManageReply};
use p2p::{ManageCorrelator, ManageQueryCorrelator};

// -----------------------------------------------------------------------
// ManageCorrelator tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn register_then_deliver_routes_reply() {
    let c = ManageCorrelator::new();
    let mut pending = c.register("msg-1".into());
    assert_eq!(c.in_flight(), 1);

    assert!(c.deliver(ManageReply::success("msg-1")));
    assert_eq!(c.in_flight(), 0, "deliver must remove the slot");

    let got = pending.recv().await.expect("reply");
    assert_eq!(got.message_id, "msg-1");
}

#[tokio::test]
async fn stale_reply_returns_false() {
    let c = ManageCorrelator::new();
    assert!(!c.deliver(ManageReply::success("never-registered")));
    assert_eq!(c.in_flight(), 0);
}

#[tokio::test]
async fn dropping_guard_cancels_slot() {
    let c = ManageCorrelator::new();
    let pending = c.register("msg-2".into());
    assert_eq!(c.in_flight(), 1);
    drop(pending);
    assert_eq!(c.in_flight(), 0, "Drop must release the slot");
    // A reply arriving after the requester gave up is stale, not delivered.
    assert!(!c.deliver(ManageReply::success("msg-2")));
}

#[tokio::test]
async fn cancel_removes_slot() {
    let c = ManageCorrelator::new();
    let _pending = c.register("msg-3".into());
    c.cancel("msg-3");
    assert_eq!(c.in_flight(), 0);
}

#[tokio::test]
async fn second_register_overwrites_first() {
    let c = ManageCorrelator::new();
    let mut first = c.register("dup".into());
    let mut second = c.register("dup".into());
    assert_eq!(c.in_flight(), 1);

    assert!(c.deliver(ManageReply::success("dup")));
    // The second registration's receiver wins; the first is dropped.
    assert!(first.recv().await.is_err());
    assert_eq!(second.recv().await.expect("reply").message_id, "dup");
}

// -----------------------------------------------------------------------
// ManageQueryCorrelator tests
// -----------------------------------------------------------------------

fn query_reply(message_id: &str) -> ManageQueryReply {
    ManageQueryReply::success(message_id, ManageQueryResult::Strings { values: vec![] })
}

#[tokio::test]
async fn query_register_then_deliver_routes_reply() {
    let c = ManageQueryCorrelator::new();
    let mut pending = c.register("msg-1".into());
    assert_eq!(c.in_flight(), 1);

    assert!(c.deliver(query_reply("msg-1")));
    assert_eq!(c.in_flight(), 0, "deliver must remove the slot");

    let got = pending.recv().await.expect("reply");
    assert_eq!(got.message_id, "msg-1");
}

#[tokio::test]
async fn query_stale_reply_returns_false() {
    let c = ManageQueryCorrelator::new();
    assert!(!c.deliver(query_reply("never-registered")));
    assert_eq!(c.in_flight(), 0);
}

#[tokio::test]
async fn query_dropping_guard_cancels_slot() {
    let c = ManageQueryCorrelator::new();
    let pending = c.register("msg-2".into());
    assert_eq!(c.in_flight(), 1);
    drop(pending);
    assert_eq!(c.in_flight(), 0, "Drop must release the slot");
    assert!(!c.deliver(query_reply("msg-2")));
}

#[tokio::test]
async fn query_cancel_removes_slot() {
    let c = ManageQueryCorrelator::new();
    let _pending = c.register("msg-3".into());
    c.cancel("msg-3");
    assert_eq!(c.in_flight(), 0);
}

#[tokio::test]
async fn query_second_register_overwrites_first() {
    let c = ManageQueryCorrelator::new();
    let mut first = c.register("dup".into());
    let mut second = c.register("dup".into());
    assert_eq!(c.in_flight(), 1);

    assert!(c.deliver(query_reply("dup")));
    // The second registration's receiver wins; the first is dropped.
    assert!(first.recv().await.is_err());
    assert_eq!(second.recv().await.expect("reply").message_id, "dup");
}
