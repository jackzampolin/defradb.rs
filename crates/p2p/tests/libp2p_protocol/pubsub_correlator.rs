use cid::Cid;
use libp2p::identity::Keypair;
use p2p::pubsub_rpc::{derive_request_id, Correlator, InternalResponse, PublishOptions};

fn a_peer() -> String {
    libp2p::PeerId::from_public_key(&Keypair::generate_ed25519().public()).to_string()
}

fn internal_for(id: &Cid, data: &[u8], err: &str) -> InternalResponse {
    InternalResponse {
        id: id.to_string(),
        err: err.to_string(),
        data: data.to_vec(),
        from: Vec::new(),
    }
}

#[tokio::test]
async fn single_response_delivers_and_removes() {
    let c = Correlator::new();
    let mut prep = c.publish(b"req".to_vec(), PublishOptions::default());
    assert_eq!(c.in_flight(), 1);

    let from = a_peer();
    let delivered = c.deliver(from.clone(), internal_for(&prep.id, b"resp", ""));
    assert!(delivered);
    assert_eq!(c.in_flight(), 0, "single-response entry must auto-remove");

    let r = prep.responses.recv().await.expect("response");
    assert_eq!(r.id, prep.id);
    assert_eq!(r.from, from);
    assert_eq!(r.data, b"resp");
    assert!(r.err.is_none());
}

#[tokio::test]
async fn multi_response_keeps_entry_open() {
    let c = Correlator::new();
    let mut prep = c.publish(
        b"req".to_vec(),
        PublishOptions {
            multi_response: true,
            ..Default::default()
        },
    );

    let p1 = a_peer();
    let p2 = a_peer();
    assert!(c.deliver(p1.clone(), internal_for(&prep.id, b"r1", "")));
    assert!(c.deliver(p2.clone(), internal_for(&prep.id, b"r2", "boom")));
    assert_eq!(
        c.in_flight(),
        1,
        "multi-response entry stays until handle dropped"
    );

    let r1 = prep.responses.recv().await.expect("first response");
    let r2 = prep.responses.recv().await.expect("second response");
    assert_eq!(r1.from, p1);
    assert_eq!(r2.from, p2);
    assert_eq!(r1.err, None);
    assert_eq!(r2.err.as_deref(), Some("boom"));

    drop(prep);
    assert_eq!(c.in_flight(), 0, "dropping handle releases multi entry");
}

#[test]
fn publish_fire_and_forget_allocates_no_entry() {
    let c = Correlator::new();
    let id = c.publish_fire_and_forget(b"req");
    assert_eq!(c.in_flight(), 0);
    assert_eq!(id, derive_request_id(b"req"));
}

#[tokio::test]
async fn dropping_handle_cancels_in_flight() {
    let c = Correlator::new();
    let prep = c.publish(b"req".to_vec(), PublishOptions::default());
    assert_eq!(c.in_flight(), 1);
    drop(prep);
    assert_eq!(c.in_flight(), 0, "Drop must auto-cancel");
}

#[tokio::test]
async fn cancel_all_wakes_waiting_publishers() {
    let c = Correlator::new();
    let mut first = c.publish(b"first".to_vec(), PublishOptions::default());
    let mut second = c.publish(b"second".to_vec(), PublishOptions::default());
    assert_eq!(c.in_flight(), 2);

    assert_eq!(c.cancel_all(), 2);
    assert_eq!(c.in_flight(), 0);

    assert!(first.responses.recv().await.is_none());
    assert!(second.responses.recv().await.is_none());
}

#[test]
fn multi_response_full_buffer_reports_drop_without_panic() {
    let c = Correlator::new();
    let _prep = c.publish(
        b"req".to_vec(),
        PublishOptions {
            multi_response: true,
            multi_response_buffer: 1,
        },
    );
    let id = derive_request_id(b"req");
    let p = a_peer();
    // First send fills the 1-slot buffer.
    assert!(c.deliver(p.clone(), internal_for(&id, b"a", "")));
    // Second send should report false (backpressure drop) but keep the entry.
    assert!(!c.deliver(p, internal_for(&id, b"b", "")));
    assert_eq!(
        c.in_flight(),
        1,
        "full channel must not remove multi-response entry"
    );
}

#[test]
fn late_response_is_ignored() {
    let c = Correlator::new();
    let id = derive_request_id(b"never-sent");
    let delivered = c.deliver(a_peer(), internal_for(&id, b"", ""));
    assert!(
        !delivered,
        "response with no matching ongoing request drops"
    );
}

#[test]
fn malformed_cid_is_dropped() {
    let c = Correlator::new();
    let delivered = c.deliver(
        a_peer(),
        InternalResponse {
            id: "not-a-cid".to_string(),
            err: String::new(),
            data: Vec::new(),
            from: Vec::new(),
        },
    );
    assert!(!delivered);
}
