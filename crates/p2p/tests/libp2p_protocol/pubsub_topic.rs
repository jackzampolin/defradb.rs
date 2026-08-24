use libp2p::{identity::Keypair, PeerId};
use p2p::pubsub_rpc::{response_topic, strip_response_topic};

fn a_peer() -> PeerId {
    PeerId::from_public_key(&Keypair::generate_ed25519().public())
}

#[test]
fn topic_shape_matches_go_join() {
    let p = a_peer();
    let t = response_topic("doc-sync", &p);
    let expected = format!("doc-sync/{p}/_response");
    assert_eq!(t, expected);
}

#[test]
fn strip_response_recovers_base_for_self() {
    let me = a_peer();
    let t = response_topic("sync-branchable", &me);
    assert_eq!(strip_response_topic(&t, &me), Some("sync-branchable"));
}

#[test]
fn strip_response_rejects_other_peer() {
    let me = a_peer();
    let other = a_peer();
    let t = response_topic("doc-sync", &other);
    // `_response` topics addressed to other peers must not decode for us.
    assert_eq!(strip_response_topic(&t, &me), None);
}

#[test]
fn strip_response_rejects_plain_topic() {
    let me = a_peer();
    assert_eq!(strip_response_topic("doc-sync", &me), None);
}
