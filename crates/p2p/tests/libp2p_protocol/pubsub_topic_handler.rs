use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use cid::Cid;
use libp2p::{identity::Keypair, PeerId};
use p2p::pubsub_rpc::{
    derive_request_id, response_topic, DeliveryOutcome, FnHandler, InternalResponse,
    MessageHandler, PublishOptions, PubsubResponse, TopicHandler,
};
use tokio::sync::mpsc;

fn a_peer() -> PeerId {
    PeerId::from_public_key(&Keypair::generate_ed25519().public())
}

struct CountingEcho {
    calls: AtomicUsize,
}
#[async_trait]
impl MessageHandler for CountingEcho {
    async fn handle(&self, _from: PeerId, data: Vec<u8>) -> Result<Vec<u8>, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(data)
    }
}

#[tokio::test]
async fn round_trip_request_reply() {
    let alice = a_peer();
    let bob = a_peer();

    let echo = Arc::new(CountingEcho {
        calls: AtomicUsize::new(0),
    });
    let bob_topic = TopicHandler::new("doc-sync", bob, echo.clone());
    let alice_topic = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Err::<Vec<u8>, String>("alice is a pure caller".into())
        })),
    );

    let mut prep = alice_topic.prepare_publish(b"hello".to_vec(), PublishOptions::default());
    assert_eq!(alice_topic.in_flight(), 1);

    let outcome = bob_topic
        .deliver_gossip_message("doc-sync", alice, prep.data.clone())
        .await;
    assert_eq!(echo.calls.load(Ordering::SeqCst), 1);
    let response = match outcome {
        DeliveryOutcome::Respond(r) => r,
        other => panic!("expected Respond, got {other:?}"),
    };
    assert_eq!(
        response.topic,
        response_topic("doc-sync", &alice),
        "bob must reply on alice's response sub-topic"
    );

    let ret = alice_topic
        .deliver_gossip_message(alice_topic.self_response_topic(), bob, response.bytes)
        .await;
    assert!(matches!(ret, DeliveryOutcome::Forwarded));

    let got = prep.responses.recv().await.expect("response arrives");
    assert_eq!(got.from, bob.to_string());
    assert_eq!(got.data, b"hello");
    assert_eq!(got.id, prep.id);
    assert!(got.err.is_none());
    assert_eq!(alice_topic.in_flight(), 0, "single-response auto-closes");
}

#[tokio::test]
async fn echoed_own_request_is_ignored() {
    let alice = a_peer();
    let t = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            panic!("handler must not run for own echoed request");
        })),
    );
    let outcome = t
        .deliver_gossip_message("doc-sync", alice, b"x".to_vec())
        .await;
    assert!(matches!(outcome, DeliveryOutcome::Ignored));
}

#[tokio::test]
async fn response_for_other_peer_is_ignored() {
    let alice = a_peer();
    let bob = a_peer();
    let t = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Ok::<Vec<u8>, String>(Vec::new())
        })),
    );
    let not_my_topic = response_topic("doc-sync", &bob);
    let outcome = t
        .deliver_gossip_message(&not_my_topic, bob, b"x".to_vec())
        .await;
    assert!(matches!(outcome, DeliveryOutcome::Ignored));
}

#[tokio::test]
async fn handler_error_propagates_as_err_string() {
    let alice = a_peer();
    let bob = a_peer();

    let bob_topic = TopicHandler::new(
        "doc-sync",
        bob,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Err::<Vec<u8>, String>("unknown doc".into())
        })),
    );
    let alice_topic = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Ok::<Vec<u8>, String>(Vec::new())
        })),
    );

    let mut prep = alice_topic.prepare_publish(b"req".to_vec(), PublishOptions::default());
    let resp = match bob_topic
        .deliver_gossip_message("doc-sync", alice, prep.data.clone())
        .await
    {
        DeliveryOutcome::Respond(r) => r,
        other => panic!("expected Respond, got {other:?}"),
    };
    alice_topic
        .deliver_gossip_message(alice_topic.self_response_topic(), bob, resp.bytes)
        .await;

    let got = prep.responses.recv().await.expect("response");
    assert_eq!(got.err.as_deref(), Some("unknown doc"));
    assert!(got.data.is_empty());
}

#[tokio::test]
async fn out_of_band_topic_is_ignored() {
    let alice = a_peer();
    let t = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            panic!("handler must not run for unrelated topic");
        })),
    );
    let outcome = t
        .deliver_gossip_message("some-other-topic", a_peer(), b"x".to_vec())
        .await;
    assert!(matches!(outcome, DeliveryOutcome::Ignored));
}

#[tokio::test]
async fn multi_response_collects_until_dropped() {
    let alice = a_peer();
    let bob = a_peer();
    let carol = a_peer();

    let alice_topic = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Ok::<Vec<u8>, String>(Vec::new())
        })),
    );

    let mut prep = alice_topic.prepare_publish(
        b"multi".to_vec(),
        PublishOptions {
            multi_response: true,
            ..Default::default()
        },
    );
    let make_envelope = |id: &Cid, data: &[u8]| {
        let env = InternalResponse {
            id: id.to_string(),
            err: String::new(),
            data: data.to_vec(),
            from: Vec::new(),
        };
        env.to_cbor().expect("encode")
    };
    let rt = alice_topic.self_response_topic().to_string();
    assert!(matches!(
        alice_topic
            .deliver_gossip_message(&rt, bob, make_envelope(&prep.id, b"from-bob"))
            .await,
        DeliveryOutcome::Forwarded
    ));
    assert!(matches!(
        alice_topic
            .deliver_gossip_message(&rt, carol, make_envelope(&prep.id, b"from-carol"))
            .await,
        DeliveryOutcome::Forwarded
    ));

    let rx: &mut mpsc::Receiver<PubsubResponse> = &mut prep.responses;
    let first = rx.recv().await.expect("first");
    let second = rx.recv().await.expect("second");
    assert_eq!(
        {
            let s: std::collections::HashSet<String> =
                [first.from, second.from].into_iter().collect();
            s.len()
        },
        2
    );
    assert_eq!(
        alice_topic.in_flight(),
        1,
        "multi-response stays open until dropped"
    );

    drop(prep);
    assert_eq!(alice_topic.in_flight(), 0);
}

#[tokio::test]
async fn late_response_is_dropped() {
    let alice = a_peer();
    let t = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Ok::<Vec<u8>, String>(Vec::new())
        })),
    );
    let envelope = InternalResponse {
        id: derive_request_id(b"never").to_string(),
        err: String::new(),
        data: Vec::new(),
        from: Vec::new(),
    };
    let outcome = t
        .deliver_gossip_message(
            t.self_response_topic(),
            a_peer(),
            envelope.to_cbor().unwrap(),
        )
        .await;
    assert!(matches!(outcome, DeliveryOutcome::Ignored));
}

#[tokio::test]
async fn malformed_response_envelope_is_ignored() {
    let alice = a_peer();
    let t = TopicHandler::new(
        "doc-sync",
        alice,
        Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
            Ok::<Vec<u8>, String>(Vec::new())
        })),
    );
    let outcome = t
        .deliver_gossip_message(t.self_response_topic(), a_peer(), vec![0xff, 0xff, 0xff])
        .await;
    assert!(matches!(outcome, DeliveryOutcome::Ignored));
}
