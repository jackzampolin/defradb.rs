#![cfg(feature = "libp2p-transport")]

use std::str::FromStr;

use libp2p::{
    gossipsub::{ConfigBuilder, IdentTopic, Message},
    PeerId,
};
use p2p::behaviour::go_compatible_gossipsub_message_id;

struct GoMessageIdFixture {
    source_peer_id: &'static str,
    seqno: u64,
    expected_message_id_hex: &'static str,
}

// Fixtures generated with github.com/libp2p/go-libp2p-pubsub v0.15.0,
// the version pulled in by github.com/sourcenetwork/go-p2p v0.1.9.
// Go's DefaultMsgIdFn returns raw `from` bytes followed by raw `seqno` bytes.
const GO_MESSAGE_ID_FIXTURES: &[GoMessageIdFixture] = &[
    GoMessageIdFixture {
        source_peer_id: "12D3KooWRthjSZqhYSw9stPs7gyDZNCQj56DPrexwu81x4pBPWrf",
        seqno: 0,
        expected_message_id_hex:
            "002408011220eed76f5297f40aa13424a7d137b0ece2767af93cb1e7ab1d6a1822246c3e9d040000000000000000",
    },
    GoMessageIdFixture {
        source_peer_id: "12D3KooWRVrtCQVw1cS9yL6be2LubujAwLfWBj8ezdAmaVsfG3Mk",
        seqno: 1,
        expected_message_id_hex:
            "002408011220e8fd6c5b8d0f7af079c17f7283e71980b9e7489adb8f25a6a37dc47d3df138730000000000000001",
    },
    GoMessageIdFixture {
        source_peer_id: "12D3KooWJE6tgHjS38qKvYxJyxj6v4wiL3qm5zpGfZXRue94AXjt",
        seqno: 42,
        expected_message_id_hex:
            "0024080112207cf2293b8c0ab992f698ad6b9dc4e3d623c707ff5e7c9c96517facc1ff88a0c7000000000000002a",
    },
    GoMessageIdFixture {
        source_peer_id: "12D3KooWQcTgQo6AqZZi93a6RmjSSyiaM6Pox3UwQcGQZLxoc93t",
        seqno: 72_623_859_790_382_856,
        expected_message_id_hex:
            "002408011220dbd290e8c6bb7efe5adcf2405dbc014e19c3ce73dc15dbf82e95436cee47433f0102030405060708",
    },
    GoMessageIdFixture {
        source_peer_id: "12D3KooWGvPsoXFvBELWCpmJfNLj2iP6CSQCtNhYfbm6h7gbeLD4",
        seqno: u64::MAX,
        expected_message_id_hex:
            "002408011220698d39770c1d14d255b15d8b59183aab280159446a7345c0a0ceed86ffc45a2fffffffffffffffff",
    },
];

#[test]
fn go_compatible_gossipsub_message_id_matches_go_pubsub_fixtures() {
    let config = ConfigBuilder::default()
        .message_id_fn(go_compatible_gossipsub_message_id)
        .build()
        .unwrap();
    let topic = IdentTopic::new("defra/go-message-id-parity").hash();

    for fixture in GO_MESSAGE_ID_FIXTURES {
        let source = PeerId::from_str(fixture.source_peer_id).unwrap();
        let message = Message {
            source: Some(source),
            data: b"message-id is independent of payload".to_vec(),
            sequence_number: Some(fixture.seqno),
            topic: topic.clone(),
        };

        let actual_message_id_hex = hex::encode(config.message_id(&message).0);

        assert_eq!(
            actual_message_id_hex, fixture.expected_message_id_hex,
            "Rust gossipsub MessageId drifted from go-libp2p-pubsub \
             DefaultMsgIdFn for source {} seqno {}",
            fixture.source_peer_id, fixture.seqno
        );
    }
}
