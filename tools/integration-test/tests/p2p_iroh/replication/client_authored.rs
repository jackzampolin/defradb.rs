//! A commit fragment a client signed, pushed to one node, reaching a peer.
//!
//! `POST /api/v0/sync` is how a device that holds its own key writes: it
//! builds the commit blocks, signs them with a key the node never sees, and
//! the node validates and merges rather than authors. What the node then owes
//! the rest of the network is the subject here.
//!
//! Merging is not enough on its own: the node that took the push is the only
//! one that holds the blocks, so unless it announces what it merged, no
//! replicator and no gossip topic ever hears about the document.
//!
//! These pin both halves of that: the document travels, and the signature
//! travels with it. The second is what makes the first safe -- a peer
//! registers the owner the *signature* proves, not whichever node happened to
//! hand it over.

use integration_test::{setup_two_node_replicated, wait_for_doc_count};
use serde_json::json;
use serial_test::serial;

use crate::client_authored_common::{actor, build, reading, Node, SCHEMA};

/// Two iroh nodes with `Reading` on both, connected, replicator 0 → 1: a
/// gateway taking device pushes, and a peer that should learn of them.
async fn replicated_pair() -> (integration_test::TestCluster, Node, Node) {
    let (cluster, _addr1) = setup_two_node_replicated(SCHEMA, &["Reading"]).await;
    let gateway = Node::attach(cluster.api_url(0).to_string()).await;
    let peer = Node::attach(cluster.api_url(1).to_string()).await;
    (cluster, gateway, peer)
}

#[tokio::test]
#[serial]
async fn a_pushed_fragment_reaches_a_peer() {
    let (cluster, gateway, peer) = replicated_pair().await;
    let author = actor(0x11);

    let fragment = build(
        &reading(1, 2137),
        &gateway.version_id,
        Some(&author.key),
        false,
    );
    gateway.push_ok(&fragment, None).await;

    // The peer was never pushed to and never asked. The only thing that can
    // put this document on it is the node that merged the fragment announcing
    // what it merged.
    wait_for_doc_count(&cluster.client(1), "Reading", 1).await;

    let rows = peer
        .graphql("query { Reading { _docID device seq centicelsius } }", None)
        .await["Reading"]
        .clone();
    let rows = rows.as_array().expect("rows");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_docID"], json!(fragment.doc_id));
    assert_eq!(rows[0]["device"], json!("sensor-7"));
    assert_eq!(rows[0]["seq"], json!(1));
    assert_eq!(rows[0]["centicelsius"], json!(2137));
}

#[tokio::test]
#[serial]
async fn the_signature_survives_the_hop() {
    // The point of the whole path: the peer holds the client's signature, not
    // one belonging to the node that carried it. A hop that cannot alter what
    // it carries and cannot claim it is what makes an intermediary safe --
    // whether it is a relay in front of the nodes or a node in the middle.
    let (cluster, gateway, peer) = replicated_pair().await;
    let author = actor(0x11);

    let fragment = build(
        &reading(1, 2137),
        &gateway.version_id,
        Some(&author.key),
        false,
    );
    gateway.push_ok(&fragment, None).await;
    wait_for_doc_count(&cluster.client(1), "Reading", 1).await;

    assert!(
        peer.commit_signers(&fragment.doc_id)
            .await
            .contains(&author.public_key_hex),
        "the peer must hold the key the client signed with, not one from the node that relayed it"
    );
}
