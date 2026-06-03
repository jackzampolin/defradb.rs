//! End-to-end tests for the authenticated P2P management-relay channel over
//! libp2p.
//!
//! A caller hits node A's HTTP `/api/v0/p2p/manage` (and `/p2p/manage/query`) to
//! make node A relay a signed P2P management request to node B. The caller mints
//! a JWT (`aud` = B's peer-id), node A relays it, and node B authorizes the actor
//! against its NAC engine before applying the op.
//!
//! Shared helpers (token mint, raw `reqwest` POST to the custom endpoints) live in
//! `manage_relay_common` and are also used by the iroh mirror.

use std::time::{Duration, Instant};

use integration_test::{generate_identity, TestCluster};
use reqwest::StatusCode;
use serde_json::{json, Value};
use serial_test::serial;

use crate::manage_relay_common::{peer_id_of, post_manage, post_manage_query};

const SCHEMA: &str = "type User { name: String  age: Int }";

/// Bring up a NAC-enabled 2-node libp2p cluster, deploy the `User` schema on
/// node B, and return the admin key plus node B's peer-id and dial address.
async fn setup() -> (TestCluster, String, String, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_acp_local()
        .with_nac()
        .with_p2p()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    // Node B (index 1) is the managed peer. Its schema must exist so CollectionAdd
    // has a real collection to subscribe.
    let node_b = cluster.client(1);
    node_b
        .schema_add_with_identity(SCHEMA, &admin_key)
        .expect("deploy schema on node B");

    let info_b = node_b
        .p2p_info_with_identity(&admin_key)
        .expect("p2p_info node B");
    let addr_b = info_b
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node B has no P2P address")
        .to_string();
    let peer_id_b = peer_id_of(&addr_b).to_string();

    (cluster, admin_key, addr_b, peer_id_b)
}

/// Admin token (`aud` = B peer-id) relayed via node A subscribes node B to the
/// `User` collection. Proven by querying B's subscriptions over the same relay.
#[tokio::test]
#[serial]
async fn manage_collection_add_over_p2p_authorized() {
    let (cluster, admin_key, addr_b, peer_id_b) = setup().await;
    let api_a = cluster.api_url(0).to_string();

    let token = crate::manage_relay_common::mint_manage_token(&admin_key, &peer_id_b);

    let status = post_manage(
        &api_a,
        &admin_key,
        &addr_b,
        &token,
        json!({ "Kind": "CollectionAdd", "collection_ids": ["User"] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "authorized CollectionAdd relay should return 200"
    );

    // Prove the round-trip: B is now subscribed. Query over the same relay.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = post_manage_query(
            &api_a,
            &admin_key,
            &addr_b,
            &token,
            json!({ "Kind": "CollectionList" }),
        )
        .await;
        if status == StatusCode::OK && collection_list_nonempty(&body) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node B did not report the subscribed collection over the relay"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Cross-check directly on node B.
    let node_b = cluster.client(1);
    let direct = node_b
        .p2p_collection_list_with_identity(&admin_key)
        .expect("node B p2p_collection_list");
    assert_eq!(
        direct.as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "node B should have exactly one subscribed collection"
    );
}

/// An outsider token (`aud` = B peer-id, signed by a non-admin key) is rejected
/// by node B's NAC and surfaces as 403; node B must not subscribe.
#[tokio::test]
#[serial]
async fn manage_denied_for_unauthorized_actor() {
    let (cluster, admin_key, addr_b, peer_id_b) = setup().await;
    let api_a = cluster.api_url(0).to_string();

    let node_a = cluster.client(0);
    let outsider = generate_identity(node_a.binary_path()).expect("outsider identity");

    // HTTP caller is still admin (passes node A's P2pPeerConnect gate); the relayed
    // actor token belongs to the outsider, so node B's NAC denies it.
    let outsider_token =
        crate::manage_relay_common::mint_manage_token(&outsider.private_key_hex, &peer_id_b);

    let status = post_manage(
        &api_a,
        &admin_key,
        &addr_b,
        &outsider_token,
        json!({ "Kind": "CollectionAdd", "collection_ids": ["User"] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "unauthorized actor should be denied with 403"
    );

    // Node B must not have subscribed.
    let node_b = cluster.client(1);
    let direct = node_b
        .p2p_collection_list_with_identity(&admin_key)
        .expect("node B p2p_collection_list");
    assert!(
        direct.as_array().map(|a| a.is_empty()).unwrap_or(true),
        "node B must not subscribe to any collection after a denied relay"
    );
}

/// Pure read round-trip: admin CollectionList query over the relay returns a typed
/// `Strings` result.
#[tokio::test]
#[serial]
async fn manage_query_collection_list_over_p2p() {
    let (cluster, admin_key, addr_b, peer_id_b) = setup().await;
    let api_a = cluster.api_url(0).to_string();

    let token = crate::manage_relay_common::mint_manage_token(&admin_key, &peer_id_b);

    let (status, body) = post_manage_query(
        &api_a,
        &admin_key,
        &addr_b,
        &token,
        json!({ "Kind": "CollectionList" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "CollectionList query should be 200");
    assert_eq!(
        body["Kind"], "Strings",
        "CollectionList must return a typed Strings result, got {body}"
    );
    assert!(
        body["values"].is_array(),
        "Strings result must carry a values array, got {body}"
    );
}

/// True when a `RemoteManageQueryResult::Strings` body carries at least one value.
fn collection_list_nonempty(body: &Value) -> bool {
    body["Kind"] == "Strings"
        && body["values"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false)
}
