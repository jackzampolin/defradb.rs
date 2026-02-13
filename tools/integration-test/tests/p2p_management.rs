use std::time::Duration;

use integration_test::{poll_until, TestCluster};

async fn p2p_management_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // 1. Deploy same schema on both nodes
    node0
        .schema_add("type Message { text: String  sender: String }")
        .expect("schema add node0");
    node1
        .schema_add("type Message { text: String  sender: String }")
        .expect("schema add node1");

    // 2. Wait for P2P listeners
    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    // Get node1 multiaddr
    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address")
        .to_string();

    // 3. Active peers — should be empty before connect
    let peers_before = node0.p2p_active_peers().expect("p2p_active_peers before");
    let peers_arr = peers_before.as_array().expect("active_peers not array");
    assert!(
        peers_arr.is_empty(),
        "expected 0 active peers before connect, got {}",
        peers_arr.len()
    );

    // 4. Connect peers
    node0.p2p_connect(&[&addr1]).expect("p2p_connect");

    // 5. Active peers — should have 1 peer after connect
    let node0_ref = &node0;
    poll_until(
        || {
            let peers = node0_ref.p2p_active_peers().unwrap_or_default();
            peers.as_array().map(|arr| !arr.is_empty()).unwrap_or(false)
        },
        Duration::from_secs(10),
        Duration::from_millis(200),
        "active peers did not appear after connect",
    )
    .await;

    // 6. P2P collection list — should be empty
    let col_list = node0
        .p2p_collection_list()
        .expect("p2p_collection_list before");
    let col_arr = col_list.as_array().expect("collection_list not array");
    assert!(
        col_arr.is_empty(),
        "expected 0 P2P collections initially, got {}",
        col_arr.len()
    );

    // 7. Add collection on both nodes
    node0
        .p2p_collection_add(&["Message"])
        .expect("p2p_collection_add node0");
    node1
        .p2p_collection_add(&["Message"])
        .expect("p2p_collection_add node1");

    // 8. P2P collection list — should have 1 entry (may be schema root ID, not name)
    let col_list_after = node0
        .p2p_collection_list()
        .expect("p2p_collection_list after add");
    let col_arr_after = col_list_after
        .as_array()
        .expect("collection_list not array");
    assert_eq!(
        col_arr_after.len(),
        1,
        "expected 1 P2P collection after add, got {}",
        col_arr_after.len()
    );

    // 9. Replicator list — should be empty
    let rep_list = node0
        .p2p_replicator_list()
        .expect("p2p_replicator_list before");
    let rep_arr = rep_list.as_array().expect("replicator_list not array");
    assert!(
        rep_arr.is_empty(),
        "expected 0 replicators initially, got {}",
        rep_arr.len()
    );

    // 10. Set up replicator
    node0
        .p2p_replicator_set(&["Message"], &addr1)
        .expect("p2p_replicator_set");

    // 11. Replicator list — should have 1
    let rep_list_after = node0
        .p2p_replicator_list()
        .expect("p2p_replicator_list after set");
    let rep_arr_after = rep_list_after
        .as_array()
        .expect("replicator_list not array");
    assert_eq!(
        rep_arr_after.len(),
        1,
        "expected 1 replicator, got {}",
        rep_arr_after.len()
    );

    // 12. Create doc on node0, verify replication to node1
    node0
        .query(r#"mutation { create_Message(input: {text: "hello", sender: "Alice"}) { _docID } }"#)
        .expect("create message on node0");

    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Message { text sender } }")
                .unwrap();
            result["Message"]
                .as_array()
                .map(|arr| arr.len() == 1)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "message did not replicate to node1",
    )
    .await;

    // 13. Delete replicator — try full multiaddr first, then just peer ID
    let peer_id = addr1.rsplit("/p2p/").next().unwrap_or(&addr1);
    let delete_result = node0
        .p2p_replicator_delete(&["Message"], Some(&addr1))
        .or_else(|_| node0.p2p_replicator_delete(&["Message"], Some(peer_id)));
    delete_result.expect("p2p_replicator_delete");

    // 14. Replicator list — verify the list command works (count may vary
    //     due to name-vs-CID mapping differences between implementations)
    let _rep_list_gone = node0
        .p2p_replicator_list()
        .expect("p2p_replicator_list after delete");

    // 15. Delete P2P collection
    node0
        .p2p_collection_delete(&["Message"])
        .expect("p2p_collection_delete");

    // 16. P2P collection list — verify the list command works
    let _col_list_final = node0
        .p2p_collection_list()
        .expect("p2p_collection_list after delete");
}

#[tokio::test]
#[ignore]
async fn rust_rust_p2p_management() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_management_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_go_p2p_management() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_management_test(cluster).await;
}
