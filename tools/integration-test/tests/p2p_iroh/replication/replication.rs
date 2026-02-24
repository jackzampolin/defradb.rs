//! Iroh P2P advanced replication tests.
//!
//! Tests batch document creation, updates, deletes, and filtered queries
//! over iroh transport replication between two nodes.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_replication -- --ignored

use std::collections::HashMap;
use std::time::Duration;

use integration_test::{poll_until, TestCluster};
use serial_test::serial;

/// Set up a 2-node iroh cluster with User schema and replication.
/// Returns (cluster, node1_addr).
async fn setup_replicated_cluster() -> TestCluster {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
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

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address")
        .to_string();

    node0
        .schema_add("type User { name: String  age: Int }")
        .unwrap();
    node1
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    node0.p2p_connect(&[&addr1]).unwrap();
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], &addr1).unwrap();

    cluster
}

/// Batch create 10 documents, verify all replicate with correct field values.
#[tokio::test]
#[serial]
async fn iroh_batch_replication() {
    let cluster = setup_replicated_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let users = [
        ("Alice", 30),
        ("Bob", 25),
        ("Carol", 35),
        ("Dave", 28),
        ("Eve", 22),
        ("Frank", 40),
        ("Grace", 33),
        ("Hank", 27),
        ("Iris", 31),
        ("Jack", 29),
    ];

    for (name, age) in &users {
        node0
            .query(&format!(
                r#"mutation {{ create_User(input: {{name: "{}", age: {}}}) {{ _docID }} }}"#,
                name, age
            ))
            .unwrap();
    }

    // Verify all 10 docs exist on node0 first
    let local_result = node0
        .query("query { User { name age } }")
        .expect("local query on node0");
    let local_users = local_result["User"]
        .as_array()
        .expect("node0 result not array");
    assert_eq!(
        local_users.len(),
        10,
        "node0 should have all 10 docs locally"
    );

    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name age } }")
                .unwrap_or_default();
            let arr = match result["User"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() != 10 {
                return false;
            }
            let mut found: HashMap<String, i64> = HashMap::new();
            for u in arr {
                if let (Some(name), Some(age)) = (u["name"].as_str(), u["age"].as_i64()) {
                    found.insert(name.to_string(), age);
                }
            }
            users
                .iter()
                .all(|(name, age)| found.get(*name) == Some(&(*age as i64)))
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "10 docs with correct field values did not replicate",
    )
    .await;
}

/// Update documents on node0, verify updates replicate to node1.
#[tokio::test]
#[serial]
async fn iroh_update_replication() {
    let cluster = setup_replicated_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create 3 documents
    let mut doc_ids: HashMap<String, String> = HashMap::new();
    for (name, age) in &[("Alice", 30), ("Bob", 25), ("Carol", 35)] {
        let result = node0
            .query(&format!(
                r#"mutation {{ create_User(input: {{name: "{}", age: {}}}) {{ _docID }} }}"#,
                name, age
            ))
            .unwrap();
        let doc_id = result["create_User"][0]["_docID"]
            .as_str()
            .expect("missing _docID")
            .to_string();
        doc_ids.insert(name.to_string(), doc_id);
    }

    // Wait for initial replication
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name age } }")
                .unwrap_or_default();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 3)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial 3 docs did not replicate",
    )
    .await;

    // Update Alice and Carol
    let alice_update = node0
        .query(&format!(
            r#"mutation {{ update_User(docID: "{}", input: {{age: 31}}) {{ _docID name age }} }}"#,
            doc_ids["Alice"]
        ))
        .expect("update Alice");
    assert!(
        alice_update["update_User"].is_array() || alice_update["update_User"].is_object(),
        "Alice update should return result"
    );

    let carol_update = node0
        .query(&format!(
            r#"mutation {{ update_User(docID: "{}", input: {{age: 36}}) {{ _docID name age }} }}"#,
            doc_ids["Carol"]
        ))
        .expect("update Carol");
    assert!(
        carol_update["update_User"].is_array() || carol_update["update_User"].is_object(),
        "Carol update should return result"
    );

    // Verify updates applied locally on node0
    let local_updated = node0
        .query("query { User { name age } }")
        .expect("local updated query");
    let local_arr = local_updated["User"]
        .as_array()
        .expect("local result not array");
    let mut local_ages: HashMap<String, i64> = HashMap::new();
    for u in local_arr {
        if let (Some(name), Some(age)) = (u["name"].as_str(), u["age"].as_i64()) {
            local_ages.insert(name.to_string(), age);
        }
    }
    assert_eq!(local_ages.get("Alice"), Some(&31), "Alice age on node0");
    assert_eq!(local_ages.get("Bob"), Some(&25), "Bob age on node0");
    assert_eq!(local_ages.get("Carol"), Some(&36), "Carol age on node0");

    // Verify updates replicate
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { name age } }")
                .unwrap_or_default();
            let arr = match result["User"].as_array() {
                Some(a) => a,
                None => return false,
            };
            let mut found: HashMap<String, i64> = HashMap::new();
            for u in arr {
                if let (Some(name), Some(age)) = (u["name"].as_str(), u["age"].as_i64()) {
                    found.insert(name.to_string(), age);
                }
            }
            found.get("Alice") == Some(&31)
                && found.get("Bob") == Some(&25)
                && found.get("Carol") == Some(&36)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "updates did not replicate with correct values",
    )
    .await;
}

/// Delete documents on node0, verify deletions replicate to node1.
#[tokio::test]
#[serial]
async fn iroh_delete_replication() {
    let cluster = setup_replicated_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create 3 documents
    let mut doc_ids: HashMap<String, String> = HashMap::new();
    for (name, age) in &[("Alice", 30), ("Bob", 25), ("Carol", 35)] {
        let result = node0
            .query(&format!(
                r#"mutation {{ create_User(input: {{name: "{}", age: {}}}) {{ _docID }} }}"#,
                name, age
            ))
            .unwrap();
        let doc_id = result["create_User"][0]["_docID"]
            .as_str()
            .expect("missing _docID")
            .to_string();
        doc_ids.insert(name.to_string(), doc_id);
    }

    // Wait for initial replication
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name } }")
                .unwrap_or_default();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 3)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial 3 docs did not replicate",
    )
    .await;

    // Delete Bob
    node0
        .collection_delete("User", &doc_ids["Bob"])
        .expect("delete Bob");

    // Verify deletion applied locally on node0
    let local_after_delete = node0
        .query("query { User { name } }")
        .expect("local query after delete");
    let local_names: Vec<&str> = local_after_delete["User"]
        .as_array()
        .expect("local result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(
        local_names.len(),
        2,
        "node0 should have 2 docs after delete"
    );
    assert!(
        !local_names.contains(&"Bob"),
        "Bob should be deleted on node0"
    );
    assert!(
        local_names.contains(&"Alice"),
        "Alice should remain on node0"
    );
    assert!(
        local_names.contains(&"Carol"),
        "Carol should remain on node0"
    );

    // Verify deletion replicates
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { name } }")
                .unwrap_or_default();
            let arr = match result["User"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() != 2 {
                return false;
            }
            let names: Vec<&str> = arr.iter().filter_map(|u| u["name"].as_str()).collect();
            !names.contains(&"Bob") && names.contains(&"Alice") && names.contains(&"Carol")
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "deletion did not replicate correctly",
    )
    .await;
}

/// Filter query on replicated data works correctly.
#[tokio::test]
#[serial]
async fn iroh_replicated_filter_query() {
    let cluster = setup_replicated_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create documents with varying ages
    for (name, age) in &[("Alice", 30), ("Bob", 25), ("Carol", 35), ("Dave", 40)] {
        node0
            .query(&format!(
                r#"mutation {{ create_User(input: {{name: "{}", age: {}}}) {{ _docID }} }}"#,
                name, age
            ))
            .unwrap();
    }

    // Wait for all 4 to replicate with correct field values
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { name age } }")
                .unwrap_or_default();
            let arr = match result["User"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() != 4 {
                return false;
            }
            let mut found: HashMap<String, i64> = HashMap::new();
            for u in arr {
                if let (Some(name), Some(age)) = (u["name"].as_str(), u["age"].as_i64()) {
                    found.insert(name.to_string(), age);
                }
            }
            found.get("Alice") == Some(&30)
                && found.get("Bob") == Some(&25)
                && found.get("Carol") == Some(&35)
                && found.get("Dave") == Some(&40)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "4 docs with correct values did not replicate",
    )
    .await;

    // Filter query on node1: ages > 30
    let filtered = node1
        .query(r#"query { User(filter: {age: {_gt: 30}}) { name age } }"#)
        .unwrap();
    let filtered_users = filtered["User"]
        .as_array()
        .expect("filtered result not array");
    assert_eq!(
        filtered_users.len(),
        2,
        "expected 2 users with age > 30 (Carol=35, Dave=40), got {:?}",
        filtered_users
    );
    let names: Vec<&str> = filtered_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(
        names.contains(&"Carol"),
        "Carol (35) should be in age>30 filter"
    );
    assert!(
        names.contains(&"Dave"),
        "Dave (40) should be in age>30 filter"
    );

    // Boundary: Alice (age=30) should NOT be in age > 30 results
    assert!(
        !names.contains(&"Alice"),
        "Alice (30) should NOT be in age>30 filter (boundary case)"
    );
    assert!(
        !names.contains(&"Bob"),
        "Bob (25) should NOT be in age>30 filter"
    );
}
