use std::collections::HashMap;
use std::time::Duration;

use integration_test::{for_each_p2p_topology, poll_until, TestCluster};

async fn replication_crud_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Wait for P2P listeners
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
    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Deploy schema on both nodes
    node0
        .schema_add("type User { name: String  age: Int }")
        .unwrap();
    node1
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    // Connect and set up replication
    node0.p2p_connect(&[addr1]).unwrap();
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // --- Step 1: Create 10 documents rapidly on node0 ---
    // Creating many documents at once increases the chance that multiple
    // sync events land in the same batch window (try_recv draining).
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

    let mut doc_ids: HashMap<String, String> = HashMap::new();
    for (name, age) in &users {
        let result = node0
            .query(&format!(
                r#"mutation {{ add_User(input: {{name: "{}", age: {}}}) {{ _docID name age }} }}"#,
                name, age
            ))
            .unwrap();
        let doc_id = result["add_User"][0]["_docID"]
            .as_str()
            .unwrap_or_else(|| panic!("missing _docID for {}", name))
            .to_string();
        doc_ids.insert(name.to_string(), doc_id);
    }

    // --- Step 2: Wait for all 10 to replicate, verify exact field values ---
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name age } }")
                .unwrap();
            let arr = match result["User"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() != 10 {
                return false;
            }
            // Verify every user has correct name and age
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
        Duration::from_millis(200),
        "10 docs with correct field values did not replicate",
    )
    .await;

    // --- Step 3: Batch update — change ages for Alice, Dave, and Grace ---
    // Multiple rapid updates also exercise the batch merge path.
    let updates = [("Alice", 31), ("Dave", 29), ("Grace", 34)];
    for (name, new_age) in &updates {
        let doc_id = &doc_ids[*name];
        node0
            .query(&format!(
                r#"mutation {{ update_User(docID: "{}", input: {{age: {}}}) {{ _docID name age }} }}"#,
                doc_id, new_age
            ))
            .unwrap();
    }

    // --- Step 4: Wait for all 3 updates to replicate with exact values ---
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name age } }")
                .unwrap();
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
            // Check updated values
            found.get("Alice") == Some(&31)
                && found.get("Dave") == Some(&29)
                && found.get("Grace") == Some(&34)
                // Check unchanged values are still correct
                && found.get("Bob") == Some(&25)
                && found.get("Carol") == Some(&35)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "batch updates did not replicate with correct values",
    )
    .await;

    // --- Step 5: Query with filter on node1 (ages > 30) ---
    let filtered = node1
        .query(r#"query { User(filter: {age: {_gt: 30}}) { name age } }"#)
        .unwrap();
    let filtered_users = filtered["User"]
        .as_array()
        .expect("filtered result not array");
    // Alice=31, Carol=35, Grace=34, Frank=40, Iris=31 → 5 users
    assert_eq!(
        filtered_users.len(),
        5,
        "expected 5 users with age > 30, got {:?}",
        filtered_users
    );
    let names: Vec<&str> = filtered_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    for expected in &["Alice", "Carol", "Grace", "Frank", "Iris"] {
        assert!(
            names.contains(expected),
            "{} missing from age>30 filter, got {:?}",
            expected,
            names
        );
    }

    // --- Step 6: Delete Bob and Eve on node0 ---
    node0.collection_delete("User", &doc_ids["Bob"]).unwrap();
    node0.collection_delete("User", &doc_ids["Eve"]).unwrap();

    // --- Step 7: Wait for deletions to replicate, verify remaining 8 ---
    poll_until(
        || {
            let result = node1_ref.query("query { User { name age } }").unwrap();
            let arr = match result["User"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() != 8 {
                return false;
            }
            let names: Vec<&str> = arr.iter().filter_map(|u| u["name"].as_str()).collect();
            !names.contains(&"Bob")
                && !names.contains(&"Eve")
                && names.contains(&"Alice")
                && names.contains(&"Carol")
                && names.contains(&"Dave")
                && names.contains(&"Frank")
                && names.contains(&"Grace")
                && names.contains(&"Hank")
                && names.contains(&"Iris")
                && names.contains(&"Jack")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "deletions did not replicate correctly",
    )
    .await;

    // Final verification: all remaining field values are correct
    let final_result = node1.query("query { User { name age } }").unwrap();
    let final_users = final_result["User"].as_array().unwrap();
    let mut final_map: HashMap<String, i64> = HashMap::new();
    for u in final_users {
        if let (Some(name), Some(age)) = (u["name"].as_str(), u["age"].as_i64()) {
            final_map.insert(name.to_string(), age);
        }
    }
    // Updated values
    assert_eq!(final_map["Alice"], 31);
    assert_eq!(final_map["Dave"], 29);
    assert_eq!(final_map["Grace"], 34);
    // Unchanged values
    assert_eq!(final_map["Carol"], 35);
    assert_eq!(final_map["Frank"], 40);
    assert_eq!(final_map["Hank"], 27);
    assert_eq!(final_map["Iris"], 31);
    assert_eq!(final_map["Jack"], 29);
}

for_each_p2p_topology!(replication_crud, replication_crud_test, .with_p2p());
