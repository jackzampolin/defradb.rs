use integration_test::{for_each_runtime, TestCluster};

async fn transactions_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema
    client
        .schema_add("type Account { name: String  balance: Int }")
        .expect("failed to add schema");

    // 2. Create baseline doc outside any transaction
    let data = client
        .query(r#"mutation { add_Account(input: {name: "Alice", balance: 100}) { _docID name balance } }"#)
        .expect("create Alice");
    let alice_id = data["add_Account"][0]["_docID"]
        .as_str()
        .expect("missing Alice _docID")
        .to_string();

    // -- Commit flow --

    // 3a. Create transaction
    let tx_id = client.tx_create().expect("tx_create failed");
    assert!(!tx_id.is_empty(), "tx_create should return a non-empty ID");

    // 3b. Create Bob inside the transaction
    client
        .query_with_tx(
            r#"mutation { add_Account(input: {name: "Bob", balance: 200}) { _docID name balance } }"#,
            &tx_id,
        )
        .expect("create Bob in tx");

    // 3c. Query outside tx — only Alice visible
    let outside = client
        .query("query { Account { name balance } }")
        .expect("query outside tx");
    let outside_accounts = outside["Account"]
        .as_array()
        .expect("outside result not array");
    assert_eq!(
        outside_accounts.len(),
        1,
        "expected 1 account outside tx, got {}",
        outside_accounts.len()
    );
    assert_eq!(outside_accounts[0]["name"], "Alice");

    // 3d. Query inside tx — Alice + Bob visible
    let inside = client
        .query_with_tx("query { Account { name balance } }", &tx_id)
        .expect("query inside tx");
    let inside_accounts = inside["Account"]
        .as_array()
        .expect("inside result not array");
    assert_eq!(
        inside_accounts.len(),
        2,
        "expected 2 accounts inside tx, got {}",
        inside_accounts.len()
    );
    let names: Vec<&str> = inside_accounts
        .iter()
        .filter_map(|a| a["name"].as_str())
        .collect();
    assert!(names.contains(&"Alice"), "Alice missing inside tx");
    assert!(names.contains(&"Bob"), "Bob missing inside tx");

    // 3e. Commit transaction
    client.tx_commit(&tx_id).expect("tx_commit failed");

    // 3f. Query outside tx — both visible now
    let after_commit = client
        .query("query { Account { name balance } }")
        .expect("query after commit");
    let committed = after_commit["Account"]
        .as_array()
        .expect("committed result not array");
    assert_eq!(
        committed.len(),
        2,
        "expected 2 accounts after commit, got {}",
        committed.len()
    );

    // -- Discard flow --

    // 4a. Create second transaction
    let tx_id_2 = client.tx_create().expect("tx_create 2 failed");

    // 4b. Update Alice balance to 999 inside tx
    client
        .query_with_tx(
            &format!(
                r#"mutation {{ update_Account(docID: "{}", input: {{balance: 999}}) {{ _docID balance }} }}"#,
                alice_id
            ),
            &tx_id_2,
        )
        .expect("update Alice in tx");

    // 4c. Discard transaction
    client.tx_discard(&tx_id_2).expect("tx_discard failed");

    // 4d. Verify Alice still has balance=100
    let after_discard = client
        .query("query { Account { name balance } }")
        .expect("query after discard");
    let accounts = after_discard["Account"]
        .as_array()
        .expect("after_discard result not array");
    let alice = accounts
        .iter()
        .find(|a| a["name"] == "Alice")
        .expect("Alice not found after discard");
    assert_eq!(
        alice["balance"], 100,
        "Alice balance should still be 100 after discard, got {:?}",
        alice["balance"]
    );
}

for_each_runtime!(transactions, transactions_test);
