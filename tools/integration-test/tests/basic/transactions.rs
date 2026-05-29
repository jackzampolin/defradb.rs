use std::time::Duration;

use integration_test::{for_each_runtime, open_events_sse, poll_until, TestCluster};

async fn txn_schema_add_and_mutate(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    // 1. Create a transaction
    let tx_id = client.tx_create().expect("tx_create failed");

    // 2. Add a schema within the transaction via HTTP
    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(format!("{}/api/v0/tx/{}/schema", api_url, tx_id))
        .body("type Widget { name: String  weight: Int }")
        .send()
        .await
        .expect("schema add in txn request failed");
    assert!(
        resp.status().is_success(),
        "schema add in txn failed with status: {} body: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // 3. Run a mutation on the new collection within the same transaction
    let create_result = client
        .query_with_tx(
            r#"mutation { add_Widget(input: {name: "Sprocket", weight: 42}) { _docID name weight } }"#,
            &tx_id,
        )
        .expect("create Widget in tx");
    let widget_id = create_result["add_Widget"][0]["_docID"]
        .as_str()
        .expect("missing Widget _docID");
    assert!(!widget_id.is_empty(), "Widget _docID should be non-empty");
    assert_eq!(create_result["add_Widget"][0]["name"], "Sprocket");
    assert_eq!(create_result["add_Widget"][0]["weight"], 42);

    // 4. Query within the same transaction to verify the data is visible
    let query_result = client
        .query_with_tx("query { Widget { name weight } }", &tx_id)
        .expect("query Widget in tx");
    let widgets = query_result["Widget"]
        .as_array()
        .expect("Widget result not array");
    assert_eq!(widgets.len(), 1, "expected 1 Widget in tx");
    assert_eq!(widgets[0]["name"], "Sprocket");

    // 5. Query outside the transaction - Widget collection should not exist yet
    let outside = client.query("query { Widget { name weight } }");
    assert!(
        outside.is_err(),
        "Widget should not be visible outside uncommitted transaction"
    );

    // 6. Commit the transaction
    client.tx_commit(&tx_id).expect("tx_commit failed");

    // 7. Verify the data persists after commit
    let after_commit = client
        .query("query { Widget { name weight } }")
        .expect("query Widget after commit");
    let committed_widgets = after_commit["Widget"]
        .as_array()
        .expect("committed Widget result not array");
    assert_eq!(committed_widgets.len(), 1, "expected 1 Widget after commit");
    assert_eq!(committed_widgets[0]["name"], "Sprocket");
    assert_eq!(committed_widgets[0]["weight"], 42);
}

#[tokio::test]
async fn rust_txn_schema_add_and_mutate() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    txn_schema_add_and_mutate(cluster).await;
}

async fn txn_schema_add_via_header_is_scoped(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);
    let tx_id = client.tx_create().expect("tx_create failed");

    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(format!("{}/api/v0/collections", api_url))
        .header("x-defradb-tx", &tx_id)
        .body("type HeaderWidget { name: String }")
        .send()
        .await
        .expect("schema add request failed");
    assert!(
        resp.status().is_success(),
        "schema add in tx failed with status: {} body: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let in_tx = client
        .query_with_tx(
            r#"mutation { add_HeaderWidget(input: {name: "scoped"}) { name } }"#,
            &tx_id,
        )
        .expect("create HeaderWidget in tx");
    assert_eq!(in_tx["add_HeaderWidget"][0]["name"], "scoped");

    let outside = client.query("query { HeaderWidget { name } }");
    assert!(
        outside.is_err(),
        "schema created with x-defradb-tx should not be visible outside the transaction"
    );

    client.tx_commit(&tx_id).expect("tx_commit failed");

    let after_commit = client
        .query("query { HeaderWidget { name } }")
        .expect("query HeaderWidget after commit");
    assert_eq!(after_commit["HeaderWidget"][0]["name"], "scoped");
}

#[tokio::test]
async fn rust_txn_schema_add_via_header_is_scoped() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    txn_schema_add_via_header_is_scoped(cluster).await;
}

async fn concurrent_txn_endpoint_is_not_exposed(cluster: TestCluster) {
    let api_url = cluster.api_url(0);
    let response = reqwest::Client::new()
        .post(format!("{}/api/v0/tx/concurrent", api_url))
        .send()
        .await
        .expect("tx concurrent request failed");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        body.contains("invalid transaction id"),
        "expected invalid transaction id error, got: {body}"
    );
}

#[tokio::test]
async fn rust_concurrent_txn_endpoint_is_not_exposed() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    concurrent_txn_endpoint_is_not_exposed(cluster).await;
}

async fn update_with_filter_sees_txn_scoped_state(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);
    let tx_id = client.tx_create().expect("tx_create failed");

    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(format!("{}/api/v0/tx/{}/schema", api_url, tx_id))
        .body("type TxnFilterUser { name: String  age: Int }")
        .send()
        .await
        .expect("schema add in txn request failed");
    assert!(
        resp.status().is_success(),
        "schema add in txn failed with status: {} body: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    client
        .query_with_tx(
            r#"mutation { add_TxnFilterUser(input: {name: "John", age: 27}) { _docID } }"#,
            &tx_id,
        )
        .expect("create TxnFilterUser in tx");

    let update = client
        .query_with_tx(
            r#"mutation { update_TxnFilterUser(filter: {name: {_eq: "John"}}, input: {name: "Chris"}) { name age } }"#,
            &tx_id,
        )
        .expect("update TxnFilterUser by filter in tx");

    let updated = update["update_TxnFilterUser"]
        .as_array()
        .expect("update result not array");
    assert_eq!(updated.len(), 1, "expected one filtered update result");
    assert_eq!(updated[0]["name"], "Chris");
    assert_eq!(updated[0]["age"], 27);

    client.tx_commit(&tx_id).expect("tx_commit failed");

    let after_commit = client
        .query("query { TxnFilterUser { name age } }")
        .expect("query TxnFilterUser after commit");
    assert_eq!(after_commit["TxnFilterUser"][0]["name"], "Chris");
    assert_eq!(after_commit["TxnFilterUser"][0]["age"], 27);
}

#[tokio::test]
async fn rust_update_with_filter_sees_txn_scoped_state() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    update_with_filter_sees_txn_scoped_state(cluster).await;
}

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

async fn txn_commit_publishes_update_events(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    // Schema first (auto-commit, generates events we don't care about)
    client
        .schema_add("type TxEvent { name: String value: Int }")
        .expect("schema add");

    // Open SSE stream BEFORE the tx so we capture every emitted event
    let (sse_handle, events) = open_events_sse(api_url, "update").await;

    // Drain any startup events from schema add
    tokio::time::sleep(Duration::from_millis(200)).await;
    let baseline = events.lock().unwrap().len();

    // Open a transaction and do three writes inside it
    let tx_id = client.tx_create().expect("tx_create failed");

    let create_a = client
        .query_with_tx(
            r#"mutation { add_TxEvent(input: {name: "a", value: 1}) { _docID } }"#,
            &tx_id,
        )
        .expect("create a in tx");
    let create_b = client
        .query_with_tx(
            r#"mutation { add_TxEvent(input: {name: "b", value: 2}) { _docID } }"#,
            &tx_id,
        )
        .expect("create b in tx");
    let create_c = client
        .query_with_tx(
            r#"mutation { add_TxEvent(input: {name: "c", value: 3}) { _docID } }"#,
            &tx_id,
        )
        .expect("create c in tx");
    let doc_a = create_a["add_TxEvent"][0]["_docID"]
        .as_str()
        .expect("missing _docID for TxEvent a")
        .to_string();
    let doc_b = create_b["add_TxEvent"][0]["_docID"]
        .as_str()
        .expect("missing _docID for TxEvent b")
        .to_string();
    let doc_c = create_c["add_TxEvent"][0]["_docID"]
        .as_str()
        .expect("missing _docID for TxEvent c")
        .to_string();

    // Critical assertion: zero events while tx is uncommitted
    tokio::time::sleep(Duration::from_millis(300)).await;
    {
        let observed = events.lock().unwrap();
        let count_after_writes = observed.len();
        assert_eq!(
            count_after_writes,
            baseline,
            "no update events should fire before tx commit; got {} new events",
            count_after_writes.saturating_sub(baseline)
        );
    }

    // Commit the transaction
    client.tx_commit(&tx_id).expect("tx_commit failed");

    // After commit: exactly three events should arrive, one per doc
    poll_until(
        || {
            let observed = events.lock().unwrap();
            let new_events: Vec<_> = observed.iter().skip(baseline).collect();
            new_events.len() >= 3
        },
        Duration::from_secs(5),
        Duration::from_millis(50),
        "three update events should arrive after tx commit",
    )
    .await;

    // Validate doc_ids match
    let observed = events.lock().unwrap();
    let new_events: Vec<_> = observed.iter().skip(baseline).collect();
    assert_eq!(
        new_events.len(),
        3,
        "expected exactly 3 update events after commit; got {}",
        new_events.len()
    );
    let observed_doc_ids: std::collections::HashSet<String> = new_events
        .iter()
        .filter_map(|e| e.pointer("/data/doc_id").and_then(|v| v.as_str()))
        .map(String::from)
        .collect();
    assert!(
        observed_doc_ids.contains(&doc_a),
        "missing event for doc_a={doc_a}"
    );
    assert!(
        observed_doc_ids.contains(&doc_b),
        "missing event for doc_b={doc_b}"
    );
    assert!(
        observed_doc_ids.contains(&doc_c),
        "missing event for doc_c={doc_c}"
    );

    // Also validate cids are non-default. Cid::default().to_string() is
    // "baeaaaaa" (non-empty), so an is_empty() check would silently pass the
    // pre-fix bug where a failed block write emitted an event with a default
    // cid. Compare against the parsed default explicitly.
    let default_cid = cid::Cid::default();
    for event in &new_events {
        let cid_str = event
            .pointer("/data/cid")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(
            !cid_str.is_empty(),
            "event cid should not be empty: {event:?}"
        );
        let parsed: cid::Cid = cid_str.parse().unwrap_or_else(|e| {
            panic!("event cid should parse as a real Cid (got {cid_str:?}): {e}")
        });
        assert_ne!(
            parsed, default_cid,
            "event cid should not be Cid::default() (would indicate the block write failed): {event:?}"
        );

        // The block field carries hex-encoded composite commit bytes. It
        // must reach SSE subscribers so downstream consumers (e.g. defra-agent)
        // can traverse the DAG without an extra round-trip.
        let block_hex = event
            .pointer("/data/block")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("event missing /data/block: {event:?}"));
        assert!(
            !block_hex.is_empty(),
            "event block hex should not be empty: {event:?}"
        );
        let block_bytes = hex::decode(block_hex)
            .unwrap_or_else(|e| panic!("block field should be valid hex ({block_hex:?}): {e}"));
        assert!(
            !block_bytes.is_empty(),
            "decoded block bytes should not be empty: {event:?}"
        );
    }

    sse_handle.abort();
}

#[tokio::test]
async fn rust_txn_commit_publishes_update_events() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    txn_commit_publishes_update_events(cluster).await;
}

async fn txn_discard_publishes_no_events(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    client
        .schema_add("type TxDiscard { name: String }")
        .expect("schema add");

    let (sse_handle, events) = open_events_sse(api_url, "update").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let baseline = events.lock().unwrap().len();

    let tx_id = client.tx_create().expect("tx_create failed");
    client
        .query_with_tx(
            r#"mutation { add_TxDiscard(input: {name: "doomed"}) { _docID } }"#,
            &tx_id,
        )
        .expect("create in tx");

    // Discard, not commit
    client.tx_discard(&tx_id).expect("tx_discard failed");

    // Wait long enough that an event would have arrived if it were going to
    tokio::time::sleep(Duration::from_millis(500)).await;
    let observed = events.lock().unwrap();
    assert_eq!(
        observed.len(),
        baseline,
        "no events should fire on discard; got {} new events",
        observed.len().saturating_sub(baseline)
    );

    sse_handle.abort();
}

#[tokio::test]
async fn rust_txn_discard_publishes_no_events() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    txn_discard_publishes_no_events(cluster).await;
}
