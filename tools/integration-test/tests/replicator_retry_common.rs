use std::time::{Duration, SystemTime, UNIX_EPOCH};

use integration_test::TestClusterBuilder;

pub async fn retry_intervals_test(builder: TestClusterBuilder) {
    let mut cluster = builder
        .rust_nodes(2)
        .with_extra_rust_args(["--replicator-retry-intervals=1,3600"])
        .build()
        .await
        .unwrap();
    let sender = cluster.client(0);
    let receiver = cluster.client(1);
    for node in [&sender, &receiver] {
        node.schema_add("type RetryDoc { name: String }").unwrap();
    }
    for index in 0..2 {
        cluster
            .wait_for_log(index, "p2p_listening", Duration::from_secs(15))
            .await
            .unwrap();
    }
    let addresses = receiver.p2p_info().unwrap();
    let address = addresses[0].as_str().expect("receiver address");
    sender.p2p_connect(&[address]).unwrap();
    sender.p2p_replicator_set(&["RetryDoc"], address).unwrap();
    cluster.nodes[1].process.kill();

    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    sender
        .query(r#"mutation { add_RetryDoc(input: {name: "pending"}) { _docID } }"#)
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!("{}/api/v0/p2p/sync/status", cluster.api_url(0));
    let mut last_status = serde_json::Value::Null;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            last_status = client
                .get(&url)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            let markers = &last_status["push_retry_markers"];
            if markers["document_markers"].as_u64() == Some(1) {
                if let Some(deadline) = markers["oldest_scheduled_retry_unix"].as_u64() {
                    // Reaching the second rung proves both the short initial delay
                    // and the configured reschedule, not just the reported options.
                    if deadline >= before + 1800 {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        assert!(deadline <= now + 3600);
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "retry did not reach the configured second interval: {last_status}"
    );
}
