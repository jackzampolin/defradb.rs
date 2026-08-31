use std::time::Duration;

use integration_test::TestCluster;

#[tokio::test]
async fn rust_lens_survives_restart() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_store("regolith")
        .build()
        .await
        .unwrap();

    let client = cluster.client(0);

    // Deploy schema and create documents
    client
        .schema_add("type Article { title: String }")
        .expect("add Article schema");

    client
        .query(r#"mutation { add_Article(input: {title: "First"}) { _docID } }"#)
        .expect("create article 1");
    client
        .query(r#"mutation { add_Article(input: {title: "Second"}) { _docID } }"#)
        .expect("create article 2");

    // Query before restart to establish baseline
    let before = client
        .query("query { Article { title } }")
        .expect("query articles before restart");
    let before_arr = before["Article"].as_array().expect("articles array");
    assert_eq!(before_arr.len(), 2, "should have 2 articles before restart");

    // Verify lens commands work
    let list_result = client.lens_list().expect("lens_list before restart");
    let is_empty = list_result.is_null()
        || list_result
            .as_object()
            .map(|o| o.is_empty())
            .unwrap_or(false)
        || list_result
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(false);
    assert!(
        is_empty,
        "lens_list should be empty on fresh node, got: {}",
        list_result
    );

    // Restart the node (same rootdir, regolith persists to disk)
    cluster
        .restart_node(0, Duration::from_secs(30))
        .await
        .expect("restart node");

    // Query after restart — data should survive
    let client = cluster.client(0);
    let after = client
        .query("query { Article { title } }")
        .expect("query articles after restart");
    let after_arr = after["Article"].as_array().expect("articles array");
    assert_eq!(after_arr.len(), 2, "should have 2 articles after restart");

    // Verify lens subsystem is functional after restart
    client.lens_reload().expect("lens_reload after restart");
    client.lens_list().expect("lens_list after restart");
}
