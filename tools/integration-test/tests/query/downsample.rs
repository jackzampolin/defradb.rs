use integration_test::{poll_until, workspace_root, TestCluster};
use serde_json::Value;
use std::time::Duration;

const REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

fn view_names(response: &Value) -> Vec<String> {
    let mut names: Vec<String> = response["UserDownsample"]
        .as_array()
        .expect("UserDownsample response should be an array")
        .iter()
        .map(|row| {
            row["name"]
                .as_str()
                .expect("view row should contain a name")
                .to_string()
        })
        .collect();
    names.sort();
    names
}

async fn downsample_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type User { name: String  age: Int }")
        .expect("add base schema");
    client
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create seed user");

    let created_view = client
        .view_add(
            "User { name age }",
            "type UserDownsample @downsample(interval: 1) { name: String  age: Int }",
        )
        .expect("create downsampled view");
    assert_eq!(
        created_view[0]["DownsampleInterval"].as_u64(),
        Some(1),
        "view creation should preserve the downsample interval on the collection schema"
    );

    let initial = client
        .query(r#"query { UserDownsample(order: {name: ASC}) { name age } }"#)
        .expect("query initial downsample view");
    assert_eq!(view_names(&initial), vec!["Alice".to_string()]);

    client
        .query(r#"mutation { add_User(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create second user");

    let stale = client
        .query(r#"query { UserDownsample(order: {name: ASC}) { name age } }"#)
        .expect("query stale downsample view");
    assert_eq!(
        view_names(&stale),
        vec!["Alice".to_string()],
        "downsample view should stay stale until the scheduled refresh runs"
    );

    poll_until(
        || {
            let refreshed = client
                .query(r#"query { UserDownsample(order: {name: ASC}) { name age } }"#)
                .expect("query refreshed downsample view");
            view_names(&refreshed) == vec!["Alice".to_string(), "Bob".to_string()]
        },
        REFRESH_TIMEOUT,
        POLL_INTERVAL,
        "scheduled downsample refresh should materialize the new row",
    )
    .await;
}

#[tokio::test]
async fn rust_downsample_refreshes_materialized_view_on_interval() {
    let _root = workspace_root();
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .expect("build cluster");
    downsample_test(cluster).await;
}
