use integration_test::{for_each_runtime, TestCluster};

async fn export_requires_dev_mode_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type Note { text: String }")
        .expect("failed to add schema");

    let node_dir = cluster.nodes[0].rootdir.to_str().unwrap().to_string();
    let path = format!("{}/backup.json", node_dir);

    let result = client.backup_export(&path, &[], false);
    assert!(
        result.is_err(),
        "backup export should fail when not in dev mode"
    );

    let err_msg = result.unwrap_err().to_string();
    assert_operation_requires_developer_mode(&err_msg);
}

async fn import_requires_dev_mode_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type Note { text: String }")
        .expect("failed to add schema");

    let node_dir = cluster.nodes[0].rootdir.to_str().unwrap().to_string();
    let path = format!("{}/backup.json", node_dir);

    std::fs::write(&path, r#"{"Note": [{"text": "hello"}]}"#).expect("failed to write file");

    let result = client.backup_import(&path);
    assert!(
        result.is_err(),
        "backup import should fail when not in dev mode"
    );

    let err_msg = result.unwrap_err().to_string();
    assert_operation_requires_developer_mode(&err_msg);
}

for_each_runtime!(export_requires_dev_mode, export_requires_dev_mode_test);
for_each_runtime!(import_requires_dev_mode, import_requires_dev_mode_test);

fn assert_operation_requires_developer_mode(err_msg: &str) {
    assert!(
        err_msg.contains("operation not permitted whilst development mode is disabled"),
        "error should match developer-mode semantics, got: {}",
        err_msg
    );
}
