use integration_test::TestCluster;

async fn dump_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // Deploy schema and create a document so there's data to dump
    client
        .schema_add("type User { name: String  age: Int }")
        .unwrap();
    client
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .unwrap();

    // Dump should return data
    let result = client.dump();
    assert!(result.is_ok(), "dump failed: {:?}", result.err());

    let dump_val = result.unwrap();
    // Dump returns an array of strings
    let arr = dump_val.as_array().expect("dump should return an array");
    assert!(
        !arr.is_empty(),
        "dump should contain entries after creating data"
    );
}

#[tokio::test]
async fn rust_dump() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap();
    dump_test(cluster).await;
}

/// Go has a CID parsing bug in the dump endpoint: "invalid cid: trailing bytes
/// in data buffer passed to cid Cast". Ignored until fixed upstream.
#[tokio::test]
#[ignore]
async fn go_dump() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap();
    dump_test(cluster).await;
}
