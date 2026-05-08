use integration_test::TestCluster;

const SCHEMA: &str = r#"
type Book {
    publisher: Publisher @primary
}

type Publisher {
    book: Book
}
"#;

async fn patch_new_field_with_existing_secondary_relation_test(cluster: TestCluster) {
    let client = cluster.client(0);
    client.schema_add(SCHEMA).expect("add schema");

    client
        .collection_patch(
            r#"[{"op":"add","path":"/Publisher/Fields/-","value":{"Name":"name","Kind":"String"}}]"#,
        )
        .expect("patch Publisher.name");

    client
        .query(r#"mutation { add_Publisher(input: {name: "Penguin Books"}) { name } }"#)
        .expect("add Publisher after patch");

    let data = client
        .query("query { Publisher { name } }")
        .expect("query Publisher");
    assert_eq!(data["Publisher"][0]["name"], "Penguin Books");
}

#[tokio::test]
async fn rust_patch_new_field_with_existing_secondary_relation() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    patch_new_field_with_existing_secondary_relation_test(cluster).await;
}
