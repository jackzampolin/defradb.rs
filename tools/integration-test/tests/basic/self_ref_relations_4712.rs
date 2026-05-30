use integration_test::TestCluster;
use serde_json::Value;

const SCHEMA: &str = r#"
type Dev_RC_Domain {
    routes: [Dev_RC_RedirectRoute]
    firstRoute: Dev_RC_RedirectRoute @primary @relation(name: "domain_first_route")
}

type Dev_RC_RedirectRoute {
    firstForDomain: Dev_RC_Domain @relation(name: "domain_first_route")

    domain: Dev_RC_Domain
    after: Dev_RC_RedirectRoute
}
"#;

fn field<'a>(collection: &'a Value, name: &str) -> &'a Value {
    collection["Fields"]
        .as_array()
        .expect("Fields should be an array")
        .iter()
        .find(|field| field["Name"] == name)
        .unwrap_or_else(|| panic!("missing field {name} in collection {collection:?}"))
}

fn assert_relative_id(collection: &Value, field_name: &str, expected: &str) {
    let field = field(collection, field_name);
    let actual = &field["Kind"]["RelativeID"];
    assert_eq!(
        actual, expected,
        "unexpected RelativeID for {field_name}: {field:?}"
    );
}

async fn multi_layer_self_ref_relations_test(cluster: TestCluster) {
    let client = cluster.client(0);
    client.schema_add(SCHEMA).expect("add schema");

    let collections: Vec<Value> = reqwest::get(format!(
        "{}/api/v0/collections/versions",
        cluster.api_url(0)
    ))
    .await
    .expect("get collection versions")
    .json()
    .await
    .expect("decode collection versions");
    let domain = collections
        .iter()
        .find(|collection| collection["Name"] == "Dev_RC_Domain")
        .expect("missing Dev_RC_Domain");
    let route = collections
        .iter()
        .find(|collection| collection["Name"] == "Dev_RC_RedirectRoute")
        .expect("missing Dev_RC_RedirectRoute");

    assert_relative_id(domain, "firstRoute", "1");
    assert_relative_id(domain, "routes", "1");
    assert_relative_id(route, "firstForDomain", "0");
    assert_relative_id(route, "domain", "0");
    assert_relative_id(route, "after", "1");
}

#[tokio::test]
async fn rust_multi_layer_self_ref_relations() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    multi_layer_self_ref_relations_test(cluster).await;
}
