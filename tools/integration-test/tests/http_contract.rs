use std::collections::BTreeSet;

use integration_test::TestCluster;
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Map, Value};

struct JsonResponse {
    status: StatusCode,
    content_type: String,
    body: Value,
}

async fn send_json(
    client: &Client,
    method: Method,
    base_url: &str,
    path: &str,
    body: Option<&Value>,
) -> JsonResponse {
    let request = client.request(method, format!("{base_url}{path}"));
    let response = match body {
        Some(body) => request.json(body),
        None => request,
    }
    .send()
    .await
    .expect("send HTTP request");

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = response.json().await.expect("decode JSON response");

    JsonResponse {
        status,
        content_type,
        body,
    }
}

async fn add_schema(client: &Client, base_url: &str) {
    let response = client
        .post(format!("{base_url}/api/v0/collections"))
        .body("type Book { title: String }")
        .send()
        .await
        .expect("send schema request");
    assert!(
        response.status().is_success(),
        "add schema: status={} body={}",
        response.status(),
        response.text().await.unwrap_or_default()
    );
}

fn operations(document: &Value) -> BTreeSet<String> {
    const METHODS: &[&str] = &[
        "delete", "get", "head", "options", "patch", "post", "put", "trace",
    ];

    document["paths"]
        .as_object()
        .expect("OpenAPI paths object")
        .iter()
        .flat_map(|(path, item)| {
            let item = item.as_object().expect("OpenAPI path item");
            METHODS
                .iter()
                .filter(|method| item.contains_key(**method))
                .map(move |method| format!("{} {path}", method.to_ascii_uppercase()))
        })
        .collect()
}

fn concrete_path(path: &str) -> String {
    ["sender", "data", "name", "docID", "field", "index", "id"]
        .into_iter()
        .fold(path.to_owned(), |path, parameter| {
            path.replace(&format!("{{{parameter}}}"), "probe")
        })
}

async fn assert_go_routes_resolve_in_rust(client: &Client, rust_url: &str, document: &Value) {
    for path in document["paths"].as_object().unwrap().keys() {
        let response = client
            .request(
                Method::TRACE,
                format!("{rust_url}/api/v0{}", concrete_path(path)),
            )
            .send()
            .await
            .expect("probe Rust route");
        assert_eq!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "Go path does not resolve in Rust: {path}"
        );
    }

    for operation in operations(document) {
        let (method, path) = operation.split_once(' ').unwrap();
        let response = client
            .request(
                Method::from_bytes(method.as_bytes()).unwrap(),
                format!("{rust_url}/api/v0{}", concrete_path(path)),
            )
            .send()
            .await
            .expect("probe Rust operation");
        assert_ne!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "Go operation does not resolve in Rust: {operation}"
        );
    }
}

fn json_shape(value: &Value) -> Value {
    match value {
        Value::Null => Value::String("null".into()),
        Value::Bool(_) => Value::String("boolean".into()),
        Value::Number(_) => Value::String("number".into()),
        Value::String(_) => Value::String("string".into()),
        Value::Array(values) => Value::Array(values.first().map(json_shape).into_iter().collect()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_shape(value)))
                .collect::<Map<_, _>>(),
        ),
    }
}

fn assert_same_contract(rust: &JsonResponse, go: &JsonResponse, route: &str) {
    assert_eq!(rust.status, go.status, "{route}: HTTP status");
    assert_eq!(rust.content_type, go.content_type, "{route}: content type");
    assert_eq!(
        json_shape(&rust.body),
        json_shape(&go.body),
        "{route}: JSON response shape"
    );
}

async fn compare_json(
    client: &Client,
    method: Method,
    rust_url: &str,
    go_url: &str,
    path: &str,
    body: Option<&Value>,
) -> (JsonResponse, JsonResponse) {
    let route = format!("{method} {path}");
    let rust = send_json(client, method.clone(), rust_url, path, body).await;
    let go = send_json(client, method, go_url, path, body).await;
    assert_same_contract(&rust, &go, &route);
    (rust, go)
}

#[tokio::test]
async fn go_rust_http_contract() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .build()
        .await
        .unwrap();
    let rust_url = cluster.api_url(0);
    let go_url = cluster.api_url(1);
    let client = Client::new();

    add_schema(&client, rust_url).await;
    add_schema(&client, go_url).await;

    let (rust_openapi, go_openapi) = compare_json(
        &client,
        Method::GET,
        rust_url,
        go_url,
        "/openapi.json",
        None,
    )
    .await;
    assert_eq!(
        operations(&rust_openapi.body),
        operations(&go_openapi.body),
        "registered HTTP operations"
    );
    assert_go_routes_resolve_in_rust(&client, rust_url, &go_openapi.body).await;

    let (rust_health, go_health) = compare_json(
        &client,
        Method::GET,
        rust_url,
        go_url,
        "/health-check",
        None,
    )
    .await;
    assert_eq!(rust_health.body, go_health.body);
    assert_eq!(rust_health.body, json!("Healthy"));

    let query = json!({"query": "{ Book { title } }"});
    compare_json(
        &client,
        Method::POST,
        rust_url,
        go_url,
        "/api/v0/graphql",
        Some(&query),
    )
    .await;

    let invalid_query = json!({"query": "{"});
    compare_json(
        &client,
        Method::POST,
        rust_url,
        go_url,
        "/api/v0/graphql",
        Some(&invalid_query),
    )
    .await;

    let missing_document = "/api/v0/collections/Missing/document/missing";
    let (rust_not_found, _) = compare_json(
        &client,
        Method::GET,
        rust_url,
        go_url,
        missing_document,
        None,
    )
    .await;
    assert_eq!(rust_not_found.status, StatusCode::NOT_FOUND);
    assert!(rust_not_found.body["error"].is_string());
}
