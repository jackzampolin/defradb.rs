use defra_harness::DefraClient;
use serde_json::Value;

pub fn query_both(rust: &DefraClient, go: &DefraClient, query: &str) -> (Value, Value) {
    (
        rust.query(query)
            .unwrap_or_else(|error| panic!("Rust query failed: {error}\n{query}")),
        go.query(query)
            .unwrap_or_else(|error| panic!("Go query failed: {error}\n{query}")),
    )
}

pub fn assert_query_equivalent(rust: &DefraClient, go: &DefraClient, query: &str) -> Value {
    let (rust_response, go_response) = query_both(rust, go, query);
    assert_eq!(
        rust_response, go_response,
        "query responses diverged: {query}"
    );
    rust_response
}
