//! `GET /collections` has to honour Go's four selectors.
//!
//! Go's client sends `name`, `version_id`, `collection_id` and `get_inactive`
//! (`http/client.go:422-448`) and Go resolves them through `getCollections`
//! (`internal/db/collection.go:193-301`). Rust parsed none of them and
//! answered every request with the full active listing, so a client asking for
//! one collection got an arbitrary one and could not tell.
//!
//! The precedence is the part worth pinning: `collection_id` only picks
//! candidates, so a name outranks it, and a name plus `get_inactive`
//! deliberately misses the active-by-name case so it can reach an inactive
//! version.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header::HOST, Request, StatusCode},
    Router,
};
use defra_http::router::{AppStateBuilder, CollectionVersionOperations};
use defra_http::{MockQueryExecutor, MockRestOperations};
use query::rest::RestOperations;
use schema::CollectionVersion;
use tower::ServiceExt;

/// The stored versions, covering every branch of Go's candidate switch:
/// a collection with both an active and an inactive version, a plain active
/// one, and a wholly inactive one.
#[derive(Debug)]
struct Versions;

fn version(name: &str, version_id: &str, collection_id: &str, active: bool) -> CollectionVersion {
    let mut version = CollectionVersion::new(name, version_id, collection_id, vec![]);
    version.is_active = active;
    version
}

#[async_trait]
impl CollectionVersionOperations for Versions {
    async fn get_all_collections(&self) -> Result<Vec<CollectionVersion>, String> {
        Ok(vec![
            version("Users", "users-v2", "users-c", true),
            version("Users", "users-v1", "users-c", false),
            version("Books", "books-v1", "books-c", true),
            version("Orders", "orders-v1", "orders-c", false),
        ])
    }
}

fn router() -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
        .with_collection_versions(Arc::new(Versions))
        .build();
    defra_http::create_router_with_state(state)
}

/// A router that can list but has no version store, so a narrowed request
/// cannot be answered.
fn router_without_versions() -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
        .build();
    defra_http::create_router_with_state(state)
}

async fn get(router: Router, query: &str) -> (StatusCode, String) {
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!("/api/v0/collections{query}"))
                .header(HOST, "localhost:9181")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn names(query: &str) -> Vec<String> {
    let (status, body) = get(router(), query).await;
    assert_eq!(status, StatusCode::OK, "{query}: {body}");
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
    parsed["collections"]
        .as_array()
        .expect("collections array")
        .iter()
        .map(|name| name.as_str().expect("name is a string").to_string())
        .collect()
}

/// The regression: a request for one collection used to answer with all of
/// them.
#[tokio::test]
async fn a_name_selects_only_that_collection() {
    assert_eq!(names("?name=Users").await, vec!["Users"]);
    assert_eq!(names("?name=Books").await, vec!["Books"]);
}

#[tokio::test]
async fn an_unknown_name_selects_nothing() {
    assert!(names("?name=Nope").await.is_empty());
}

/// Go returns a named version whether or not it is active, which is why
/// `version_id` alone reaches the inactive one.
#[tokio::test]
async fn a_version_id_selects_that_version_even_when_inactive() {
    assert_eq!(names("?version_id=users-v1").await, vec!["Users"]);
    assert_eq!(names("?version_id=orders-v1").await, vec!["Orders"]);
}

#[tokio::test]
async fn a_collection_id_selects_its_active_version() {
    assert_eq!(names("?collection_id=users-c").await, vec!["Users"]);
    assert!(names("?collection_id=orders-c").await.is_empty());
}

#[tokio::test]
async fn get_inactive_widens_to_the_inactive_versions() {
    assert_eq!(
        names("?get_inactive=true").await,
        vec!["Books", "Orders", "Users"]
    );
    assert_eq!(names("").await, vec!["Books", "Users"]);
}

/// Stage one of Go's lookup is a switch, so a name with `get_inactive` false
/// wins outright and a disagreeing collection id never applies. A flat AND
/// over the selectors would wrongly answer with nothing here.
#[tokio::test]
async fn a_name_outranks_a_disagreeing_collection_id() {
    assert_eq!(
        names("?name=Users&collection_id=books-c").await,
        vec!["Users"]
    );
}

/// The case Go's switch exists for: name plus `get_inactive` skips the
/// active-by-name arm and is narrowed by the stage-two name filter instead.
#[tokio::test]
async fn a_name_with_get_inactive_reaches_an_inactive_version() {
    assert_eq!(
        names("?name=Orders&get_inactive=true").await,
        vec!["Orders"]
    );
    assert!(names("?name=Orders").await.is_empty());
}

/// The listing is a name list, so the two versions of one collection must not
/// show up as a repeated name.
#[tokio::test]
async fn a_collection_with_two_versions_is_named_once() {
    assert_eq!(
        names("?collection_id=users-c&get_inactive=true").await,
        vec!["Users"]
    );
}

/// An unnarrowed request keeps its existing answer and its existing source,
/// which is the REST listing rather than the version store.
#[tokio::test]
async fn no_selectors_keep_the_previous_behaviour() {
    let (status, body) = get(router_without_versions(), "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("Users"), "{body}");
}

/// Failing loudly is the point of the fix. Answering a narrowed request from
/// the unnarrowed listing would be the same silent-widening bug in a new
/// place.
#[tokio::test]
async fn a_narrowed_request_without_a_version_store_is_an_error() {
    let (status, _) = get(router_without_versions(), "?name=Users").await;
    assert!(
        status.is_server_error() || status.is_client_error(),
        "expected a failure, got {status}"
    );
}

/// A selector Rust cannot parse must be refused, not dropped.
#[tokio::test]
async fn an_unparseable_selector_is_refused() {
    let (status, _) = get(router(), "?get_inactive=maybe").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
