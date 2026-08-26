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
    http::{header::HOST, Method, Request, StatusCode},
    response::Response,
    Router,
};
use defra_http::router::{AppStateBuilder, CollectionVersionOperations};
use defra_http::{MockQueryExecutor, MockRestOperations};
use query::rest::RestOperations;
use schema::CollectionVersion;
use std::sync::atomic::{AtomicUsize, Ordering};
use tower::ServiceExt;

/// The stored versions, covering every branch of Go's candidate switch:
/// a collection with both an active and an inactive version, a plain active
/// one, and a wholly inactive one.
///
/// Counts which listing the handler asked for, so the cheap path can be
/// asserted rather than assumed.
#[derive(Debug, Default)]
struct Versions {
    all_calls: AtomicUsize,
    active_calls: AtomicUsize,
}

fn version(name: &str, version_id: &str, collection_id: &str, active: bool) -> CollectionVersion {
    let mut version = CollectionVersion::new(name, version_id, collection_id, vec![]);
    version.is_active = active;
    version
}

impl Versions {
    fn stored() -> Vec<CollectionVersion> {
        vec![
            version("Users", "users-v2", "users-c", true),
            version("Users", "users-v1", "users-c", false),
            version("Books", "books-v1", "books-c", true),
            version("Orders", "orders-v1", "orders-c", false),
        ]
    }
}

#[async_trait]
impl CollectionVersionOperations for Versions {
    async fn get_all_collections(&self) -> Result<Vec<CollectionVersion>, String> {
        self.all_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Self::stored())
    }

    async fn get_active_collections(&self) -> Result<Vec<CollectionVersion>, String> {
        self.active_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Self::stored().into_iter().filter(|v| v.is_active).collect())
    }
}

fn router() -> Router {
    router_with(Arc::new(Versions::default()))
}

fn router_with(versions: Arc<Versions>) -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
        .with_collection_versions(versions)
        .build();
    defra_http::create_router_with_state(state)
}

/// A router with no version store, so no listing can be answered.
fn router_without_versions() -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
        .build();
    defra_http::create_router_with_state(state)
}

async fn get_response(router: Router, query: &str) -> Response {
    router
        .oneshot(
            Request::builder()
                .uri(format!("/api/v0/collections{query}"))
                .header(HOST, "localhost:9181")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond")
}

async fn get(router: Router, query: &str) -> (StatusCode, String) {
    let response = get_response(router, query).await;
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

/// The divergence that two sources allowed: the collection cache deliberately
/// holds inactive P2P-synced collections, so the unnarrowed listing used to
/// name a collection the narrowed one dropped. Both read the stored versions
/// now, as Go does, so an inactive collection is absent from both.
#[tokio::test]
async fn an_inactive_collection_is_absent_from_narrowed_and_unnarrowed_alike() {
    assert!(
        !names("").await.contains(&"Orders".to_string()),
        "an inactive collection must not be listed"
    );
    assert!(
        names("?name=Orders").await.is_empty(),
        "and must not be selectable by name either"
    );
}

/// Every request reads the same source, so a missing version store fails the
/// same way for all of them rather than quietly answering from somewhere else.
#[tokio::test]
async fn every_request_needs_the_version_store() {
    for query in ["", "?name=Users"] {
        let (status, _) = get(router_without_versions(), query).await;
        assert!(
            status.is_server_error() || status.is_client_error(),
            "{query:?} should fail without a version store, got {status}"
        );
    }
}

/// A selector Rust cannot parse must be refused, not dropped.
#[tokio::test]
async fn an_unparseable_selector_is_refused() {
    for query in ["?get_inactive=maybe", "?get_inactive="] {
        let response = get_response(router(), query).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(
            response.headers()["content-type"],
            "application/json",
            "{query} must use the Go error envelope"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).expect("JSON error body");
        assert!(error["error"].as_str().is_some(), "{query}: {error}");
    }
}

#[tokio::test]
async fn get_inactive_accepts_every_go_boolean_form() {
    for value in ["1", "t", "T", "true", "TRUE", "True"] {
        assert_eq!(
            names(&format!("?get_inactive={value}")).await,
            vec!["Books", "Orders", "Users"],
            "{value} should be true"
        );
    }
    for value in ["0", "f", "F", "false", "FALSE", "False"] {
        assert_eq!(
            names(&format!("?get_inactive={value}")).await,
            vec!["Books", "Users"],
            "{value} should be false"
        );
    }
}

#[tokio::test]
async fn view_refresh_uses_the_same_json_selector_error() {
    let response = router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v0/view/refresh?get_inactive=maybe")
                .header(HOST, "localhost:9181")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["content-type"], "application/json");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).expect("JSON error body");
    assert!(error["error"].as_str().is_some(), "{error}");
}

/// Go keys the selectors off `Query().Has(..)`, so `?name=` is a name set to
/// the empty string, not an absent selector. It matches nothing and comes back
/// as an empty list (`http/handler_store.go:391`, then the stage-two name
/// filter drops the zero-value collection because it is not active).
#[tokio::test]
async fn an_empty_name_or_collection_id_selects_nothing() {
    for query in [
        "?name=",
        "?collection_id=",
        "?name=%20",
        "?name=&get_inactive=true",
    ] {
        let (status, body) = get(router(), query).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert!(
            names(query).await.is_empty(),
            "{query} should select nothing"
        );
    }
}

/// Go looks a version id up directly and returns `ErrCollectionNotFound`,
/// which its error mapping turns into a 404 (`internal/db/collection.go:210`,
/// `GetCollectionByID`). An unknown name is swallowed into an empty list, but
/// an unknown version id is not, so a typo must not read as success.
#[tokio::test]
async fn an_unknown_version_id_is_a_404() {
    for query in ["?version_id=nope", "?version_id="] {
        let (status, body) = get(router(), query).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{query}: {body}");
    }
}

/// The 404 fires from Go's direct lookup, which the active-by-name arm
/// pre-empts, so a name alongside the version id turns it back into an empty
/// 200. `collection describe --collection-name X --version-id Y` sends exactly
/// that combination (`cli/collection.go:86-100`), so getting this wrong 404s a
/// working CLI call.
#[tokio::test]
async fn a_name_pre_empts_the_version_lookup_so_a_bad_version_is_not_a_404() {
    for query in [
        "?name=Users&version_id=nope",
        "?name=Books&version_id=users-v2",
    ] {
        let (status, body) = get(router(), query).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert!(names(query).await.is_empty(), "{query} selects nothing");
    }
}

/// The name arm selects the active version before the version id filters it.
/// A matching active id survives, while an inactive id cannot replace it.
#[tokio::test]
async fn a_name_filters_its_active_candidate_by_version_id() {
    assert_eq!(
        names("?name=Users&version_id=users-v2").await,
        vec!["Users"]
    );
    assert!(names("?name=Users&version_id=users-v1").await.is_empty());
}

/// `get_inactive` takes the name arm out of the running, so the version
/// lookup runs again and an id that exists is not an error even when the name
/// filters it away.
#[tokio::test]
async fn get_inactive_restores_the_version_lookup() {
    let (status, body) = get(
        router(),
        "?name=Books&get_inactive=true&version_id=users-v2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(names("?name=Books&get_inactive=true&version_id=users-v2")
        .await
        .is_empty());

    let (status, _) = get(router(), "?name=Books&get_inactive=true&version_id=nope").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the lookup runs here, so a missing id is not found"
    );
}

/// Go's message for this is `collection not found` (`client/errors.go:29`).
#[tokio::test]
async fn the_not_found_body_matches_gos_message() {
    let (_, body) = get(router(), "?version_id=nope").await;
    assert!(
        body.contains("collection not found"),
        "expected Go's message, got {body}"
    );
}

/// Inactive versions cost a scan of every stored version. A selector that does
/// not ask for them must take the active listing, the same branch
/// `refresh_views` makes on the same selector.
#[tokio::test]
async fn a_selector_that_needs_no_inactive_versions_takes_the_cheap_listing() {
    for query in ["?name=Users", "?name=Users&version_id=users-v2"] {
        let versions = Arc::new(Versions::default());
        let (status, body) = get(router_with(versions.clone()), query).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");

        assert_eq!(versions.active_calls.load(Ordering::SeqCst), 1, "{query}");
        assert_eq!(
            versions.all_calls.load(Ordering::SeqCst),
            0,
            "{query} must not scan every stored version"
        );
    }
}

/// The converse: asking for inactive versions, or for a version by id, does
/// need the full listing.
#[tokio::test]
async fn a_selector_that_needs_inactive_versions_takes_the_full_listing() {
    for query in ["?get_inactive=true", "?version_id=users-v1"] {
        let versions = Arc::new(Versions::default());
        let (status, body) = get(router_with(versions.clone()), query).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");

        assert_eq!(
            versions.all_calls.load(Ordering::SeqCst),
            1,
            "{query} needs every stored version"
        );
        assert_eq!(versions.active_calls.load(Ordering::SeqCst), 0, "{query}");
    }
}
