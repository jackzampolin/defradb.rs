//! The CLI must speak Go's view wire contract, not just reach a route that
//! exists.
//!
//! Go's `client view refresh` takes four selectors and sends them as query
//! parameters against `/view/refresh`; Rust's CLI could express only the name,
//! and posted a JSON body to `/views/refresh`. A server that accepts Go's
//! contract is only half the fix while the shipped client cannot produce it.

use clap::Parser;
use cli::commands::client::http_client::{HttpClient, ViewRefreshSelectors};
use cli::commands::client::view::ViewRefreshArgs;

#[derive(Parser, Debug)]
struct Wrapper {
    #[command(flatten)]
    args: ViewRefreshArgs,
}

fn parse(extra: &[&str]) -> ViewRefreshArgs {
    let mut argv = vec!["view-refresh"];
    argv.extend_from_slice(extra);
    Wrapper::parse_from(argv).args
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// The URLs Go's own client builds, from `http/client.go` and
/// `http/client_acp.go`. Rust's client used its own spellings, so it never
/// exercised the routes a Go-compatible client actually calls.
///
/// The path segments come from `defra_http::go_paths`, which the server's own
/// router test also asserts against, so client and server cannot drift.
#[test]
fn the_client_builds_gos_wire_urls() {
    let client = HttpClient::new("http://localhost:9181").expect("a valid base URL");

    assert_eq!(
        client.acp_policy_url(),
        "http://localhost:9181/api/v0/acp/document/policy"
    );
    assert_eq!(client.view_add_url(), "http://localhost:9181/api/v0/view");
    assert_eq!(
        client.view_refresh_url(),
        "http://localhost:9181/api/v0/view/refresh"
    );
}

/// A trailing slash on the base URL must not produce a doubled separator, since
/// Rust does not normalize paths the way Go's `StripSlashes` does.
#[test]
fn a_trailing_slash_on_the_base_url_does_not_double_up() {
    let client = HttpClient::new("http://localhost:9181/").expect("a valid base URL");
    assert_eq!(client.view_add_url(), "http://localhost:9181/api/v0/view");
}

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

/// Every flag Go's `view refresh` accepts must exist here under the same name.
#[test]
fn every_go_selector_flag_is_accepted() {
    let args = parse(&[
        "--collection-name",
        "UserView",
        "--collection-id",
        "bae-collection",
        "--version-id",
        "bae-version",
        "--get-inactive",
    ]);

    assert_eq!(
        args.selectors(),
        ViewRefreshSelectors {
            name: Some("UserView".to_string()),
            collection_id: Some("bae-collection".to_string()),
            version_id: Some("bae-version".to_string()),
            get_inactive: true,
        }
    );
}

#[test]
fn no_flags_select_nothing() {
    assert_eq!(parse(&[]).selectors(), ViewRefreshSelectors::default());
}

#[test]
fn get_inactive_defaults_to_false() {
    assert!(!parse(&["--collection-name", "UserView"]).get_inactive);
}

/// A selector Go does not have must not be invented here.
#[test]
fn an_unknown_selector_is_refused() {
    Wrapper::try_parse_from(["view-refresh", "--collection-set-id", "x"])
        .expect_err("an unknown selector must not parse");
}

// ---------------------------------------------------------------------------
// Query string
// ---------------------------------------------------------------------------

/// Go names the parameters `name`, `version_id`, `collection_id` and
/// `get_inactive` (`http/client.go:294-309`). A mismatch here is silently
/// ignored by the server rather than rejected, so it is pinned exactly.
#[test]
fn the_query_string_uses_gos_parameter_names() {
    let query = ViewRefreshSelectors {
        name: Some("UserView".to_string()),
        collection_id: Some("c1".to_string()),
        version_id: Some("v1".to_string()),
        get_inactive: true,
    }
    .query_string();

    assert!(query.starts_with('?'), "{query}");
    for expected in [
        "name=UserView",
        "version_id=v1",
        "collection_id=c1",
        "get_inactive=true",
    ] {
        assert!(query.contains(expected), "{expected} missing from {query}");
    }
}

/// Go omits an unset selector rather than sending it empty, because an empty
/// `name` would select the collection named "" instead of every collection.
#[test]
fn unset_selectors_are_omitted() {
    let query = ViewRefreshSelectors {
        name: Some("UserView".to_string()),
        ..ViewRefreshSelectors::default()
    }
    .query_string();

    assert_eq!(query, "?name=UserView");
}

/// Go sets `get_inactive` only when true, so refreshing everything sends no
/// query string at all.
#[test]
fn selecting_nothing_produces_no_query_string() {
    assert_eq!(ViewRefreshSelectors::default().query_string(), "");
}

#[test]
fn get_inactive_alone_is_the_only_parameter() {
    let query = ViewRefreshSelectors {
        get_inactive: true,
        ..ViewRefreshSelectors::default()
    }
    .query_string();

    assert_eq!(query, "?get_inactive=true");
}

/// A name is user input and can carry characters that would otherwise split the
/// query string or be read as another parameter.
#[test]
fn selector_values_are_percent_encoded() {
    let query = ViewRefreshSelectors {
        name: Some("a b&get_inactive=true".to_string()),
        ..ViewRefreshSelectors::default()
    }
    .query_string();

    assert!(!query.contains("a b"), "a space must be encoded: {query}");
    assert_eq!(
        query.matches('&').count(),
        0,
        "an embedded ampersand must not add a parameter: {query}"
    );
}

// ---------------------------------------------------------------------------
// Add-view body
// ---------------------------------------------------------------------------

/// Go's client marshals the transform as `TransformCID`. Rust sent `Transform`,
/// which a Go-compatible server ignores, dropping the lens transform without
/// telling the caller.
#[test]
fn the_add_view_body_uses_gos_transform_key() {
    let body = cli::commands::client::http_client::AddViewRequest {
        query: "User { name }".to_string(),
        sdl: "type UserView { name: String }".to_string(),
        transform: Some("bafy".to_string()),
    };
    let json = serde_json::to_value(&body).unwrap();

    assert_eq!(json["TransformCID"], "bafy");
    assert_eq!(json["Query"], "User { name }");
    assert_eq!(json["SDL"], "type UserView { name: String }");
    assert!(
        json.get("Transform").is_none(),
        "the legacy key must not also be sent: {json}"
    );
}

/// Go omits the key when there is no transform, rather than sending null.
#[test]
fn an_absent_transform_is_omitted() {
    let body = cli::commands::client::http_client::AddViewRequest {
        query: "User { name }".to_string(),
        sdl: "type UserView { name: String }".to_string(),
        transform: None,
    };
    let json = serde_json::to_value(&body).unwrap();

    assert!(json.get("TransformCID").is_none(), "{json}");
}

/// Go's refresh answers `200` with no body (`http/handler_store.go:450`) and its
/// own client uses the non-JSON request path. Deserializing a response here made
/// a successful refresh against a Go node report a parse error, so the call must
/// not yield a parsed body at all.
#[test]
fn view_refresh_parses_no_response_body() {
    // Compiles only while `view_refresh` yields `Result<()>`. A body-parsing
    // signature returns `Result<JsonValue>` and fails to unify here.
    fn _typecheck(client: &HttpClient, selectors: &ViewRefreshSelectors) {
        let call = client.view_refresh(selectors);
        let _: &dyn std::future::Future<Output = cli::error::Result<()>> = &call;
    }
}
