//! Go wire paths, defined once because the router, the CLI and Go's client all
//! have to agree on them. `tests/go_route_paths.rs` proves each one resolves.

/// The API prefixes the router mounts, oldest version first.
///
/// The bare `/api` entry is Go's unversioned fallback: `r.Handle("/*", router)`
/// in `http/handler.go:127` serves the whole route set with no version segment.
pub const API_PREFIXES: &[&str] = &["/api/v0", "/api/v1", "/api"];

/// `POST` - Go's `AddDACPolicy` (`http/handler_acp.go:338`).
pub const ACP_POLICY: &str = "/acp/document/policy";

/// `POST` - Go's `AddView` (`http/handler_store.go:1115`).
pub const VIEW_ADD: &str = "/view";

/// `POST` - Go's `RefreshViews` (`http/handler_store.go:1116`).
pub const VIEW_REFRESH: &str = "/view/refresh";

/// Every Go path this server adds for wire compatibility.
pub const ALL: &[&str] = &[ACP_POLICY, VIEW_ADD, VIEW_REFRESH];

/// The full path under the default API version, for a client building a URL.
pub fn v0(path: &str) -> String {
    format!("/api/v0{path}")
}
