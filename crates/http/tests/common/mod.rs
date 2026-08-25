#![allow(dead_code)]

//! Shared harness for the Go wire-contract tests.

pub use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
pub use axum::http::{Method, StatusCode};

use axum::{
    body::{to_bytes, Body},
    http::{
        header::{AUTHORIZATION, CONTENT_TYPE, HOST},
        Request,
    },
    Router,
};
use identity::{new_token, Identity, RawIdentity};
use tower::ServiceExt;

#[allow(unused_imports)]
pub use defra_http::route_permissions::{route_permission, RoutePermission};
#[allow(unused_imports)]
pub use defra_http::router::NodePermission;
use defra_http::router::{AppStateBuilder, ViewOperations};
use defra_http::{MockAcpOperations, MockQueryExecutor};

pub const TEST_HOST: &str = "localhost:9181";
pub const POLICY: &str = "name: test\nresources:\n  doc:\n    permissions:\n";

/// Records what the handler asked the view layer to do.
#[derive(Debug, Default)]
pub struct RecordingViewOps {
    pub add_view_transform: std::sync::Mutex<Option<Option<String>>>,
    pub refresh: std::sync::Mutex<Option<db::CollectionSelector>>,
}

#[async_trait]
impl ViewOperations for RecordingViewOps {
    async fn add_view(
        &self,
        _gql_query: &str,
        _sdl: &str,
        transform: Option<&str>,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        *self.add_view_transform.lock().unwrap() = Some(transform.map(str::to_owned));
        Ok(vec![])
    }

    async fn refresh_views(&self, options: db::CollectionSelector) -> Result<(), String> {
        *self.refresh.lock().unwrap() = Some(options);
        Ok(())
    }

    async fn gc_downsample_histories(&self, _names: Option<Vec<String>>) -> Result<(), String> {
        Ok(())
    }
}

pub fn router_with(view: Arc<RecordingViewOps>) -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_view(view)
        .build();
    defra_http::create_router_with_state(state)
}

pub fn router() -> Router {
    router_with(Arc::new(RecordingViewOps::default()))
}

pub fn bearer_token() -> String {
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let token = new_token(
        &identity,
        Duration::from_secs(3600),
        Some(TEST_HOST.to_lowercase()),
        None,
    )
    .unwrap();
    format!("Bearer {}", String::from_utf8(token).unwrap())
}

/// A NAC owner and a bearer token that authenticates as them, for tests that
/// go through the real auth middleware rather than calling a handler direct.
pub fn nac_owner() -> (identity::Did, String) {
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let did = identity.did().expect("mock identity has a did");
    let token = new_token(
        &identity,
        Duration::from_secs(3600),
        Some(TEST_HOST.to_lowercase()),
        None,
    )
    .unwrap();
    (did, format!("Bearer {}", String::from_utf8(token).unwrap()))
}

pub struct Call {
    pub method: Method,
    pub path: String,
    pub body: String,
    pub content_type: Option<&'static str>,
    pub authenticated: bool,
}

impl Call {
    pub fn post(path: &str) -> Self {
        Self {
            method: Method::POST,
            path: path.to_string(),
            body: String::new(),
            content_type: None,
            authenticated: false,
        }
    }

    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }

    pub fn json(mut self, body: &str) -> Self {
        self.content_type = Some("application/json");
        self.body = body.to_string();
        self
    }

    pub fn authenticated(mut self) -> Self {
        self.authenticated = true;
        self
    }

    pub async fn send_to(&self, router: Router) -> (StatusCode, String) {
        let mut builder = Request::builder()
            .method(self.method.clone())
            .uri(&self.path)
            .header(HOST, TEST_HOST);
        if let Some(content_type) = self.content_type {
            builder = builder.header(CONTENT_TYPE, content_type);
        }
        if self.authenticated {
            builder = builder.header(AUTHORIZATION, bearer_token());
        }
        let response = router
            .oneshot(builder.body(Body::from(self.body.clone())).unwrap())
            .await
            .expect("router should respond");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    /// Sends to a freshly built router, so two calls compared against each
    /// other cannot differ through accumulated mock state.
    pub async fn send(&self) -> (StatusCode, String) {
        self.send_to(router()).await
    }
}
