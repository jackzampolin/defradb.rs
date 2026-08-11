use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use query::executor::QueryExecutor;
use serde_json::Value;
use tower::ServiceExt;

use crate::mock::MockQueryExecutor;
use crate::router::{create_router_with_state, AppStateBuilder, BlockOperations};

#[derive(Default)]
struct RecordingBlock {
    calls: Mutex<Vec<(String, Option<String>)>>,
}

#[async_trait]
impl BlockOperations for RecordingBlock {
    async fn signed_block_bytes(
        &self,
        cid: &str,
        caller_did: Option<&str>,
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        self.calls
            .lock()
            .expect("recording block lock")
            .push((cid.to_string(), caller_did.map(str::to_string)));
        Ok((b"canonical block".to_vec(), b"detached signature".to_vec()))
    }

    async fn verify_signature(
        &self,
        _cid: &str,
        _public_key: &str,
        _key_type: Option<&str>,
        _caller_did: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn signed_block_route_returns_canonical_material() {
    let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
    let block = Arc::new(RecordingBlock::default());
    let state = AppStateBuilder::new(executor)
        .with_block(block.clone() as Arc<dyn BlockOperations>)
        .build();

    for path in [
        "/api/v0/block/signed?cid=bafy-test",
        "/api/v1/block/signed?cid=bafy-test",
    ] {
        let response = create_router_with_state(state.clone())
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        let value: Value = serde_json::from_slice(&body).expect("valid JSON response");
        assert_eq!(value["cid"], "bafy-test");
        assert_eq!(value["block"], "Y2Fub25pY2FsIGJsb2Nr");
        assert_eq!(value["signature"], "ZGV0YWNoZWQgc2lnbmF0dXJl");
    }

    assert_eq!(
        *block.calls.lock().expect("recording block lock"),
        vec![
            ("bafy-test".to_string(), None),
            ("bafy-test".to_string(), None),
        ]
    );
}
