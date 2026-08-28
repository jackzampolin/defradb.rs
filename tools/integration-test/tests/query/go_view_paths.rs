//! Go's view wire paths must answer the same on both runtimes.
//!
//! Go's client posts views to `/view` and refreshes at `/view/refresh` with no
//! body and query-parameter selectors. Rust served `/views` and demanded a JSON
//! body, so a Go-compatible client got a 404, and after a path-only fix would
//! have got a 415.

use integration_test::{for_each_runtime, TestCluster};

async fn post(api_url: &str, path: &str, body: Option<serde_json::Value>) -> (u16, String) {
    let request = reqwest::Client::new().post(format!("{api_url}{path}"));
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    let response = request.send().await.expect("send request");
    let status = response.status().as_u16();
    (status, response.text().await.expect("read body"))
}

async fn go_view_paths_test(cluster: TestCluster) {
    let client = cluster.client(0);
    client
        .schema_add("type Reading { name: String  value: Int }")
        .unwrap();
    let api_url = cluster.api_url(0);

    // Go's AddView: JSON body keyed exactly as its client marshals it.
    let (status, body) = post(
        api_url,
        "/api/v1/view",
        Some(serde_json::json!({
            "Query": "Reading { name value }",
            "SDL": "type ReadingView { name: String  value: Int }",
            "TransformCID": null,
        })),
    )
    .await;
    assert!(
        (200..300).contains(&status),
        "POST /api/v1/view: status={status} body={body}"
    );

    // A supplied transform must never be silently dropped. Either the runtime
    // records it, or it rejects the request; succeeding while discarding it is
    // the failure mode this asserts against, because the caller cannot see it.
    const TRANSFORM_CID: &str = "bafkreieqfyxcnvxnbxvpqnbdbslr5qbvrhbhgnrmsdrbfhqhbhmnhqhqhq";
    let (status, body) = post(
        api_url,
        "/api/v1/view",
        Some(serde_json::json!({
            "Query": "Reading { name value }",
            "SDL": "type TransformedReadingView { name: String  value: Int }",
            "TransformCID": TRANSFORM_CID,
        })),
    )
    .await;
    if (200..300).contains(&status) {
        assert!(
            body.contains(TRANSFORM_CID),
            "an accepted transform must be recorded on the created view: body={body}"
        );
    } else {
        assert!(
            (400..500).contains(&status),
            "a transform must be recorded or refused, not dropped: status={status} body={body}"
        );
    }

    // Go's RefreshViews: no body at all, selectors in the query string.
    for path in [
        "/api/v1/view/refresh",
        "/api/v1/view/refresh?name=ReadingView",
        "/api/v1/view/refresh?get_inactive=true",
    ] {
        let (status, body) = post(api_url, path, None).await;
        assert!(
            (200..300).contains(&status),
            "POST {path}: status={status} body={body}"
        );
    }

    // Go returns 400 from strconv.ParseBool for a non-boolean.
    let (status, body) = post(api_url, "/api/v1/view/refresh?get_inactive=notabool", None).await;
    assert_eq!(
        status, 400,
        "a malformed bool must be rejected: body={body}"
    );
}

for_each_runtime!(go_view_paths, go_view_paths_test);
