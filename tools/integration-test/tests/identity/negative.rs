use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use integration_test::{
    for_each_runtime, generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

/// Build a secp256k1 JWT with expired `exp` (120 seconds in the past).
///
/// Clock skew tolerance is 60 seconds, so 120 seconds past guarantees rejection.
/// The token is cryptographically valid (signature covers the expired claims),
/// meaning the server must actually verify expiry, not just the signature.
fn build_expired_secp256k1_jwt(audience: &str) -> String {
    use crypto::generate_secp256k1;
    use identity::{FullIdentity, Identity, RawIdentity};

    let private_key = generate_secp256k1().expect("generate secp256k1 key");
    let identity = RawIdentity::from_secp256k1(private_key).expect("create identity");
    let did = identity.did().expect("derive DID");
    let pub_key_hex = hex::encode(identity.public_key_bytes());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs();

    // exp is 120 seconds in the past — beyond the 60s clock skew tolerance
    let exp = now.saturating_sub(120);

    let header = serde_json::json!({"alg": "ES256K", "typ": "JWT"});
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());

    let claims = serde_json::json!({
        "sub": pub_key_hex,
        "iss": did.to_string(),
        "exp": exp,
        "nbf": now.saturating_sub(130),
        "iat": now.saturating_sub(130),
        "aud": [audience],
        "key_type": "secp256k1",
    });
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

    let signing_input = format!("{}.{}", header_b64, claims_b64);
    let der_sig = identity
        .sign(signing_input.as_bytes())
        .expect("sign expired claims");

    let raw_sig = der_to_raw_secp256k1(&der_sig).expect("DER to raw");
    let sig_b64 = URL_SAFE_NO_PAD.encode(&raw_sig);

    format!("{}.{}", signing_input, sig_b64)
}

/// Convert DER-encoded ECDSA signature to raw R||S (64 bytes) for ES256K JWT format.
fn der_to_raw_secp256k1(der: &[u8]) -> Option<Vec<u8>> {
    if der.len() < 8 || der[0] != 0x30 {
        return None;
    }
    let mut pos = 2usize;
    if der[1] & 0x80 != 0 {
        pos += (der[1] & 0x7f) as usize;
    }
    if pos >= der.len() || der[pos] != 0x02 {
        return None;
    }
    pos += 1;
    let r_len = der[pos] as usize;
    pos += 1;
    let r = &der[pos..pos + r_len];
    pos += r_len;
    if pos >= der.len() || der[pos] != 0x02 {
        return None;
    }
    pos += 1;
    let s_len = der[pos] as usize;
    pos += 1;
    let s = &der[pos..pos + s_len];

    let mut r = r;
    let mut s = s;
    while r.len() > 32 && r[0] == 0 {
        r = &r[1..];
    }
    while s.len() > 32 && s[0] == 0 {
        s = &s[1..];
    }

    let mut result = vec![0u8; 64];
    let r_offset = 32 - r.len().min(32);
    let s_offset = 32 - s.len().min(32);
    result[r_offset..32].copy_from_slice(&r[..r.len().min(32)]);
    result[32 + s_offset..64].copy_from_slice(&s[..s.len().min(32)]);
    Some(result)
}

/// Send a GraphQL query with a raw Authorization header value and return the HTTP status.
async fn graphql_with_raw_auth(api_url: &str, auth_header: &str) -> u16 {
    let url = format!("{}/api/v0/graphql", api_url);
    let body = serde_json::json!({"query": "query { __typename }"});
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("Authorization", auth_header)
        .json(&body)
        .send()
        .await
        .expect("HTTP request failed");
    resp.status().as_u16()
}

// ---------------------------------------------------------------------------
// Test: expired token is rejected with 403
// ---------------------------------------------------------------------------

async fn expired_token_rejected_test(cluster: TestCluster) {
    let api_url = cluster.api_url(0);

    // Strip http:// to get the bare host:port for audience (matches server behavior)
    let audience = api_url
        .strip_prefix("https://")
        .or_else(|| api_url.strip_prefix("http://"))
        .unwrap_or(api_url);

    let expired_jwt = build_expired_secp256k1_jwt(audience);
    let status = graphql_with_raw_auth(api_url, &format!("Bearer {}", expired_jwt)).await;

    assert_eq!(
        status, 403,
        "expired JWT must be rejected with 403, got {}",
        status
    );
}

for_each_runtime!(expired_token_rejected, expired_token_rejected_test);

// ---------------------------------------------------------------------------
// Test: malformed token strings are rejected with 403
// ---------------------------------------------------------------------------

async fn malformed_token_rejected_test(cluster: TestCluster) {
    let api_url = cluster.api_url(0);

    let cases: &[(&str, &str)] = &[
        ("not-a-jwt", "Bearer not-a-jwt"),
        ("two-parts", "Bearer header.payload"),
        ("garbage-base64", "Bearer !!!.!!!.!!!"),
        ("non-bearer-scheme", "Basic dXNlcjpwYXNz"),
    ];

    for (label, auth_header) in cases {
        let status = graphql_with_raw_auth(api_url, auth_header).await;
        assert_eq!(
            status, 403,
            "malformed token '{}' must yield 403, got {}",
            label, status
        );
    }
}

for_each_runtime!(malformed_token_rejected, malformed_token_rejected_test);

// ---------------------------------------------------------------------------
// Test: no token on ACP-protected collection yields filtered results (not 403)
//
// Anonymous requests are allowed but ACP filters out protected documents.
// ---------------------------------------------------------------------------

async fn unauthenticated_acp_query_filtered_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    node.query_with_identity(
        r#"mutation { add_User(input: {name: "Private", age: 30}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("create document as Alice");

    // Anonymous query: ACP hides Alice's document — result is empty, not 403
    let anon_result = node
        .query("query { User { name } }")
        .expect("anonymous query should succeed");

    let users = anon_result["User"].as_array().expect("User array");
    assert_eq!(
        users.len(),
        0,
        "anonymous user must see 0 ACP-protected documents, got {}",
        users.len()
    );
}

for_each_runtime!(
    unauthenticated_acp_query_filtered,
    unauthenticated_acp_query_filtered_test,
    .with_acp_local()
);

// ---------------------------------------------------------------------------
// Test: identity isolation — Bob cannot read or write Alice's documents
// ---------------------------------------------------------------------------

async fn identity_isolation_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    let create_result = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "AliceDoc", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create Alice document");
    let doc_id = create_result["add_User"][0]["_docID"]
        .as_str()
        .expect("doc_id");

    // Bob queries — must see nothing (identity isolation via ACP)
    let bob_read = node
        .query_with_identity("query { User { name age } }", &bob.private_key_hex)
        .expect("Bob read query");
    let bob_users = bob_read["User"].as_array().expect("Bob User array");
    assert_eq!(
        bob_users.len(),
        0,
        "Bob must see 0 of Alice's ACP-protected documents"
    );

    // Bob attempts to update Alice's document — must see no affected rows
    let update_mutation = format!(
        r#"mutation {{ update_User(docID: "{}", input: {{name: "BobHijacked"}}) {{ _docID name }} }}"#,
        doc_id
    );
    let bob_update = node
        .query_with_identity(&update_mutation, &bob.private_key_hex)
        .expect("Bob update query");
    let updated = bob_update["update_User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        updated, 0,
        "Bob must not be able to update Alice's document"
    );

    // Alice still owns her document unchanged
    let alice_read = node
        .query_with_identity("query { User { name age } }", &alice.private_key_hex)
        .expect("Alice read query");
    let alice_users = alice_read["User"].as_array().expect("Alice User array");
    assert_eq!(alice_users.len(), 1, "Alice must still see her document");
    assert_eq!(
        alice_users[0]["name"], "AliceDoc",
        "Alice's document name must be unchanged"
    );
}

for_each_runtime!(identity_isolation, identity_isolation_test, .with_acp_local());
