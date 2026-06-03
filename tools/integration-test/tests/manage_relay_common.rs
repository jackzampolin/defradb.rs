//! Shared helpers for the authenticated P2P management-relay e2e tests
//! (`p2p::manage_relay` over libp2p and `p2p_iroh::connection::manage_relay`).
//!
//! The feature under test: a caller hits node A's HTTP `/api/v0/p2p/manage`
//! (and `/p2p/manage/query`) to make node A relay a signed P2P management
//! request to node B. The caller mints a JWT (`aud` = B's peer-id), node A
//! relays it, and node B authorizes the actor via NAC before applying the op.
//!
//! The harness `DefraClient` is CLI-only (no generic HTTP method), so these
//! endpoints are exercised with a raw `reqwest` POST against `cluster.api_url`.
//! Tokens are minted inline with the `identity` crate, mirroring
//! `cli::mint_manage_token` / `p2p-adapter::manage::auth::mint_token_for`.

#![allow(dead_code)]

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{json, Value};

/// Mint an actor JWT signed by `private_key_hex` with `aud` = `target_peer_id`.
///
/// Mirrors `cli::mint_manage_token`: secp256k1 keys are 32 bytes, ed25519 64.
pub fn mint_manage_token(private_key_hex: &str, target_peer_id: &str) -> String {
    let key_bytes = hex::decode(private_key_hex).expect("identity key is valid hex");
    let key_type = match key_bytes.len() {
        32 => crypto::KeyType::Secp256k1,
        64 => crypto::KeyType::Ed25519,
        len => panic!("unexpected identity key length: {len} bytes"),
    };
    let id = identity::RawIdentity::from_bytes(key_type, &key_bytes)
        .expect("build raw identity from key bytes");
    let token = identity::new_token(
        &id,
        Duration::from_secs(15 * 60),
        Some(target_peer_id.to_string()),
        None,
    )
    .expect("mint manage token");
    String::from_utf8(token).expect("token is valid UTF-8")
}

/// Extract the peer-id portion (after the last `/p2p/`) of a P2P address.
///
/// This is the `aud` value node B checks (its own `local_peer_id`).
pub fn peer_id_of(addr: &str) -> &str {
    match addr.rfind("/p2p/") {
        Some(pos) => &addr[pos + 5..],
        None => addr,
    }
}

/// POST a relayed mutating management op to node A's `/api/v0/p2p/manage`.
///
/// `caller_key` authenticates the HTTP caller against node A's `P2pPeerConnect`
/// gate (Authorization bearer). `auth_token` is the relayed actor JWT.
/// Returns the HTTP status so callers can assert 200 / 403.
pub async fn post_manage(
    api_url: &str,
    caller_key: &str,
    target: &str,
    auth_token: &str,
    op: Value,
) -> StatusCode {
    let body = json!({ "Target": target, "AuthToken": auth_token, "Op": op });
    let token = bearer_token(api_url, caller_key);
    reqwest::Client::new()
        .post(format!("{api_url}/api/v0/p2p/manage"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .expect("POST /p2p/manage")
        .status()
}

/// POST a relayed read-only management query to `/api/v0/p2p/manage/query`.
///
/// Returns `(status, body)`; on success the body deserializes to a
/// `RemoteManageQueryResult` (`{"Kind":"Strings",...}` / `{"Kind":"Replicators",...}`).
pub async fn post_manage_query(
    api_url: &str,
    caller_key: &str,
    target: &str,
    auth_token: &str,
    op: Value,
) -> (StatusCode, Value) {
    let body = json!({ "Target": target, "AuthToken": auth_token, "Op": op });
    let token = bearer_token(api_url, caller_key);
    let resp = reqwest::Client::new()
        .post(format!("{api_url}/api/v0/p2p/manage/query"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .expect("POST /p2p/manage/query");
    let status = resp.status();
    let value = resp.json::<Value>().await.unwrap_or(Value::Null);
    (status, value)
}

/// Mint the HTTP-caller bearer token: the node's HTTP API expects an actor JWT
/// with `aud` = the node's own HTTP host (mirrors the CLI `--identity` path).
fn bearer_token(api_url: &str, caller_key: &str) -> String {
    let host = api_url
        .strip_prefix("http://")
        .or_else(|| api_url.strip_prefix("https://"))
        .unwrap_or(api_url);
    mint_manage_token(caller_key, host)
}
