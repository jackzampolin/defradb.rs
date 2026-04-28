use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cosmrs::Any;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use super::*;

#[derive(Clone, Copy)]
enum ServerMode {
    DelayFirstTx,
    FailFirstTx,
}

struct RpcState {
    mode: ServerMode,
    account_queries: AtomicUsize,
    broadcasts: AtomicUsize,
    tx_queries: AtomicUsize,
    first_tx_poll_started: Notify,
    first_tx_done: AtomicBool,
    second_broadcast_before_first_tx_done: AtomicBool,
}

impl RpcState {
    fn new(mode: ServerMode) -> Self {
        Self {
            mode,
            account_queries: AtomicUsize::new(0),
            broadcasts: AtomicUsize::new(0),
            tx_queries: AtomicUsize::new(0),
            first_tx_poll_started: Notify::new(),
            first_tx_done: AtomicBool::new(false),
            second_broadcast_before_first_tx_done: AtomicBool::new(false),
        }
    }
}

#[tokio::test]
async fn sign_and_broadcast_holds_sequence_lock_until_inclusion() {
    let state = Arc::new(RpcState::new(ServerMode::DelayFirstTx));
    let base_url = spawn_sourcehub_rpc(state.clone()).await;
    let client = Arc::new(test_client(&base_url));
    let signer = Arc::new(test_signer());

    let first = {
        let client = client.clone();
        let signer = signer.clone();
        tokio::spawn(async move {
            signer
                .sign_and_broadcast(client.as_ref(), vec![test_msg("first")])
                .await
        })
    };

    tokio::time::timeout(
        Duration::from_secs(5),
        state.first_tx_poll_started.notified(),
    )
    .await
    .expect("first tx should start waiting for inclusion");

    let second = {
        let client = client.clone();
        let signer = signer.clone();
        tokio::spawn(async move {
            signer
                .sign_and_broadcast(client.as_ref(), vec![test_msg("second")])
                .await
        })
    };

    first.await.unwrap().expect("first tx should be included");
    second.await.unwrap().expect("second tx should be included");

    assert_eq!(state.account_queries.load(Ordering::SeqCst), 1);
    assert_eq!(state.broadcasts.load(Ordering::SeqCst), 2);
    assert!(!state
        .second_broadcast_before_first_tx_done
        .load(Ordering::SeqCst));
}

#[tokio::test]
async fn sign_and_broadcast_resets_sequence_after_inclusion_failure() {
    let state = Arc::new(RpcState::new(ServerMode::FailFirstTx));
    let base_url = spawn_sourcehub_rpc(state.clone()).await;
    let client = test_client(&base_url);
    let signer = test_signer();

    let first = signer
        .sign_and_broadcast(&client, vec![test_msg("first")])
        .await;

    assert!(matches!(
        first,
        Err(TxSignerError::Broadcast(message))
            if message.contains("tx execution failed")
    ));

    signer
        .sign_and_broadcast(&client, vec![test_msg("second")])
        .await
        .expect("second tx should re-query sequence and succeed");

    assert_eq!(state.account_queries.load(Ordering::SeqCst), 2);
    assert_eq!(state.broadcasts.load(Ordering::SeqCst), 2);
}

async fn spawn_sourcehub_rpc(state: Arc<RpcState>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test rpc should bind");
    let address = listener.local_addr().expect("test rpc should have address");

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let state = state.clone();
            tokio::spawn(async move {
                handle_rpc_connection(stream, state).await;
            });
        }
    });

    format!("http://{address}")
}

async fn handle_rpc_connection(mut stream: TcpStream, state: Arc<RpcState>) {
    let request = read_http_request(&mut stream).await;
    let (method, target) = request_line(&request);
    let (status, body) = rpc_response(&state, method, target).await;
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn read_http_request(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = stream.read(&mut chunk).await.expect("request read");
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..read]);

        let Some(header_end) = find_header_end(&buf) else {
            continue;
        };
        let headers = String::from_utf8_lossy(&buf[..header_end]);
        let content_len = content_length(&headers);
        if buf.len() >= header_end + 4 + content_len {
            break;
        }
    }

    String::from_utf8_lossy(&buf).into_owned()
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or(0)
}

fn request_line(request: &str) -> (&str, &str) {
    let mut parts = request
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    (method, target)
}

async fn rpc_response(state: &RpcState, method: &str, target: &str) -> (&'static str, String) {
    if method == "GET" && target.starts_with("/cosmos/auth/v1beta1/accounts/") {
        let query = state.account_queries.fetch_add(1, Ordering::SeqCst);
        return (
            "200 OK",
            serde_json::json!({
                "account": {
                    "account_number": "7",
                    "sequence": (41 + query).to_string(),
                }
            })
            .to_string(),
        );
    }

    if method == "POST" && target == "/" {
        let broadcast = state.broadcasts.fetch_add(1, Ordering::SeqCst) + 1;
        if broadcast == 2 && !state.first_tx_done.load(Ordering::SeqCst) {
            state
                .second_broadcast_before_first_tx_done
                .store(true, Ordering::SeqCst);
        }
        return (
            "200 OK",
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "code": 0,
                    "hash": format!("HASH{broadcast}"),
                }
            })
            .to_string(),
        );
    }

    if method == "GET" && target.starts_with("/tx?hash=0x") {
        let query = state.tx_queries.fetch_add(1, Ordering::SeqCst) + 1;
        return tx_query_response(state, query).await;
    }

    ("404 Not Found", "{}".to_string())
}

async fn tx_query_response(state: &RpcState, query: usize) -> (&'static str, String) {
    match (state.mode, query) {
        (ServerMode::DelayFirstTx, 1) => {
            state.first_tx_poll_started.notify_waiters();
            tokio::time::sleep(Duration::from_millis(150)).await;
            state.first_tx_done.store(true, Ordering::SeqCst);
            ("200 OK", successful_tx_response())
        }
        (ServerMode::FailFirstTx, 1) => (
            "200 OK",
            serde_json::json!({
                "result": {
                    "tx_result": {
                        "code": 5,
                        "log": "forced failure",
                    }
                }
            })
            .to_string(),
        ),
        _ => {
            state.first_tx_done.store(true, Ordering::SeqCst);
            ("200 OK", successful_tx_response())
        }
    }
}

fn successful_tx_response() -> String {
    serde_json::json!({
        "result": {
            "tx_result": {
                "code": 0,
                "data": "",
                "events": [],
            }
        }
    })
    .to_string()
}

fn test_client(base_url: &str) -> SourceHubClient {
    SourceHubClient::new(
        base_url.to_string(),
        base_url.to_string(),
        Duration::from_secs(5),
    )
    .expect("client should build")
}

fn test_signer() -> TxSigner {
    TxSigner::from_secp256k1_bytes(&[1_u8; 32], "sourcehub-test").expect("signer should build")
}

fn test_msg(name: &str) -> Any {
    Any {
        type_url: format!("/test.{name}"),
        value: Vec::new(),
    }
}
