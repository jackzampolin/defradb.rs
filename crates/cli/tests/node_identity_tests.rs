use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crypto::KeyType;
use serde_json::Value;

struct RunningNode {
    child: Child,
    url: String,
    client: reqwest::Client,
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl RunningNode {
    async fn start(root: &Path, key_type: &str, flags: &[&str]) -> Self {
        let port = portpicker::pick_unused_port().expect("unused HTTP port");
        let address = format!("127.0.0.1:{port}");
        let log_path = root.join("node.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_defra"))
            .arg("--rootdir")
            .arg(root)
            .args(["--url", &address])
            .args([
                "start",
                "--store",
                "memory",
                "--no-p2p",
                "--default-key-type",
                key_type,
            ])
            .args(flags)
            .env("DEFRA_KEYRING_SECRET", "node-identity-test-secret")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("start node");
        let mut node = Self {
            child,
            url: format!("http://{address}"),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        };
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    node.child.try_wait().unwrap().is_none(),
                    "node exited during startup: {}",
                    std::fs::read_to_string(&log_path).unwrap_or_default()
                );
                if let Ok(response) = node
                    .client
                    .get(format!("{}/health-check", node.url))
                    .send()
                    .await
                {
                    if response.status().is_success() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("node did not become healthy");
        node
    }

    async fn identity(&self) -> Value {
        self.client
            .get(format!("{}/api/v0/node/identity", self.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn query(&self, query: &str) -> Value {
        let result: Value = self
            .client
            .post(format!("{}/api/v0/graphql", self.url))
            .json(&serde_json::json!({"query": query}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(result["errors"].is_null(), "query failed: {result}");
        result["data"].clone()
    }
}

#[tokio::test]
async fn development_node_uses_the_requested_identity_key_type_without_p2p() {
    for (name, expected) in [
        ("ed25519", KeyType::Ed25519),
        ("secp256k1", KeyType::Secp256k1),
        ("secp256r1", KeyType::Secp256r1),
    ] {
        let root = tempfile::tempdir().unwrap();
        let node = RunningNode::start(root.path(), name, &["--development", "--no-keyring"]).await;
        let response = node.identity().await;
        let did = response["DID"].as_str().expect("generated node identity");
        assert_eq!(crypto::parse_did_key(did).unwrap().0, expected);
        assert!(response["PeerID"].is_null());
    }
}

#[tokio::test]
async fn persisted_node_identity_survives_a_default_key_type_change() {
    let root = tempfile::tempdir().unwrap();
    let node = RunningNode::start(root.path(), "secp256r1", &[]).await;
    let before = node.identity().await;
    let did = before["DID"].as_str().expect("persisted node identity");
    assert_eq!(crypto::parse_did_key(did).unwrap().0, KeyType::Secp256r1);
    drop(node);

    let restarted = RunningNode::start(root.path(), "ed25519", &[]).await;
    assert_eq!(restarted.identity().await["DID"], before["DID"]);
}

#[tokio::test]
async fn generated_identity_signs_commits_unless_signing_is_disabled() {
    for no_signing in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut flags = vec!["--development", "--no-keyring"];
        if no_signing {
            flags.push("--no-signing");
        }
        let node = RunningNode::start(root.path(), "secp256k1", &flags).await;
        let identity = node.identity().await;
        let did = identity["DID"].as_str().unwrap();
        let (key_type, public_bytes) = crypto::parse_did_key(did).unwrap();
        let public_key = crypto::public_key_from_bytes(key_type, &public_bytes).unwrap();
        node.client
            .post(format!("{}/api/v0/schema", node.url))
            .body("type Users { name: String }")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        node.query(r#"mutation { add_Users(input: {name: "Alice"}) { _docID } }"#)
            .await;
        let result = node
            .query("query { _commits { signature { identity } } }")
            .await;
        let commits = result["_commits"].as_array().unwrap();
        assert!(!commits.is_empty());
        let signed: Vec<_> = commits
            .iter()
            .filter(|commit| !commit["signature"].is_null())
            .collect();
        if no_signing {
            assert!(signed.is_empty(), "--no-signing was ignored: {result}");
        } else {
            assert!(
                !signed.is_empty(),
                "generated identity did not sign: {result}"
            );
            for commit in signed {
                assert_eq!(
                    commit["signature"]["identity"],
                    hex::encode(public_key.raw())
                );
            }
        }
        let options: Value = node
            .client
            .get(format!("{}/api/v0/node/options", node.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(options["DB"]["Identity"], "<redacted>");
        assert_eq!(options["DB"]["EnableSigning"], !no_signing);
    }
}

#[tokio::test]
async fn generated_identity_does_not_authenticate_http_requests() {
    let root = tempfile::tempdir().unwrap();
    let node =
        RunningNode::start(root.path(), "secp256k1", &["--development", "--no-keyring"]).await;
    assert!(node.identity().await["DID"].is_string());

    let response = node
        .client
        .post(format!("{}/api/v0/acp/document/policy", node.url))
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("policy creator can not be empty"));
}

#[tokio::test]
async fn explicit_identity_overrides_the_generation_default() {
    use crypto::Key;
    use identity::Identity;

    let root = tempfile::tempdir().unwrap();
    let key = crypto::generate_ed25519().unwrap();
    let key_hex = hex::encode(key.raw());
    let expected = identity::RawIdentity::from_private_key(key)
        .unwrap()
        .did()
        .unwrap();
    let node = RunningNode::start(
        root.path(),
        "invalid",
        &["--no-keyring", "--identity", &key_hex],
    )
    .await;
    assert_eq!(node.identity().await["DID"], expected.as_str());
}

#[tokio::test]
async fn production_without_keyring_or_explicit_identity_stays_anonymous() {
    let root = tempfile::tempdir().unwrap();
    let node = RunningNode::start(root.path(), "ed25519", &["--no-keyring"]).await;
    assert!(node.identity().await.get("DID").is_none());
}
