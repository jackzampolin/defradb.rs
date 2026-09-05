//! Exercise TLS configuration through the running CLI, not just the config fields.

use std::path::Path;
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use reqwest::{Certificate, Client, StatusCode};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

struct RunningNode {
    child: Child,
    root: TempDir,
    address: String,
}

impl RunningNode {
    fn start(root: TempDir, tls: bool) -> Self {
        let port = portpicker::pick_unused_port().expect("available port");
        let address = format!("127.0.0.1:{port}");
        let log = std::fs::File::create(root.path().join("node.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_defra"));
        command
            .args([
                "start",
                "--no-keyring",
                "--no-p2p",
                "--store",
                "memory",
                "--url",
            ])
            .arg(&address)
            .arg("--rootdir")
            .arg(root.path())
            .env("RUST_LOG", "info")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true);
        if tls {
            command
                .arg("--pubkeypath")
                .arg(root.path().join("cert.pem"))
                .arg("--privkeypath")
                .arg(root.path().join("key.pem"));
        }
        Self {
            child: command.spawn().unwrap(),
            root,
            address,
        }
    }

    fn logs(&self) -> String {
        std::fs::read_to_string(self.root.path().join("node.log")).unwrap()
    }

    async fn healthy(&mut self, client: &Client, scheme: &str) {
        let url = format!("{scheme}://{}/health-check", self.address);
        let result = timeout(Duration::from_secs(20), async {
            loop {
                assert!(self.child.try_wait().unwrap().is_none(), "{}", self.logs());
                if let Ok(response) = client.get(&url).send().await {
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(response.json::<String>().await.unwrap(), "Healthy");
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(result.is_ok(), "node not healthy at {url}: {}", self.logs());
    }

    async fn stop(&mut self) {
        self.child.kill().await.unwrap();
    }
}

fn write_certificate(root: &Path) -> Certificate {
    let certified = generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    std::fs::write(root.join("cert.pem"), certified.cert.pem()).unwrap();
    std::fs::write(root.join("key.pem"), certified.key_pair.serialize_pem()).unwrap();
    Certificate::from_der(certified.cert.der()).unwrap()
}

fn client(certificate: Option<Certificate>) -> Client {
    let mut builder = Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .timeout(Duration::from_secs(2));
    if let Some(certificate) = certificate {
        builder = builder.add_root_certificate(certificate);
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn certificate_flags_serve_https_and_reject_plaintext() {
    let root = tempfile::tempdir().unwrap();
    let certificate = write_certificate(root.path());
    let trusted = client(Some(certificate.clone()));
    let untrusted = client(None);
    let mut node = RunningNode::start(root, true);
    node.healthy(&trusted, "https").await;

    let response = trusted
        .get(format!("https://{}/health-check", node.address))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), reqwest::Version::HTTP_2);
    assert_eq!(response.status(), StatusCode::OK);

    assert!(untrusted
        .get(format!("https://{}/health-check", node.address))
        .send()
        .await
        .is_err());
    assert!(trusted
        .get(format!("http://{}/health-check", node.address))
        .send()
        .await
        .is_err());
    // Failed handshakes must not kill the listener or block other clients.
    let _idle_connection = tokio::net::TcpStream::connect(&node.address).await.unwrap();
    node.healthy(&client(Some(certificate)), "https").await;
    assert!(node.logs().contains(&format!("https://{}", node.address)));
    node.stop().await;
}

#[tokio::test]
async fn node_shutdown_closes_existing_https_connections() {
    let root = tempfile::tempdir().unwrap();
    let certificate = write_certificate(root.path());
    let mut config = cli::config::Config {
        rootdir: root.path().to_path_buf(),
        ..Default::default()
    };
    config.datastore.store = cli::config::DatastoreType::Memory;
    config.net.p2p_disabled = true;
    config.keyring.disabled = true;
    config.api.address = format!(
        "127.0.0.1:{}",
        portpicker::pick_unused_port().expect("available port")
    );
    config.api.pubkey_path = root.path().join("cert.pem").display().to_string();
    config.api.privkey_path = root.path().join("key.pem").display().to_string();
    let url = format!("https://{}/health-check", config.api.address);
    let client = Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .add_root_certificate(certificate)
        .http1_only()
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .max_tls_version(reqwest::tls::Version::TLS_1_2)
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let node = cli::commands::Node::new(config, None).await.unwrap();
    let shutdown = node.shutdown_tx.clone();
    let task = tokio::spawn(node.run());
    timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get(&url).send().await {
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(response.version(), reqwest::Version::HTTP_11);
                assert_eq!(response.json::<String>().await.unwrap(), "Healthy");
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    shutdown.send(()).await.unwrap();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The client retains its keep-alive connection after consuming the body.
    assert!(client.get(&url).send().await.is_err());
}

#[tokio::test]
async fn absent_certificate_flags_keep_plain_http() {
    let mut node = RunningNode::start(tempfile::tempdir().unwrap(), false);
    node.healthy(&client(None), "http").await;
    node.stop().await;
}

#[tokio::test]
async fn config_file_enables_tls_with_root_relative_paths() {
    let root = tempfile::tempdir().unwrap();
    let certificate = write_certificate(root.path());
    let mut config = cli::config::Config::default();
    config.api.pubkey_path = "cert.pem".into();
    config.api.privkey_path = "key.pem".into();
    std::fs::write(
        root.path().join("config.yaml"),
        serde_yaml::to_string(&config).unwrap(),
    )
    .unwrap();
    let mut node = RunningNode::start(root, false);
    node.healthy(&client(Some(certificate)), "https").await;
    node.stop().await;
}

#[tokio::test]
async fn invalid_tls_configuration_fails_before_serving() {
    for case in ["missing", "bad certificate", "bad key", "mismatched key"] {
        let root = tempfile::tempdir().unwrap();
        write_certificate(root.path());
        match case {
            "missing" => std::fs::remove_file(root.path().join("cert.pem")).unwrap(),
            "bad certificate" => {
                std::fs::write(root.path().join("cert.pem"), "not a certificate").unwrap();
            }
            "bad key" => {
                std::fs::write(root.path().join("key.pem"), "not a private key").unwrap();
            }
            "mismatched key" => {
                let other = generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
                std::fs::write(root.path().join("key.pem"), other.key_pair.serialize_pem())
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let mut node = RunningNode::start(root, true);
        let status = timeout(Duration::from_secs(10), node.child.wait())
            .await
            .unwrap_or_else(|_| panic!("{case}: invalid TLS allowed startup: {}", node.logs()))
            .unwrap();
        assert!(!status.success(), "{case}: {}", node.logs());
        assert!(node.logs().contains("TLS"), "{case}: {}", node.logs());
        assert!(
            !node.logs().contains("DefraDB node started"),
            "{}",
            node.logs()
        );
    }
}
