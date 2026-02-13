use std::path::PathBuf;
use std::process::{Command, Stdio};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root")
}

fn defra_binary() -> PathBuf {
    workspace_root().join("target/debug/defra")
}

/// Run a `defra identity` subcommand with the file keyring pointed at `keyring_dir`.
fn defra_identity(keyring_dir: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(defra_binary())
        .arg("--keyring-backend")
        .arg("file")
        .arg("--keyring-path")
        .arg(keyring_dir)
        .arg("identity")
        .args(args)
        .env("DEFRA_KEYRING_SECRET", "test-secret")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run defra binary")
}

/// Pipe `input` into a `defra identity` subcommand via stdin.
fn defra_identity_stdin(
    keyring_dir: &std::path::Path,
    args: &[&str],
    input: &str,
) -> std::process::Output {
    use std::io::Write;
    let mut child = Command::new(defra_binary())
        .arg("--keyring-backend")
        .arg("file")
        .arg("--keyring-path")
        .arg(keyring_dir)
        .arg("identity")
        .args(args)
        .env("DEFRA_KEYRING_SECRET", "test-secret")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn defra binary");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// Extract the "did" field from a JWK JSON printed on stdout.
fn did_from_jwk_stdout(output: &std::process::Output) -> String {
    let text = stdout(output);
    let jwk: serde_json::Value = serde_json::from_str(text.trim()).expect("not valid JSON");
    jwk["did"]
        .as_str()
        .expect("missing 'did' field")
        .to_string()
}

#[test]
#[ignore] // Run with: cargo test -p integration-test --test identity_lifecycle -- --ignored
fn new_output_key_delete_reimport_secp256k1() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    // new --name a --output-key → JWK with DID
    let out = defra_identity(kr, &["new", "--name", "a", "--output-key"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));
    let did1 = did_from_jwk_stdout(&out);
    assert!(did1.starts_with("did:key:z"), "unexpected DID: {}", did1);
    let jwk_text = stdout(&out);

    // delete --name a
    let out = defra_identity(kr, &["delete", "--name", "a"]);
    assert!(out.status.success(), "delete failed: {}", stderr(&out));

    // import --name a --stdin (pipe the JWK back)
    let out = defra_identity_stdin(kr, &["import", "--name", "a", "--stdin"], &jwk_text);
    assert!(out.status.success(), "import failed: {}", stderr(&out));
    let did2_text = stdout(&out);
    assert!(
        did2_text.contains(&did1),
        "DID mismatch after reimport: got '{}', expected '{}'",
        did2_text.trim(),
        did1
    );
}

#[test]
#[ignore]
fn new_output_key_delete_reimport_ed25519() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_identity(
        kr,
        &["new", "--name", "b", "--output-key", "--type", "ed25519"],
    );
    assert!(out.status.success(), "new failed: {}", stderr(&out));
    let did1 = did_from_jwk_stdout(&out);
    let jwk_text = stdout(&out);

    let out = defra_identity(kr, &["delete", "--name", "b"]);
    assert!(out.status.success(), "delete failed: {}", stderr(&out));

    let out = defra_identity_stdin(kr, &["import", "--name", "b", "--stdin"], &jwk_text);
    assert!(out.status.success(), "import failed: {}", stderr(&out));
    let did2_text = stdout(&out);
    assert!(
        did2_text.contains(&did1),
        "DID mismatch after reimport: got '{}', expected '{}'",
        did2_text.trim(),
        did1
    );
}

#[test]
#[ignore]
fn export_delete_reimport_preserves_did() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    // new --name c
    let out = defra_identity(kr, &["new", "--name", "c"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));
    let did1_text = stdout(&out);
    // DID: did:key:z...
    let did1 = did1_text
        .trim()
        .strip_prefix("DID: ")
        .expect("unexpected output format");

    // export --name c
    let out = defra_identity(kr, &["export", "--name", "c"]);
    assert!(out.status.success(), "export failed: {}", stderr(&out));
    let jwk_text = stdout(&out);
    let export_did = did_from_jwk_stdout(&out);
    assert_eq!(did1, export_did, "export DID should match new DID");

    // delete --name c
    let out = defra_identity(kr, &["delete", "--name", "c"]);
    assert!(out.status.success(), "delete failed: {}", stderr(&out));

    // import --name c --stdin
    let out = defra_identity_stdin(kr, &["import", "--name", "c", "--stdin"], &jwk_text);
    assert!(out.status.success(), "import failed: {}", stderr(&out));
    let did2_text = stdout(&out);
    assert!(
        did2_text.contains(did1),
        "DID mismatch: got '{}', expected '{}'",
        did2_text.trim(),
        did1
    );
}

#[test]
#[ignore]
fn delete_removes_key() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_identity(kr, &["new", "--name", "d"]);
    assert!(out.status.success());

    let out = defra_identity(kr, &["delete", "--name", "d"]);
    assert!(out.status.success());

    // export should fail — key no longer exists
    let out = defra_identity(kr, &["export", "--name", "d"]);
    assert!(!out.status.success(), "export should fail after delete");
    let err = stderr(&out);
    assert!(
        err.contains("not found") || err.contains("NotFound"),
        "unexpected error: {}",
        err
    );
}

#[test]
#[ignore]
fn import_rejects_malformed_json() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_identity_stdin(kr, &["import", "--name", "x", "--stdin"], "not json");
    assert!(!out.status.success());
    assert!(stderr(&out).contains("invalid JWK JSON"));
}

#[test]
#[ignore]
fn import_rejects_missing_d_field() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let jwk = r#"{"kty":"EC","crv":"secp256k1"}"#;
    let out = defra_identity_stdin(kr, &["import", "--name", "x", "--stdin"], jwk);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("missing 'd' field"));
}

#[test]
#[ignore]
fn import_rejects_wrong_curve() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let jwk = r#"{"kty":"EC","crv":"P-256","d":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}"#;
    let out = defra_identity_stdin(kr, &["import", "--name", "x", "--stdin"], jwk);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("unsupported JWK curve"));
}
