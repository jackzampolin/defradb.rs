use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root")
}

fn defra_binary() -> PathBuf {
    workspace_root().join("target/debug/defra")
}

fn defra_keyring(binary: &Path, keyring_dir: &Path, args: &[&str]) -> Output {
    Command::new(binary)
        .arg("--keyring-backend")
        .arg("file")
        .arg("--keyring-path")
        .arg(keyring_dir)
        .arg("keyring")
        .args(args)
        .env("DEFRA_KEYRING_SECRET", "test-secret")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run defra binary")
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_requires_development(output: &Output) {
    assert!(
        !output.status.success(),
        "command should fail without development mode"
    );
    assert!(
        combined_output(output)
            .contains("operation not permitted whilst development mode is disabled"),
        "unexpected error: {}",
        combined_output(output)
    );
}

#[test]
fn rust_add_and_get_require_development_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let binary = defra_binary();
    let hex_key = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

    let out = defra_keyring(&binary, kr, &["add", "dev-key", hex_key]);
    assert_requires_development(&out);

    let out = Command::new(&binary)
        .arg("--development")
        .arg("--keyring-backend")
        .arg("file")
        .arg("--keyring-path")
        .arg(kr)
        .arg("keyring")
        .arg("new")
        .env("DEFRA_KEYRING_SECRET", "test-secret")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run defra binary");
    assert!(
        out.status.success(),
        "new failed: {}",
        combined_output(&out)
    );

    let out = defra_keyring(&binary, kr, &["get", "peer-key"]);
    assert_requires_development(&out);
}
