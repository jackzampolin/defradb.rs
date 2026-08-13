use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn defra_binary() -> PathBuf {
    integration_test::rust_binary()
}

fn is_rust_binary(binary: &Path) -> bool {
    binary.file_name().and_then(|name| name.to_str()) == Some("defra")
}

fn go_binary() -> Option<PathBuf> {
    Command::new("defradb")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()
        .filter(|s| s.success())
        .map(|_| PathBuf::from("defradb"))
}

fn defra_keyring(binary: &Path, keyring_dir: &Path, args: &[&str]) -> Output {
    defra_keyring_with_mode(binary, keyring_dir, args, true)
}

fn defra_keyring_with_mode(
    binary: &Path,
    keyring_dir: &Path,
    args: &[&str],
    add_development_flag: bool,
) -> Output {
    let mut cmd = Command::new(binary);
    cmd.arg("--keyring-backend")
        .arg("file")
        .arg("--keyring-path")
        .arg(keyring_dir);
    if add_development_flag && is_rust_binary(binary) && requires_development(args) {
        cmd.arg("--development");
    }
    cmd.arg("keyring")
        .args(args)
        .env("DEFRA_KEYRING_SECRET", "test-secret")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd.output().expect("failed to run defra binary")
}

fn defra_keyring_stdin(binary: &Path, keyring_dir: &Path, args: &[&str], input: &str) -> Output {
    use std::io::Write;
    let mut cmd = Command::new(binary);
    cmd.arg("--keyring-backend")
        .arg("file")
        .arg("--keyring-path")
        .arg(keyring_dir);
    if is_rust_binary(binary) && requires_development(args) {
        cmd.arg("--development");
    }
    let mut child = cmd
        .arg("keyring")
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

fn requires_development(args: &[&str]) -> bool {
    matches!(args.first().copied(), Some("add" | "get"))
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// Combined stdout + stderr. Go's cobra writes command output to stderr,
/// while Rust writes to stdout. This helper lets shared tests work with both.
fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Extract the first hex string from combined output.
/// Filters out Go's INF log lines and finds the hex-only line.
fn extract_hex_line(output: &Output) -> String {
    let combined = combined_output(output);
    for line in combined.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty()
            && trimmed.chars().all(|c| c.is_ascii_hexdigit())
            && trimmed.len() >= 2
        {
            return trimmed.to_string();
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// Go-compatible tests: shared inner + _rust / _go wrappers
// ---------------------------------------------------------------------------

fn generate_creates_both_keys(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));

    let out = defra_keyring(binary, kr, &["list"]);
    assert!(out.status.success(), "list failed: {}", stderr(&out));
    let list = combined_output(&out);
    assert!(list.contains("peer-key"), "missing peer-key in: {}", list);
    assert!(
        list.contains("encryption-key"),
        "missing encryption-key in: {}",
        list
    );
}

#[test]

fn rust_generate_creates_both_keys() {
    generate_creates_both_keys(&defra_binary());
}

#[test]

fn go_generate_creates_both_keys() {
    let go = go_binary().expect("Go defradb not in PATH");
    generate_creates_both_keys(&go);
}

fn generate_no_encryption(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new", "--no-encryption"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));

    let out = defra_keyring(binary, kr, &["list"]);
    let list = combined_output(&out);
    assert!(list.contains("peer-key"), "missing peer-key in: {}", list);
    assert!(
        !list.contains("encryption-key"),
        "unexpected encryption-key in: {}",
        list
    );
}

#[test]

fn rust_generate_no_encryption() {
    generate_no_encryption(&defra_binary());
}

#[test]

fn go_generate_no_encryption() {
    let go = go_binary().expect("Go defradb not in PATH");
    generate_no_encryption(&go);
}

fn generate_no_peer_key(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new", "--no-peer-key"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));

    let out = defra_keyring(binary, kr, &["list"]);
    let list = combined_output(&out);
    assert!(
        !list.contains("peer-key"),
        "unexpected peer-key in: {}",
        list
    );
    assert!(
        list.contains("encryption-key"),
        "missing encryption-key in: {}",
        list
    );
}

#[test]

fn rust_generate_no_peer_key() {
    generate_no_peer_key(&defra_binary());
}

#[test]

fn go_generate_no_peer_key() {
    let go = go_binary().expect("Go defradb not in PATH");
    generate_no_peer_key(&go);
}

fn generate_fails_if_exists(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new"]);
    assert!(out.status.success(), "first new failed: {}", stderr(&out));

    let out = defra_keyring(binary, kr, &["new"]);
    assert!(
        !out.status.success(),
        "second new should fail but succeeded"
    );
    let err = combined_output(&out);
    assert!(
        err.contains("already exists"),
        "expected 'already exists' in error: {}",
        err
    );
}

#[test]

fn rust_generate_fails_if_exists() {
    generate_fails_if_exists(&defra_binary());
}

#[test]

fn go_generate_fails_if_exists() {
    let go = go_binary().expect("Go defradb not in PATH");
    generate_fails_if_exists(&go);
}

fn generate_force_overwrites(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new"]);
    assert!(out.status.success(), "first new failed: {}", stderr(&out));

    let out = defra_keyring(binary, kr, &["new", "--force"]);
    assert!(out.status.success(), "new --force failed: {}", stderr(&out));
}

#[test]

fn rust_generate_force_overwrites() {
    generate_force_overwrites(&defra_binary());
}

#[test]

fn go_generate_force_overwrites() {
    let go = go_binary().expect("Go defradb not in PATH");
    generate_force_overwrites(&go);
}

fn generate_silent_on_success(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));
    assert!(
        stdout(&out).trim().is_empty(),
        "expected empty stdout, got: '{}'",
        stdout(&out)
    );
}

#[test]

fn rust_generate_silent_on_success() {
    generate_silent_on_success(&defra_binary());
}

#[test]

fn go_generate_silent_on_success() {
    let go = go_binary().expect("Go defradb not in PATH");
    generate_silent_on_success(&go);
}

fn export_hex_format(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    // Add a known hex key
    let known_hex = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233";
    let out = defra_keyring(binary, kr, &["add", "test-key", known_hex]);
    assert!(out.status.success(), "add failed: {}", stderr(&out));

    // Get and verify — Go writes hex to stderr, Rust to stdout
    let out = defra_keyring(binary, kr, &["get", "test-key"]);
    assert!(out.status.success(), "get failed: {}", stderr(&out));
    let hex_out = extract_hex_line(&out);
    assert_eq!(hex_out, known_hex, "exported hex doesn't match imported");
}

#[test]

fn rust_export_hex_format() {
    export_hex_format(&defra_binary());
}

#[test]

fn go_export_hex_format() {
    let go = go_binary().expect("Go defradb not in PATH");
    export_hex_format(&go);
}

fn export_roundtrip(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["new"]);
    assert!(out.status.success(), "new failed: {}", stderr(&out));

    // Get peer-key (Ed25519 = 64 bytes = 128 hex chars)
    let out = defra_keyring(binary, kr, &["get", "peer-key"]);
    assert!(out.status.success(), "get failed: {}", stderr(&out));
    let hex_str = extract_hex_line(&out);
    assert!(
        hex_str.len() == 128,
        "expected 128 hex chars for ed25519, got {} ('{}')",
        hex_str.len(),
        hex_str
    );

    // Get encryption-key (AES-256 = 32 bytes = 64 hex chars)
    let out = defra_keyring(binary, kr, &["get", "encryption-key"]);
    assert!(out.status.success(), "get failed: {}", stderr(&out));
    let hex_str = extract_hex_line(&out);
    assert!(
        hex_str.len() == 64,
        "expected 64 hex chars for aes256, got {} ('{}')",
        hex_str.len(),
        hex_str
    );
}

#[test]

fn rust_export_roundtrip() {
    export_roundtrip(&defra_binary());
}

#[test]

fn go_export_roundtrip() {
    let go = go_binary().expect("Go defradb not in PATH");
    export_roundtrip(&go);
}

fn import_positional_hex(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let hex_key = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
    let out = defra_keyring(binary, kr, &["add", "my-key", hex_key]);
    assert!(out.status.success(), "add failed: {}", stderr(&out));

    let out = defra_keyring(binary, kr, &["get", "my-key"]);
    assert!(out.status.success(), "get failed: {}", stderr(&out));
    let hex_out = extract_hex_line(&out);
    assert_eq!(hex_out, hex_key);
}

#[test]

fn rust_import_positional_hex() {
    import_positional_hex(&defra_binary());
}

#[test]

fn go_import_positional_hex() {
    let go = go_binary().expect("Go defradb not in PATH");
    import_positional_hex(&go);
}

fn import_silent_on_success(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let hex_key = "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd";
    let out = defra_keyring(binary, kr, &["add", "s-key", hex_key]);
    assert!(out.status.success(), "add failed: {}", stderr(&out));
    assert!(
        stdout(&out).trim().is_empty(),
        "expected empty stdout, got: '{}'",
        stdout(&out)
    );
}

#[test]

fn rust_import_silent_on_success() {
    import_silent_on_success(&defra_binary());
}

#[test]

fn go_import_silent_on_success() {
    let go = go_binary().expect("Go defradb not in PATH");
    import_silent_on_success(&go);
}

fn import_invalid_hex_fails(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["add", "bad-key", "ZZZZ"]);
    assert!(!out.status.success(), "add of invalid hex should fail");
}

#[test]

fn rust_import_invalid_hex_fails() {
    import_invalid_hex_fails(&defra_binary());
}

#[test]

fn go_import_invalid_hex_fails() {
    let go = go_binary().expect("Go defradb not in PATH");
    import_invalid_hex_fails(&go);
}

fn list_empty(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let out = defra_keyring(binary, kr, &["list"]);
    assert!(out.status.success(), "list failed: {}", stderr(&out));
    let text = combined_output(&out);
    assert!(
        text.contains("No keys found in the keyring."),
        "expected 'No keys found in the keyring.', got: '{}'",
        text.trim()
    );
}

#[test]

fn rust_list_empty() {
    list_empty(&defra_binary());
}

#[test]

fn go_list_empty() {
    let go = go_binary().expect("Go defradb not in PATH");
    list_empty(&go);
}

fn list_format(binary: &Path) {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();

    let hex_key = "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd";
    let out = defra_keyring(binary, kr, &["add", "alpha", hex_key]);
    assert!(out.status.success());

    let out = defra_keyring(binary, kr, &["list"]);
    assert!(out.status.success());
    let text = combined_output(&out);
    assert!(
        text.contains("Keys in the keyring:"),
        "expected header, got: '{}'",
        text.trim()
    );
    assert!(
        text.contains("- alpha"),
        "expected '- alpha', got: '{}'",
        text.trim()
    );
}

#[test]

fn rust_list_format() {
    list_format(&defra_binary());
}

#[test]

fn go_list_format() {
    let go = go_binary().expect("Go defradb not in PATH");
    list_format(&go);
}

// ---------------------------------------------------------------------------
// Rust-only tests
// ---------------------------------------------------------------------------

#[test]

fn rust_generate_named_key() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let binary = defra_binary();

    let out = defra_keyring(&binary, kr, &["new", "custom-key", "-t", "aes256"]);
    assert!(
        out.status.success(),
        "new named key failed: {}",
        stderr(&out)
    );

    let out = defra_keyring(&binary, kr, &["list"]);
    let list = stdout(&out);
    assert!(
        list.contains("custom-key"),
        "missing custom-key in: {}",
        list
    );
}

#[test]

fn rust_generate_named_key_force() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let binary = defra_binary();

    let out = defra_keyring(&binary, kr, &["new", "my-key", "-t", "ed25519"]);
    assert!(out.status.success());

    // Without --force, should fail
    let out = defra_keyring(&binary, kr, &["new", "my-key", "-t", "ed25519"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("already exists"));

    // With --force, should succeed
    let out = defra_keyring(&binary, kr, &["new", "my-key", "-t", "ed25519", "--force"]);
    assert!(out.status.success(), "new --force failed: {}", stderr(&out));
}

#[test]

fn rust_import_stdin() {
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let binary = defra_binary();

    let hex_key = "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd";
    let out = defra_keyring_stdin(&binary, kr, &["add", "stdin-key", "--stdin"], hex_key);
    assert!(out.status.success(), "add --stdin failed: {}", stderr(&out));

    let out = defra_keyring(&binary, kr, &["get", "stdin-key"]);
    assert!(out.status.success());
    assert_eq!(stdout(&out).trim(), hex_key);
}

// ---------------------------------------------------------------------------
// Cross-binary tests (Rust <-> Go interop)
// ---------------------------------------------------------------------------

#[test]

fn go_rust_import_rust_export_go() {
    let go = go_binary().expect("Go defradb not in PATH");
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let rust = defra_binary();

    let hex_key = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
    let out = defra_keyring(&rust, kr, &["add", "cross-key", hex_key]);
    assert!(out.status.success(), "rust add failed: {}", stderr(&out));

    // Go get writes to stderr
    let out = defra_keyring(&go, kr, &["get", "cross-key"]);
    assert!(out.status.success(), "go get failed: {}", stderr(&out));
    let hex_out = extract_hex_line(&out);
    assert_eq!(hex_out, hex_key, "go export mismatch");
}

#[test]

fn go_rust_import_go_export_rust() {
    let go = go_binary().expect("Go defradb not in PATH");
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let rust = defra_binary();

    let hex_key = "cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe";
    let out = defra_keyring(&go, kr, &["add", "cross-key2", hex_key]);
    assert!(out.status.success(), "go add failed: {}", stderr(&out));

    let out = defra_keyring(&rust, kr, &["get", "cross-key2"]);
    assert!(out.status.success(), "rust get failed: {}", stderr(&out));
    assert_eq!(stdout(&out).trim(), hex_key, "rust export mismatch");
}

#[test]

fn go_rust_generate_go_list_rust() {
    let go = go_binary().expect("Go defradb not in PATH");
    let tmp = tempfile::tempdir().unwrap();
    let kr = tmp.path();
    let rust = defra_binary();

    let out = defra_keyring(&go, kr, &["new"]);
    assert!(out.status.success(), "go new failed: {}", stderr(&out));

    let out = defra_keyring(&rust, kr, &["list"]);
    assert!(out.status.success(), "rust list failed: {}", stderr(&out));
    let list = stdout(&out);
    assert!(list.contains("peer-key"), "missing peer-key in: {}", list);
    assert!(
        list.contains("encryption-key"),
        "missing encryption-key in: {}",
        list
    );
}
