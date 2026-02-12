//! Tests for systemd-creds keyring backend

#![cfg(target_os = "linux")]

use keyring::{Error, Keyring, SystemdCredsKeyring};

#[test]
#[ignore]
fn test_systemd_creds_set_get_roundtrip() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    let data = b"my-secret-key-data";
    kr.set("test-key", data).unwrap();

    let retrieved = kr.get("test-key").unwrap();
    assert_eq!(retrieved, data);
}

#[test]
#[ignore]
fn test_systemd_creds_get_not_found() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    let result = kr.get("nonexistent");
    assert!(matches!(result, Err(Error::NotFound(_))));
}

#[test]
#[ignore]
fn test_systemd_creds_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    kr.set("to-delete", b"some-data").unwrap();
    assert!(kr.get("to-delete").is_ok());

    kr.delete("to-delete").unwrap();
    assert!(matches!(kr.get("to-delete"), Err(Error::NotFound(_))));
}

#[test]
#[ignore]
fn test_systemd_creds_delete_not_found() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    let result = kr.delete("nonexistent");
    assert!(matches!(result, Err(Error::NotFound(_))));
}

#[test]
#[ignore]
fn test_systemd_creds_list() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    kr.set("key-a", b"data-a").unwrap();
    kr.set("key-b", b"data-b").unwrap();
    kr.set("key-c", b"data-c").unwrap();

    let mut keys = kr.list().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["key-a", "key-b", "key-c"]);
}

#[test]
#[ignore]
fn test_systemd_creds_list_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    let keys = kr.list().unwrap();
    assert!(keys.is_empty());
}

#[test]
#[ignore]
fn test_systemd_creds_list_ignores_non_cred_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    kr.set("real-key", b"data").unwrap();

    // Create non-.cred files in the directory
    std::fs::write(temp_dir.path().join("stray.txt"), b"not a cred").unwrap();
    std::fs::write(temp_dir.path().join(".hidden"), b"hidden").unwrap();

    let keys = kr.list().unwrap();
    assert_eq!(keys, vec!["real-key"]);
}

#[test]
#[ignore]
fn test_systemd_creds_overwrite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    kr.set("key", b"original").unwrap();
    kr.set("key", b"updated").unwrap();

    let retrieved = kr.get("key").unwrap();
    assert_eq!(retrieved, b"updated");
}

#[test]
#[ignore]
fn test_systemd_creds_binary_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let kr = SystemdCredsKeyring::open(temp_dir.path()).unwrap();

    let binary_data: Vec<u8> = (0u8..=255).collect();
    kr.set("binary-key", &binary_data).unwrap();

    let retrieved = kr.get("binary-key").unwrap();
    assert_eq!(retrieved, binary_data);
}
