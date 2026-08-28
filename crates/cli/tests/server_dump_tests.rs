use std::process::Command;
use std::sync::Arc;

use db::database::DB;
use keyring::{FileKeyring, Keyring, ENCRYPTION_KEY, KEYRING_SECRET_ENV};
use storage::encrypted_store::EncryptedStore;

#[tokio::test]
async fn server_dump_reads_encrypted_regolith_store() {
    let root = tempfile::tempdir().unwrap();
    let secret = "server-dump-test-secret";
    let encryption_key = [42; 32];

    let mut config = cli::config::Config {
        rootdir: root.path().to_path_buf(),
        ..Default::default()
    };
    config.datastore.at_rest_encryption = true;

    let keyring = FileKeyring::open(config.keyring_path(), secret.as_bytes()).unwrap();
    keyring.set(ENCRYPTION_KEY, &encryption_key).unwrap();

    std::fs::write(
        root.path().join("config.yaml"),
        serde_yaml::to_string(&config).unwrap(),
    )
    .unwrap();

    let expected = {
        let backend = storage::RegolithStore::open(config.data_path()).unwrap();
        let store = Arc::new(EncryptedStore::new(backend, encryption_key));
        let database = DB::open_from_arc(store).await.unwrap();
        let collections = query::parse_sdl("type Users { name: String }").unwrap();
        database
            .create_collections_atomic(collections)
            .await
            .unwrap();
        database.print_dump().await.unwrap()
    };

    let output = Command::new(env!("CARGO_BIN_EXE_defra"))
        .args(["--rootdir"])
        .arg(root.path())
        .arg("server-dump")
        .env(KEYRING_SECRET_ENV, secret)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "server-dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        expected
    );
}
