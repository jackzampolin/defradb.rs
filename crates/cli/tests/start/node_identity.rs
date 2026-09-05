use super::*;
use identity::Identity;
use keyring::Keyring;

#[test]
fn keyring_identity_roundtrips_all_supported_types_without_rotation() {
    for name in ["ed25519", "secp256k1", "secp256r1"] {
        let root = tempfile::tempdir().unwrap();
        let keyring = keyring::FileKeyring::open(root.path(), b"test-secret").unwrap();
        let first = load_or_create(&keyring, name).unwrap();
        assert_eq!(first.identity_key_type().to_string(), name);
        let encoded = keyring.get(NODE_IDENTITY_KEY).unwrap();
        assert!(encoded.starts_with(format!("{name}:").as_bytes()));

        let reopened = keyring::FileKeyring::open(root.path(), b"test-secret").unwrap();
        let loaded = load_or_create(&reopened, "not-a-key-type").unwrap();
        assert_eq!(loaded.did().unwrap(), first.did().unwrap());
        assert_eq!(reopened.get(NODE_IDENTITY_KEY).unwrap(), encoded);
    }
}

#[test]
fn legacy_key_is_migrated_without_changing_its_identity() {
    let root = tempfile::tempdir().unwrap();
    let keyring = keyring::FileKeyring::open(root.path(), b"test-secret").unwrap();
    let mut bytes = [1; 32];
    bytes[4] = b':';
    let expected = RawIdentity::from_identity_key_type(IdentityKeyType::Secp256k1, &bytes).unwrap();
    keyring.set(NODE_IDENTITY_KEY, &bytes).unwrap();

    let loaded = load_or_create(&keyring, "ed25519").unwrap();

    assert_eq!(loaded.did().unwrap(), expected.did().unwrap());
    let encoded = keyring.get(NODE_IDENTITY_KEY).unwrap();
    assert!(encoded.starts_with(b"secp256k1:"));
    assert_eq!(&encoded[b"secp256k1:".len()..], &bytes);
}

#[test]
fn invalid_stored_identity_is_not_replaced_or_exposed() {
    let root = tempfile::tempdir().unwrap();
    let keyring = keyring::FileKeyring::open(root.path(), b"test-secret").unwrap();
    for bytes in [
        b"ed25519:private-material".as_slice(),
        b"private-material:unknown",
        b"private-material",
        b"",
        &[0; 32],
    ] {
        keyring.set(NODE_IDENTITY_KEY, bytes).unwrap();
        let error = load_or_create(&keyring, "secp256k1").unwrap_err();
        assert!(!error.to_string().contains("private-material"));
        assert_eq!(keyring.get(NODE_IDENTITY_KEY).unwrap().as_slice(), bytes);
    }
}

#[test]
fn invalid_default_type_does_not_write_a_key() {
    let root = tempfile::tempdir().unwrap();
    let keyring = keyring::FileKeyring::open(root.path(), b"test-secret").unwrap();
    assert!(matches!(
        load_or_create(&keyring, "aes256"),
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        keyring.get(NODE_IDENTITY_KEY),
        Err(keyring::Error::NotFound(_))
    ));
}

#[test]
fn explicit_identity_takes_precedence_without_opening_the_keyring() {
    let mut config = Config::default();
    config.datastore.default_key_type = "invalid".into();
    let explicit = Arc::new(generate("secp256r1").unwrap());

    let loaded = resolve(&config, Some(explicit.clone())).unwrap().unwrap();

    assert!(Arc::ptr_eq(&loaded, &explicit));
}

#[test]
fn disabled_keyring_only_generates_an_identity_in_development() {
    let mut config = Config::default();
    config.keyring.disabled = true;
    assert!(resolve(&config, None).unwrap().is_none());

    config.development = true;
    config.datastore.default_key_type = "ed25519".into();
    let first = resolve(&config, None).unwrap().unwrap();
    let second = resolve(&config, None).unwrap().unwrap();
    assert_eq!(first.identity_key_type(), IdentityKeyType::Ed25519);
    assert_ne!(first.did().unwrap(), second.did().unwrap());
}
