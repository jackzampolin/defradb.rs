//! Backward-compatibility tests for the file keyring's JWE format.
//!
//! `tests/fixtures/golden-keyring-josekit.jwe` is an opaque ciphertext captured
//! from the josekit 0.8.7 implementation. It pins the on-disk format: a keyring
//! written by an older release must stay readable. Any change to key derivation,
//! AAD, or content encryption breaks these tests even when the JWE header is
//! unchanged, which is exactly what `test_jwe_format_go_compatible` cannot catch
//! because it only inspects a token it generated itself.
//!
//! The format is also Go-interop surface: `FileKeyring` is documented as
//! compatible with Go DefraDB's file keyring
//! (`github.com/lestrrat-go/jwx/v2/jwe`, PBES2-HS512+A256KW / A256GCM).
//!
//! **Never regenerate the fixture.** Regenerating it re-encrypts under whatever
//! code is current and destroys the property being tested.

use keyring::{FileKeyring, Keyring};

/// Must match the values used when the fixture was generated.
const FIXTURE_PASSWORD: &[u8] = b"golden-fixture-password-do-not-change";
const FIXTURE_KEY_NAME: &str = "golden-key";
const FIXTURE_KEY_BYTES: &[u8] = b"\x00\x01\x02\x03golden-secret-key-material\xfd\xfe\xff";

/// Trimmed on read so an editor or `end-of-file-fixer` hook appending a newline
/// does not break every test here with a misleading "invalid tag encoding".
fn golden_token() -> &'static str {
    include_str!("fixtures/golden-keyring-josekit.jwe").trim_end()
}

/// Stage a keyring directory containing only the committed golden ciphertext.
fn staged_keyring(password: &[u8]) -> (tempfile::TempDir, FileKeyring) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(FIXTURE_KEY_NAME), golden_token()).unwrap();
    let keyring = FileKeyring::open(dir.path(), password).unwrap();
    (dir, keyring)
}

#[test]
fn golden_keyring_from_previous_release_still_opens() {
    let (_dir, keyring) = staged_keyring(FIXTURE_PASSWORD);

    let got = keyring.get(FIXTURE_KEY_NAME).unwrap();

    assert_eq!(
        &got[..],
        FIXTURE_KEY_BYTES,
        "a keyring written by the josekit implementation must still decrypt"
    );
}

#[test]
fn golden_keyring_rejects_wrong_password() {
    let (_dir, keyring) = staged_keyring(b"not-the-right-password");

    assert!(
        keyring.get(FIXTURE_KEY_NAME).is_err(),
        "decryption must fail on a wrong password rather than returning garbage"
    );
}

/// The direction the golden fixture cannot cover: a token written here must be
/// readable by josekit, the library Go DefraDB's format follows. A self
/// round-trip would pass even if the AAD convention were wrong in a
/// self-consistent way.
#[test]
fn tokens_we_write_are_readable_by_josekit() {
    use josekit::jwe::{self, PBES2_HS512_A256KW};

    let dir = tempfile::tempdir().unwrap();
    let keyring = FileKeyring::open(dir.path(), FIXTURE_PASSWORD).unwrap();
    keyring.set(FIXTURE_KEY_NAME, FIXTURE_KEY_BYTES).unwrap();
    let written = std::fs::read(dir.path().join(FIXTURE_KEY_NAME)).unwrap();
    let token = std::str::from_utf8(&written).unwrap();

    let decrypter = PBES2_HS512_A256KW
        .decrypter_from_bytes(FIXTURE_PASSWORD)
        .unwrap();
    let (plaintext, header) = jwe::deserialize_compact(token, &decrypter)
        .expect("josekit must be able to read a token we wrote");

    assert_eq!(plaintext, FIXTURE_KEY_BYTES);
    assert_eq!(header.algorithm(), Some("PBES2-HS512+A256KW"));
    assert_eq!(header.content_encryption(), Some("A256GCM"));
}

/// Rebuild a token with `mutate` applied to its protected header. The header is
/// the AEAD's AAD, so these tokens cannot authenticate; they exist to prove the
/// header is rejected *before* any key material is derived or returned.
fn token_with_header(mutate: impl FnOnce(&mut serde_json::Value)) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let parts: Vec<&str> = golden_token().split('.').collect();
    let mut header: serde_json::Value =
        serde_json::from_slice(&b64.decode(parts[0]).unwrap()).unwrap();
    mutate(&mut header);

    let encoded = b64.encode(serde_json::to_vec(&header).unwrap());
    std::iter::once(encoded.as_str())
        .chain(parts[1..].iter().copied())
        .collect::<Vec<_>>()
        .join(".")
}

fn staged_token(token: &str) -> (tempfile::TempDir, FileKeyring) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(FIXTURE_KEY_NAME), token).unwrap();
    let keyring = FileKeyring::open(dir.path(), FIXTURE_PASSWORD).unwrap();
    (dir, keyring)
}

/// Go jwx and josekit both write and inflate `zip`. Ignoring it would let a
/// compressed token authenticate and return a DEFLATE stream as key material.
#[test]
fn compressed_token_is_rejected_rather_than_returned_compressed() {
    let token = token_with_header(|h| h["zip"] = serde_json::json!("DEF"));
    let (_dir, keyring) = staged_token(&token);

    let err = keyring.get(FIXTURE_KEY_NAME).unwrap_err().to_string();
    assert!(err.contains("zip"), "expected a zip rejection, got: {err}");
}

/// RFC 7516 §4.1.13: a token whose `crit` names extensions we do not implement
/// must be rejected, not silently processed as if they were absent.
#[test]
fn token_with_unsupported_critical_extension_is_rejected() {
    let token = token_with_header(|h| h["crit"] = serde_json::json!(["exp"]));
    let (_dir, keyring) = staged_token(&token);

    let err = keyring.get(FIXTURE_KEY_NAME).unwrap_err().to_string();
    assert!(
        err.contains("crit"),
        "expected a crit rejection, got: {err}"
    );
}

/// RFC 7518 §4.8.1.1 requires a Salt Input of 8+ octets. A short or empty salt
/// makes the derived key a function of the password alone.
#[test]
fn token_with_undersized_salt_is_rejected() {
    let token = token_with_header(|h| h["p2s"] = serde_json::json!(""));
    let (_dir, keyring) = staged_token(&token);

    let err = keyring.get(FIXTURE_KEY_NAME).unwrap_err().to_string();
    assert!(err.contains("p2s"), "expected a p2s rejection, got: {err}");
}
