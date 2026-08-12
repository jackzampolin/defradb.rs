//! PBES2-HS512+A256KW / A256GCM JWE compact serialization.
//!
//! RFC 7518 §4.8 (PBES2 key encryption) and §5.3 (AES-GCM content encryption).
//! Byte-compatible with `github.com/lestrrat-go/jwx/v2/jwe`, which Go DefraDB's
//! file keyring uses.

use aes::Aes256;
use aes_gcm::aead::AeadInPlace;
use aes_gcm::{Aes256Gcm, KeyInit};
use aes_kw::Kek;
use base64::Engine;
use rand::RngCore;
use sha2::Sha512;
use zeroize::Zeroizing;

use crate::error::{Error, Result};

pub const ALG: &str = "PBES2-HS512+A256KW";
pub const ENC: &str = "A256GCM";

/// A256KW wraps a 256-bit key, so PBES2 must derive exactly 32 bytes.
const DERIVED_KEY_LEN: usize = 32;
/// A256GCM content encryption key length.
const CEK_LEN: usize = 32;
/// A256GCM nonce length (RFC 7518 §5.3 fixes this at 96 bits).
const IV_LEN: usize = 12;
const TAG_LEN: usize = 16;
/// AES-KW output is the input plus one 8-byte integrity block (RFC 3394).
const WRAPPED_CEK_LEN: usize = CEK_LEN + 8;

/// PBKDF2 iteration count written into new tokens. Matches the Go jwx default.
pub const PBKDF2_ITER_COUNT: u32 = 10000;
/// PBKDF2 salt length written into new tokens. Matches the Go jwx default,
/// and equals the A256KW key length.
pub const PBKDF2_SALT_LEN: usize = 32;

/// RFC 7518 §4.8.1.1 minimum Salt Input length.
const MIN_SALT_LEN: usize = 8;

/// Upper bound on the PBKDF2 iteration count accepted when decrypting.
/// A hostile token could otherwise request billions of iterations and hang the
/// process; 10,000,000 is far above any legitimate writer (Go jwx uses 10,000).
const MAX_P2C: u64 = 10_000_000;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Derives the key-encryption key. Salt is `UTF8(alg) || 0x00 || p2s`
/// per RFC 7518 §4.8.1.1.
///
/// The returned key is zeroized on drop, but note the limit of that guarantee:
/// `Kek` and `Aes256Gcm` copy it into an expanded key schedule that neither
/// `aes-kw` (no zeroize support at all) nor `aes` (only on the armv8 path)
/// clears. Those schedules outlive the wrapper in freed memory.
fn derive_kek(password: &[u8], p2s: &[u8], p2c: u32) -> Zeroizing<[u8; DERIVED_KEY_LEN]> {
    let mut salt = Vec::with_capacity(ALG.len() + 1 + p2s.len());
    salt.extend_from_slice(ALG.as_bytes());
    salt.push(0x00);
    salt.extend_from_slice(p2s);

    let mut derived = Zeroizing::new([0u8; DERIVED_KEY_LEN]);
    pbkdf2::pbkdf2_hmac::<Sha512>(password, &salt, p2c, derived.as_mut());
    derived
}

/// Encrypts `plaintext` into a JWE compact serialization.
pub fn encrypt(password: &[u8], plaintext: &[u8]) -> Result<String> {
    let p2c = PBKDF2_ITER_COUNT;
    let mut rng = rand::thread_rng();

    let mut p2s = vec![0u8; PBKDF2_SALT_LEN];
    rng.fill_bytes(&mut p2s);

    let mut cek = Zeroizing::new([0u8; CEK_LEN]);
    rng.fill_bytes(cek.as_mut());

    let mut iv = [0u8; IV_LEN];
    rng.fill_bytes(&mut iv);

    // Field order is not load-bearing: the AAD is the encoded header exactly as
    // written here, and decryption reads the header back from the token.
    let header = serde_json::json!({
        "alg": ALG,
        "enc": ENC,
        "p2c": p2c,
        "p2s": B64.encode(&p2s),
    });
    let header_bytes = serde_json::to_vec(&header)
        .map_err(|e| Error::Encryption(format!("failed to encode JWE header: {e}")))?;
    let protected = B64.encode(&header_bytes);

    let kek = derive_kek(password, &p2s, p2c);
    let mut wrapped = [0u8; WRAPPED_CEK_LEN];
    Kek::<Aes256>::from(*kek)
        .wrap(cek.as_ref(), &mut wrapped)
        .map_err(|e| Error::Encryption(format!("failed to wrap content key: {e}")))?;

    let cipher = Aes256Gcm::new_from_slice(cek.as_ref())
        .map_err(|e| Error::Encryption(format!("invalid content key: {e}")))?;
    let mut buf = plaintext.to_vec();
    let tag = cipher
        .encrypt_in_place_detached(&iv.into(), protected.as_bytes(), &mut buf)
        .map_err(|e| Error::Encryption(format!("failed to encrypt: {e}")))?;

    Ok([
        protected,
        B64.encode(wrapped),
        B64.encode(iv),
        B64.encode(&buf),
        B64.encode(tag),
    ]
    .join("."))
}

/// Decrypts a JWE compact serialization produced by [`encrypt`], josekit, or Go jwx.
pub fn decrypt(password: &[u8], token: &str) -> Result<Vec<u8>> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 5 {
        return Err(Error::Decryption(format!(
            "expected 5 JWE compact parts, got {}",
            parts.len()
        )));
    }

    let header_bytes = B64
        .decode(parts[0])
        .map_err(|e| Error::Decryption(format!("invalid JWE header encoding: {e}")))?;
    let header: serde_json::Value = serde_json::from_slice(&header_bytes)
        .map_err(|e| Error::Decryption(format!("invalid JWE header: {e}")))?;

    if header["alg"] != ALG {
        return Err(Error::Decryption(format!(
            "unsupported JWE alg: {}",
            header["alg"]
        )));
    }
    if header["enc"] != ENC {
        return Err(Error::Decryption(format!(
            "unsupported JWE enc: {}",
            header["enc"]
        )));
    }
    // RFC 7516 §4.1.3. Go jwx and josekit both write and inflate `zip`; without
    // this the AEAD tag would verify and the caller would receive a DEFLATE
    // stream as key material rather than an error.
    if !header["zip"].is_null() {
        return Err(Error::Decryption(format!(
            "unsupported JWE zip: {}",
            header["zip"]
        )));
    }
    // RFC 7516 §4.1.13 requires rejecting a token whose `crit` names extensions
    // the recipient does not implement. We implement none.
    if !header["crit"].is_null() {
        return Err(Error::Decryption(format!(
            "unsupported JWE crit extensions: {}",
            header["crit"]
        )));
    }

    let p2c = header["p2c"]
        .as_u64()
        .ok_or_else(|| Error::Decryption("JWE header missing p2c".into()))?;
    if p2c == 0 || p2c > MAX_P2C {
        return Err(Error::Decryption(format!(
            "JWE p2c out of accepted range: {p2c}"
        )));
    }
    let p2s = B64
        .decode(
            header["p2s"]
                .as_str()
                .ok_or_else(|| Error::Decryption("JWE header missing p2s".into()))?,
        )
        .map_err(|e| Error::Decryption(format!("invalid p2s encoding: {e}")))?;
    // RFC 7518 §4.8.1.1: Salt Input must be 8 or more octets. An empty salt
    // makes the derived key a function of the password alone, so it is
    // precomputable across every token that carries one.
    if p2s.len() < MIN_SALT_LEN {
        return Err(Error::Decryption(format!(
            "JWE p2s shorter than the {MIN_SALT_LEN}-octet minimum: {}",
            p2s.len()
        )));
    }

    let wrapped = B64
        .decode(parts[1])
        .map_err(|e| Error::Decryption(format!("invalid encrypted key encoding: {e}")))?;
    let iv = B64
        .decode(parts[2])
        .map_err(|e| Error::Decryption(format!("invalid iv encoding: {e}")))?;
    let ciphertext = B64
        .decode(parts[3])
        .map_err(|e| Error::Decryption(format!("invalid ciphertext encoding: {e}")))?;
    let tag = B64
        .decode(parts[4])
        .map_err(|e| Error::Decryption(format!("invalid tag encoding: {e}")))?;

    if wrapped.len() != WRAPPED_CEK_LEN {
        return Err(Error::Decryption("unexpected wrapped key length".into()));
    }
    if iv.len() != IV_LEN {
        return Err(Error::Decryption("unexpected iv length".into()));
    }
    if tag.len() != TAG_LEN {
        return Err(Error::Decryption("unexpected tag length".into()));
    }

    let kek = derive_kek(password, &p2s, p2c as u32);
    let mut cek = Zeroizing::new([0u8; CEK_LEN]);
    Kek::<Aes256>::from(*kek)
        .unwrap(&wrapped, cek.as_mut())
        .map_err(|_| Error::Decryption("failed to unwrap content key".into()))?;

    let cipher = Aes256Gcm::new_from_slice(cek.as_ref())
        .map_err(|e| Error::Decryption(format!("invalid content key: {e}")))?;
    let mut buf = ciphertext;
    cipher
        .decrypt_in_place_detached(
            iv.as_slice().into(),
            parts[0].as_bytes(),
            &mut buf,
            tag.as_slice().into(),
        )
        .map_err(|_| Error::Decryption("failed to decrypt".into()))?;

    Ok(buf)
}
