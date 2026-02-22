//! Go Compatibility Tests for JWT Tokens
//!
//! These tests verify that Rust can parse and validate JWT tokens that a Go DefraDB
//! node would generate. Test vectors were generated using the known test keys from
//! go_compat_keys.rs with fixed timestamps (exp: 9999999999, nbf: 0, iat: 0).
//!
//! Algorithm mapping:
//! - Ed25519  → EdDSA (raw 64-byte signature, base64url-encoded)
//! - secp256k1 → ES256K (raw R||S 64-byte, base64url-encoded)
//! - secp256r1 → ES256  (raw R||S 64-byte, base64url-encoded)

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use identity::{from_token, Identity, IdentityKeyType};

// JWT test vectors with fixed timestamps: exp=9999999999 (year 2286), nbf=0, iat=0.
// Generated from the same test keys used in go_compat_keys.rs.
const EDDSA_JWT: &str = concat!(
    "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9",
    ".",
    "eyJzdWIiOiJkNzVhOTgwMTgyYjEwYWI3ZDU0YmZlZDNjOTY0MDczYTBlZTE3MmYzZGFhNjIzMjVhZjAyMWE2OGY3MDc1MTFhIiwiaXNzIjoiZGlkOmtleTp6Nk1rdHd1cGRtTFhWVnFUekN3NGk0NnI0dUd5b3NHWFJuUjNYak40WnE3b01Nc3ciLCJleHAiOjk5OTk5OTk5OTksIm5iZiI6MCwiaWF0IjowLCJhdWQiOlsidGVzdC1hdWRpZW5jZSJdLCJrZXlfdHlwZSI6ImVkMjU1MTkifQ",
    ".",
    "EPeTYsxh0ULPSpp5Wmhp3eo8VTtlN4qlH_6pi53Ui7DmSqGhm4nNbVtEyMyHQNcSfKf2P-bfpFn0FyCaSmMfBg"
);

const ES256K_JWT: &str = concat!(
    "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ",
    ".",
    "eyJzdWIiOiIwMjg0YmY3NTYyMjYyYmJkNjk0MDA4NTc0OGYzYmU2YWZhNTJhZTMxNzE1NTE4MWVjZTMxYjY2MzUxY2NmZmE0YjAiLCJpc3MiOiJkaWQ6a2V5Ono3cjhvcjhlY2FnWTlMRDg3czU0SzJhcmNYbWdtdzZiVWh5dnE4M1JybkIyaEppVWIydWc1WUdBazFaVWFpbWV3bm9MTDFaR3pYdVRDbldSU3JSWmdSM3YyUExQSCIsImV4cCI6OTk5OTk5OTk5OSwibmJmIjowLCJpYXQiOjAsImF1ZCI6WyJ0ZXN0LWF1ZGllbmNlIl0sImtleV90eXBlIjoic2VjcDI1NmsxIn0",
    ".",
    "WoUcWXDFz3xgMg0N5EbB-y19d4ws3skbQfPgY3RaVp8j6yU2_eGAMT5i7uK_sBqD731KOevKGZvOwlwG56f6iA"
);

const ES256_JWT: &str = concat!(
    "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9",
    ".",
    "eyJzdWIiOiIwMjUxNWMzZDZlYjllMzk2YjkwNGQzZmVjYTdmNTRmZGNkMGNjMWU5OTdiZjM3NWRjYTUxNWFkMGE2YzNiNDAzNWYiLCJpc3MiOiJkaWQ6a2V5Ono0b0o4YlFXbWRSYmtoV3NiQzg1UzdCa0xEN2RmWjJ0bTNlWjJtdEE2QzRqM1NpMTlYTGlqMVVEMXF6YUZZUU05ZkM3eDFZaDJQTWRuR2tNOFBvQm5uZERMd3pISCIsImV4cCI6OTk5OTk5OTk5OSwibmJmIjowLCJpYXQiOjAsImF1ZCI6WyJ0ZXN0LWF1ZGllbmNlIl0sImtleV90eXBlIjoic2VjcDI1NnIxIn0",
    ".",
    "0KwE_DMnRbkX-de0Xxiui3lgqN6r7pWcaiGeHj4AwODzhwIzEAKYSSWv3t6_zXwJRcr3CRgGb-dyoNR4g0dMlg"
);

const ED25519_DID: &str = "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw";
const SECP256K1_DID: &str =
    "did:key:z7r8or8ecagY9LD87s54K2arcXmgmw6bUhyvq83RrnB2hJiUb2ug5YGAk1ZUaimewnoLL1ZGzXuTCnWRSrRZgR3v2PLPH";
const SECP256R1_DID: &str =
    "did:key:z4oJ8bQWmdRbkhWsbC85S7BkLD7dfZ2tm3eZ2mtA6C4j3Si19XLij1UD1qzaFYQM9fC7x1Yh2PMdnGkM8PoBnndDLwzHH";

#[test]
fn test_parse_go_eddsa_jwt() {
    let ti = from_token(EDDSA_JWT.as_bytes()).expect("Rust should parse Go-generated EdDSA JWT");

    assert_eq!(ti.key_type(), IdentityKeyType::Ed25519);
    assert_eq!(ti.did().unwrap().as_str(), ED25519_DID);
    assert_eq!(ti.claims.aud, Some(vec!["test-audience".to_string()]));
    assert_eq!(ti.claims.key_type, "ed25519");
}

#[test]
fn test_parse_go_es256k_jwt() {
    let ti = from_token(ES256K_JWT.as_bytes()).expect("Rust should parse Go-generated ES256K JWT");

    assert_eq!(ti.key_type(), IdentityKeyType::Secp256k1);
    assert_eq!(ti.did().unwrap().as_str(), SECP256K1_DID);
    assert_eq!(ti.claims.aud, Some(vec!["test-audience".to_string()]));
    assert_eq!(ti.claims.key_type, "secp256k1");
}

#[test]
fn test_parse_go_es256_jwt() {
    let ti = from_token(ES256_JWT.as_bytes()).expect("Rust should parse Go-generated ES256 JWT");

    assert_eq!(ti.key_type(), IdentityKeyType::Secp256r1);
    assert_eq!(ti.did().unwrap().as_str(), SECP256R1_DID);
    assert_eq!(ti.claims.aud, Some(vec!["test-audience".to_string()]));
    assert_eq!(ti.claims.key_type, "secp256r1");
}

#[test]
fn test_eddsa_jwt_claims_match_go() {
    let ti = from_token(EDDSA_JWT.as_bytes()).unwrap();

    assert_eq!(ti.claims.nbf, 0);
    assert_eq!(ti.claims.iat, 0);
    assert_eq!(ti.claims.exp, 9_999_999_999);
    assert_eq!(ti.claims.iss, ED25519_DID);
    assert_eq!(
        ti.claims.sub,
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    );
}

#[test]
fn test_es256k_jwt_claims_match_go() {
    let ti = from_token(ES256K_JWT.as_bytes()).unwrap();

    assert_eq!(ti.claims.nbf, 0);
    assert_eq!(ti.claims.exp, 9_999_999_999);
    assert_eq!(ti.claims.iss, SECP256K1_DID);
    assert_eq!(
        ti.claims.sub,
        "0284bf7562262bbd6940085748f3be6afa52ae317155181ece31b66351ccffa4b0"
    );
}

#[test]
fn test_es256_jwt_claims_match_go() {
    let ti = from_token(ES256_JWT.as_bytes()).unwrap();

    assert_eq!(ti.claims.nbf, 0);
    assert_eq!(ti.claims.exp, 9_999_999_999);
    assert_eq!(ti.claims.iss, SECP256R1_DID);
    assert_eq!(
        ti.claims.sub,
        "02515c3d6eb9e396b904d3feca7f54fdcd0cc1e997bf375dca515ad0a6c3b4035f"
    );
}

#[test]
fn test_eddsa_jwt_signature_matches_go() {
    // Ed25519 is deterministic: same key + same signing_input → same signature.
    use crypto::keys::ed25519::Ed25519PrivateKey;
    use crypto::keys::PrivateKey;

    let ed25519_priv_bytes: [u8; 64] = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60, 0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9,
        0x64, 0x07, 0x3a, 0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68,
        0xf7, 0x07, 0x51, 0x1a,
    ];

    let pk = Ed25519PrivateKey::from_bytes(&ed25519_priv_bytes).unwrap();
    let parts: Vec<&str> = EDDSA_JWT.split('.').collect();
    let signing_input = format!("{}.{}", parts[0], parts[1]);

    let sig = pk.sign(signing_input.as_bytes()).unwrap();
    let sig_b64 = URL_SAFE_NO_PAD.encode(&sig);

    assert_eq!(
        sig_b64, parts[2],
        "Rust Ed25519 signature over JWT signing input must match Go (deterministic)"
    );
}

#[test]
fn test_tampered_eddsa_jwt_rejected() {
    let parts: Vec<&str> = EDDSA_JWT.split('.').collect();
    let mut sig = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
    sig[0] ^= 0xff;
    let tampered = format!("{}.{}.{}", parts[0], parts[1], URL_SAFE_NO_PAD.encode(&sig));

    assert!(
        from_token(tampered.as_bytes()).is_err(),
        "Tampered EdDSA JWT must be rejected"
    );
}

#[test]
fn test_tampered_es256k_jwt_rejected() {
    let parts: Vec<&str> = ES256K_JWT.split('.').collect();
    let mut sig = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
    sig[0] ^= 0xff;
    let tampered = format!("{}.{}.{}", parts[0], parts[1], URL_SAFE_NO_PAD.encode(&sig));

    assert!(
        from_token(tampered.as_bytes()).is_err(),
        "Tampered ES256K JWT must be rejected"
    );
}

#[test]
fn test_tampered_es256_jwt_rejected() {
    let parts: Vec<&str> = ES256_JWT.split('.').collect();
    let mut sig = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
    sig[0] ^= 0xff;
    let tampered = format!("{}.{}.{}", parts[0], parts[1], URL_SAFE_NO_PAD.encode(&sig));

    assert!(
        from_token(tampered.as_bytes()).is_err(),
        "Tampered ES256 JWT must be rejected"
    );
}

#[test]
fn test_tampered_payload_rejected() {
    // Modify the payload — signature no longer covers the new payload
    let parts: Vec<&str> = EDDSA_JWT.split('.').collect();
    let payload = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
    let mut claims: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    claims["aud"] = serde_json::json!(["attacker-controlled"]);
    let new_payload = URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims).unwrap());
    let tampered = format!("{}.{}.{}", parts[0], new_payload, parts[2]);

    assert!(
        from_token(tampered.as_bytes()).is_err(),
        "JWT with tampered payload must be rejected"
    );
}
