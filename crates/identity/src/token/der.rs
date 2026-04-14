//! ECDSA signature DER ↔ raw conversion for JWT signatures.
//!
//! Delegates to `k256::ecdsa::Signature` and `p256::ecdsa::Signature` —
//! the same upstream RustCrypto types used by the `crypto` crate. JWT
//! wire format requires raw R||S (RFC 7518), while our crypto layer
//! signs/verifies in DER, so the JWT layer needs to translate at the
//! boundary.
//!
//! Previously this module was a hand-rolled ASN.1 parser (~250 lines).
//! That parser was the highest-risk file in the crypto audit (#775)
//! because manual DER parsing is a notorious vulnerability vector.
//! `k256` and `p256` provide the same conversion via their mainline
//! `from_der` / `to_der` / `to_bytes` / `from_slice` methods, which are
//! the same code paths thousands of other Rust crypto users rely on.

use k256::ecdsa::Signature as K256Signature;
use p256::ecdsa::Signature as P256Signature;

use crate::error::Error;
use crate::Result;

/// secp256k1 (ES256K) DER → raw R||S (64 bytes).
pub(crate) fn k256_der_to_raw(der: &[u8]) -> Result<Vec<u8>> {
    let sig = K256Signature::from_der(der)
        .map_err(|e| Error::TokenEncoding(format!("invalid ES256K DER signature: {}", e)))?;
    Ok(sig.to_bytes().to_vec())
}

/// secp256k1 (ES256K) raw R||S (64 bytes) → DER.
pub(crate) fn k256_raw_to_der(raw: &[u8]) -> Result<Vec<u8>> {
    let sig = K256Signature::from_slice(raw)
        .map_err(|e| Error::TokenDecoding(format!("invalid ES256K raw signature: {}", e)))?;
    Ok(sig.to_der().as_bytes().to_vec())
}

/// secp256r1 (ES256) DER → raw R||S (64 bytes).
pub(crate) fn p256_der_to_raw(der: &[u8]) -> Result<Vec<u8>> {
    let sig = P256Signature::from_der(der)
        .map_err(|e| Error::TokenEncoding(format!("invalid ES256 DER signature: {}", e)))?;
    Ok(sig.to_bytes().to_vec())
}

/// secp256r1 (ES256) raw R||S (64 bytes) → DER.
pub(crate) fn p256_raw_to_der(raw: &[u8]) -> Result<Vec<u8>> {
    let sig = P256Signature::from_slice(raw)
        .map_err(|e| Error::TokenDecoding(format!("invalid ES256 raw signature: {}", e)))?;
    Ok(sig.to_der().as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real ES256K signature produced by k256 in DER form, used as a
    /// known-good fixture for round-trip tests.
    fn k256_der_fixture() -> Vec<u8> {
        use k256::ecdsa::{signature::Signer, SigningKey};
        let sk = SigningKey::from_bytes(&[0x42u8; 32].into()).unwrap();
        let sig: K256Signature = sk.sign(b"defradb-#775");
        sig.to_der().as_bytes().to_vec()
    }

    fn p256_der_fixture() -> Vec<u8> {
        use p256::ecdsa::{signature::Signer, SigningKey};
        let sk = SigningKey::from_bytes(&[0x42u8; 32].into()).unwrap();
        let sig: P256Signature = sk.sign(b"defradb-#775");
        sig.to_der().as_bytes().to_vec()
    }

    #[test]
    fn k256_roundtrip() {
        let der = k256_der_fixture();
        let raw = k256_der_to_raw(&der).unwrap();
        assert_eq!(raw.len(), 64);
        let der2 = k256_raw_to_der(&raw).unwrap();
        // DER may be re-canonicalized; converting back to raw must match.
        assert_eq!(k256_der_to_raw(&der2).unwrap(), raw);
    }

    #[test]
    fn p256_roundtrip() {
        let der = p256_der_fixture();
        let raw = p256_der_to_raw(&der).unwrap();
        assert_eq!(raw.len(), 64);
        let der2 = p256_raw_to_der(&raw).unwrap();
        assert_eq!(p256_der_to_raw(&der2).unwrap(), raw);
    }

    #[test]
    fn k256_der_rejects_garbage() {
        assert!(k256_der_to_raw(&[]).is_err());
        assert!(k256_der_to_raw(&[0x30, 0x00]).is_err());
        assert!(k256_der_to_raw(b"not der at all").is_err());
    }

    #[test]
    fn k256_raw_rejects_wrong_length() {
        assert!(k256_raw_to_der(&[0u8; 63]).is_err());
        assert!(k256_raw_to_der(&[0u8; 65]).is_err());
    }

    #[test]
    fn p256_der_rejects_garbage() {
        assert!(p256_der_to_raw(&[]).is_err());
        assert!(p256_der_to_raw(b"not der at all").is_err());
    }

    #[test]
    fn p256_raw_rejects_wrong_length() {
        assert!(p256_raw_to_der(&[0u8; 63]).is_err());
        assert!(p256_raw_to_der(&[0u8; 65]).is_err());
    }
}
