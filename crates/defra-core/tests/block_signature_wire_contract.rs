//! Which block signature types a Go peer can verify.
//!
//! Go's `getPublicKeyFromSignature` (`internal/core/block/signature.go:186`)
//! maps only `EdDSA` and `ES256K` to a key type and returns
//! `ErrUnsupportedPrivKeyType` for anything else. A block signed with any other
//! type is refused during replication, so which types are Rust-only is part of
//! the wire contract and belongs in a test rather than a comment: the comment
//! that recorded it was deleted once already.

use defra_core::block::SignatureType;

/// The two types Go's verifier accepts.
#[test]
fn go_verifiable_types_match_gos_verifier() {
    assert!(SignatureType::ES256K.is_go_verifiable());
    assert!(SignatureType::EdDSA.is_go_verifiable());
}

/// Rust-only types. `BLS` is the Orbis ring extension and predates this;
/// `ES256` covers secp256r1, including Secure Enclave keys.
#[test]
fn rust_only_types_are_not_go_verifiable() {
    assert!(!SignatureType::BLS.is_go_verifiable());
    assert!(!SignatureType::ES256.is_go_verifiable());
}

/// Every variant is classified. A new signature type cannot be added without
/// deciding whether Go peers can consume it, because `is_go_verifiable` matches
/// exhaustively and this walks the same set.
#[test]
fn every_signature_type_is_classified() {
    let all = [
        SignatureType::ES256K,
        SignatureType::EdDSA,
        SignatureType::ES256,
        SignatureType::BLS,
    ];
    let go_verifiable = all.iter().filter(|kind| kind.is_go_verifiable()).count();

    assert_eq!(
        go_verifiable, 2,
        "exactly the two types Go maps to a key type are wire compatible"
    );
    assert_eq!(all.len(), 4, "a new variant needs a decision in this test");
}
