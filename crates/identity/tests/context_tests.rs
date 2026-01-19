//! Integration tests for IdentityContext.
//!
//! Tests for the IdentityContext type which carries identity information
//! through request handling pipelines.

use std::sync::Arc;

use crypto::generate_ed25519;
use identity::{Identity, IdentityContext, RawIdentity};

#[test]
fn test_empty_context() {
    let ctx = IdentityContext::empty();
    assert!(!ctx.has_identity());
    assert!(!ctx.has_full_identity());
    assert!(ctx.identity().is_none());
    assert!(ctx.full_identity().is_none());
}

#[test]
fn test_default_is_empty() {
    let ctx = IdentityContext::default();
    assert!(!ctx.has_identity());
}

#[test]
fn test_with_full_identity() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();
    let expected_did = identity.did().unwrap();

    let ctx = IdentityContext::with_full_identity(identity);

    assert!(ctx.has_identity());
    assert!(ctx.has_full_identity());

    let id = ctx.identity().unwrap();
    assert_eq!(id.did().unwrap(), expected_did);

    assert!(ctx.full_identity().is_some());
}

#[test]
fn test_with_full_identity_arc() {
    let key = generate_ed25519().unwrap();
    let identity = Arc::new(RawIdentity::from_private_key(key).unwrap());
    let expected_did = identity.did().unwrap();

    let ctx = IdentityContext::with_full_identity_arc(identity.clone());

    assert!(ctx.has_identity());

    let id = ctx.identity().unwrap();
    assert_eq!(id.did().unwrap(), expected_did);

    // Verify Arc is shared
    let arc = ctx.raw_identity_arc().unwrap();
    assert!(Arc::ptr_eq(&arc, &identity));
}

#[test]
fn test_raw_identity_access() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();
    let expected_did = identity.did().unwrap();

    let ctx = IdentityContext::with_full_identity(identity);

    let raw = ctx.raw_identity().unwrap();
    assert_eq!(raw.did().unwrap(), expected_did);
}

#[test]
fn test_full_identity_can_sign() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();

    let ctx = IdentityContext::with_full_identity(identity);

    let full = ctx.full_identity().unwrap();
    let message = b"test message";
    let signature = full.sign(message).unwrap();

    // Verify signature
    let verified = ctx
        .identity()
        .unwrap()
        .pub_key()
        .verify(message, &signature)
        .unwrap();
    assert!(verified);
}

#[test]
fn test_context_is_clone() {
    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();
    let expected_did = identity.did().unwrap();

    let ctx1 = IdentityContext::with_full_identity(identity);
    let ctx2 = ctx1.clone();

    // Both should have the same identity
    assert_eq!(ctx1.identity().unwrap().did().unwrap(), expected_did);
    assert_eq!(ctx2.identity().unwrap().did().unwrap(), expected_did);
}

#[test]
fn test_debug_output() {
    let empty_ctx = IdentityContext::empty();
    let debug_str = format!("{:?}", empty_ctx);
    assert!(debug_str.contains("empty"));

    let key = generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(key).unwrap();
    let full_ctx = IdentityContext::with_full_identity(identity);
    let debug_str = format!("{:?}", full_ctx);
    assert!(debug_str.contains("full"));
    assert!(debug_str.contains("did:key:"));
}
