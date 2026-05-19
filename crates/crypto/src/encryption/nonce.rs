//! Nonce generation for AES-GCM encryption
//!
//! This module provides nonce generation for AES-GCM authenticated encryption.
//! In production, nonces are generated using cryptographically secure random numbers.
//! Deterministic nonces are only for Go-compatibility tests. Release builds
//! require `DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO=1` before the hidden test
//! switch can be enabled; production deployments must never set that variable.

use rand::rngs::OsRng;
use rand::RngCore;
use std::sync::atomic::{AtomicBool, Ordering};

use defra_core::Result;

use crate::types::AES_NONCE_SIZE;

const DETERMINISTIC_TEST_CRYPTO_ENV: &str = "DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO";

#[doc(hidden)]
static USE_DETERMINISTIC_NONCE: AtomicBool = AtomicBool::new(false);

/// Generate a cryptographically secure random nonce for AES-GCM
///
/// # Returns
/// A 12-byte (96-bit) nonce suitable for AES-GCM encryption
///
/// # Example
/// ```ignore
/// let nonce = generate_nonce()?;
/// assert_eq!(nonce.len(), 12);
/// ```
pub fn generate_nonce() -> Result<[u8; AES_NONCE_SIZE]> {
    if USE_DETERMINISTIC_NONCE.load(Ordering::Acquire) {
        return generate_deterministic_nonce();
    }

    generate_random_nonce()
}

/// Generate a random nonce using cryptographically secure RNG
fn generate_random_nonce() -> Result<[u8; AES_NONCE_SIZE]> {
    let mut nonce = [0u8; AES_NONCE_SIZE];
    OsRng.try_fill_bytes(&mut nonce).map_err(|e| {
        crate::error::crypto_error(format!("RNG failure in nonce generation: {}", e))
    })?;
    Ok(nonce)
}

/// Generate a deterministic nonce for testing
///
/// **WARNING: This should NEVER be used in production.** It's only for testing
/// purposes to ensure reproducible test results.
///
/// Uses the same deterministic value as the Go implementation:
/// "deterministic nonce for testing" (first 12 bytes)
#[doc(hidden)]
pub fn generate_deterministic_nonce() -> Result<[u8; AES_NONCE_SIZE]> {
    // Match Go's generateTestNonce(): []byte("deterministic nonce for testing")[:12]
    let full_nonce = b"deterministic nonce for testing";
    let mut nonce = [0u8; AES_NONCE_SIZE];
    nonce.copy_from_slice(&full_nonce[..AES_NONCE_SIZE]);
    Ok(nonce)
}

/// Control whether to use deterministic nonces in test/debug builds.
///
/// **WARNING: This should NEVER be set to true in production.**
/// Only use for testing with reproducible nonces. In release builds this
/// function refuses to enable deterministic mode unless
/// `DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO=1` is present in the process
/// environment.
#[doc(hidden)]
pub fn set_deterministic_nonce(enabled: bool) {
    USE_DETERMINISTIC_NONCE.store(
        enabled && deterministic_test_crypto_allowed(),
        Ordering::Release,
    );
}

#[doc(hidden)]
pub fn deterministic_nonce_enabled() -> bool {
    USE_DETERMINISTIC_NONCE.load(Ordering::Acquire)
}

fn deterministic_test_crypto_allowed() -> bool {
    cfg!(debug_assertions)
        || matches!(
            std::env::var(DETERMINISTIC_TEST_CRYPTO_ENV).as_deref(),
            Ok("1")
        )
}
