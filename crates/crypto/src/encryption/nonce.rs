//! Nonce generation for AES-GCM encryption
//!
//! This module provides nonce generation for AES-GCM authenticated encryption.
//! In production, nonces are generated using cryptographically secure random numbers.
//! In tests, deterministic nonces can be used for reproducibility.

use rand::rngs::OsRng;
use rand::RngCore;

use defra_core::Result;

use crate::types::AES_NONCE_SIZE;

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
    #[cfg(test)]
    {
        // In test mode, use deterministic nonce for reproducibility
        if USE_DETERMINISTIC_NONCE.load(std::sync::atomic::Ordering::Relaxed) {
            return generate_deterministic_nonce();
        }
    }

    generate_random_nonce()
}

/// Generate a random nonce using cryptographically secure RNG
fn generate_random_nonce() -> Result<[u8; AES_NONCE_SIZE]> {
    let mut nonce = [0u8; AES_NONCE_SIZE];
    OsRng
        .try_fill_bytes(&mut nonce)
        .map_err(|e| crate::error::crypto_error(format!("RNG failure in nonce generation: {}", e)))?;
    Ok(nonce)
}

/// Generate a deterministic nonce for testing
///
/// This should NEVER be used in production. It's only for testing purposes
/// to ensure reproducible test results.
///
/// Uses the same deterministic value as the Go implementation:
/// "deterministic nonce for testing" (first 12 bytes)
#[cfg(test)]
pub fn generate_deterministic_nonce() -> Result<[u8; AES_NONCE_SIZE]> {
    // Match Go's generateTestNonce(): []byte("deterministic nonce for testing")[:12]
    let full_nonce = b"deterministic nonce for testing";
    let mut nonce = [0u8; AES_NONCE_SIZE];
    nonce.copy_from_slice(&full_nonce[..AES_NONCE_SIZE]);
    Ok(nonce)
}

/// Control whether to use deterministic nonces in tests
#[cfg(test)]
pub static USE_DETERMINISTIC_NONCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_nonce_random() {
        // Ensure deterministic mode is off
        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        let nonce1 = generate_nonce().unwrap();
        let nonce2 = generate_nonce().unwrap();

        assert_eq!(nonce1.len(), AES_NONCE_SIZE);
        assert_eq!(nonce2.len(), AES_NONCE_SIZE);

        // Random nonces should be different
        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_generate_deterministic_nonce() {
        let nonce1 = generate_deterministic_nonce().unwrap();
        let nonce2 = generate_deterministic_nonce().unwrap();

        assert_eq!(nonce1.len(), AES_NONCE_SIZE);
        assert_eq!(nonce2.len(), AES_NONCE_SIZE);

        // Deterministic nonces should be identical
        assert_eq!(nonce1, nonce2);

        // Should match Go's generateTestNonce(): first 12 bytes of "deterministic nonce for testing"
        let expected = b"deterministi"; // First 12 bytes
        assert_eq!(&nonce1, expected);
    }

    #[test]
    fn test_generate_nonce_deterministic_mode() {
        // Enable deterministic mode
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let nonce1 = generate_nonce().unwrap();
        let nonce2 = generate_nonce().unwrap();

        assert_eq!(nonce1, nonce2);
        assert_eq!(&nonce1, b"deterministi");

        // Restore random mode
        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    #[test]
    fn test_nonce_size() {
        let nonce = generate_random_nonce().unwrap();
        assert_eq!(nonce.len(), 12);
    }

    #[test]
    fn test_nonce_randomness() {
        // Ensure deterministic mode is off
        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);

        // Generate multiple nonces
        let mut nonces = Vec::new();
        for _ in 0..10 {
            let nonce = generate_nonce().unwrap();
            nonces.push(nonce);
        }

        // Check that not all nonces are identical (basic randomness check)
        let first = &nonces[0];
        let all_same = nonces.iter().all(|n| n == first);
        assert!(!all_same, "Nonces should be different (random)");

        // Check that we have at least several unique nonces
        use std::collections::HashSet;
        let unique_nonces: HashSet<_> = nonces.iter().collect();
        assert!(
            unique_nonces.len() >= 8,
            "Expected at least 8 unique nonces out of 10, got {}",
            unique_nonces.len()
        );
    }
}

