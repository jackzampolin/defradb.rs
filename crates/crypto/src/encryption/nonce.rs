//! Nonce generation for AES-GCM encryption
//!
//! This module provides nonce generation for AES-GCM authenticated encryption.
//! In production, nonces are generated using cryptographically secure random numbers.
//! In tests, deterministic nonces can be used for reproducibility.

use rand::RngCore;

use defra_core::Result;

use crate::error::failed_to_generate_random;
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
    rand::thread_rng()
        .try_fill_bytes(&mut nonce)
        .map_err(|_| failed_to_generate_random())?;
    Ok(nonce)
}

/// Generate a deterministic nonce for testing
///
/// This should NEVER be used in production. It's only for testing purposes
/// to ensure reproducible test results.
#[cfg(test)]
pub fn generate_deterministic_nonce() -> Result<[u8; AES_NONCE_SIZE]> {
    // Return a fixed nonce that matches Go test behavior
    Ok(*b"deterministi")
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
        assert_eq!(&nonce1, b"deterministi");
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
}

