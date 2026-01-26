//! Hashing utilities.

use sha2::{Digest, Sha256};

/// A SHA-256 hash output (32 bytes).
pub type Sha256Hash = [u8; 32];

/// Compute the SHA-256 hash of the input data.
///
/// Returns the hash as a fixed-size 32-byte array.
///
/// # Example
///
/// ```
/// use crypto::sha256;
///
/// let data = b"hello world";
/// let hash = sha256(data);
/// assert_eq!(hash.len(), 32);
/// ```
pub fn sha256(data: &[u8]) -> Sha256Hash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute the SHA-256 hash and return as a Vec<u8>.
///
/// This is a convenience function for cases where a Vec is needed
/// instead of a fixed-size array.
pub fn sha256_hash(data: &[u8]) -> Vec<u8> {
    sha256(data).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_empty() {
        let hash = sha256(b"");
        // SHA-256 of empty string is well-known
        assert_eq!(
            hex::encode(hash),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_hello_world() {
        let hash = sha256(b"hello world");
        assert_eq!(
            hex::encode(hash),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_sha256_deterministic() {
        let data = b"test data";
        let hash1 = sha256(data);
        let hash2 = sha256(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_sha256_hash_vec() {
        let data = b"test";
        let hash_array = sha256(data);
        let hash_vec = sha256_hash(data);
        assert_eq!(hash_array.to_vec(), hash_vec);
    }
}
