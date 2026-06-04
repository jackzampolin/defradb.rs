//! CID verification for P2P-received blocks.
//!
//! Only SHA2-256 (multihash code 0x12) is accepted for P2P blocks. Any other
//! hash algorithm is rejected to prevent bypass via unsupported algorithm codes.

use cid::Cid;
use multihash_codetable::Code;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// Verify that `data` hashes to the digest encoded in `cid`.
///
/// Rejects CIDs that use any hash algorithm other than SHA2-256 to prevent
/// an unsupported-algorithm bypass. Returns an error if:
/// - The CID uses an algorithm other than SHA2-256, or
/// - The SHA2-256 digest of `data` does not match the digest in `cid`.
pub fn verify_block_cid(cid: &Cid, data: &[u8]) -> Result<()> {
    let mh = cid.hash();
    let code = mh.code();

    if code != u64::from(Code::Sha2_256) {
        return Err(Error::UnsupportedHashAlgorithm {
            code,
            cid: cid.to_string(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(data);
    let computed = hasher.finalize();

    if mh.digest() != computed.as_slice() {
        return Err(Error::CidVerificationFailed {
            cid: cid.to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash::Multihash;
    use multihash_codetable::{Code, MultihashDigest};

    fn sha256_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash)
    }

    #[test]
    fn valid_block_passes() {
        let data = b"hello world";
        let cid = sha256_cid(data);
        assert!(verify_block_cid(&cid, data).is_ok());
    }

    #[test]
    fn tampered_data_fails() {
        let data = b"hello world";
        let cid = sha256_cid(data);
        let tampered = b"evil world!!";
        let err = verify_block_cid(&cid, tampered).unwrap_err();
        assert!(matches!(err, Error::CidVerificationFailed { .. }));
    }

    #[test]
    fn unsupported_algorithm_fails() {
        // Construct a CID with Blake2b-256 (multihash code 0xb220)
        let digest = [0u8; 32];
        let mh: Multihash<64> = Multihash::wrap(0xb220, &digest).unwrap();
        let cid = Cid::new_v1(0x71, mh);
        let err = verify_block_cid(&cid, b"anything").unwrap_err();
        assert!(matches!(
            err,
            Error::UnsupportedHashAlgorithm { code: 0xb220, .. }
        ));
    }
}
