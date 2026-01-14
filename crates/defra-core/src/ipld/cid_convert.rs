//! CID conversion between crate versions.
//!
//! The `cid` crate we use directly may differ in version from the one bundled
//! with `libipld`. These helpers convert between them via bytes.

use crate::{Error, Result};

/// Convert from crate-level cid::Cid to libipld's bundled cid type.
pub fn cid_to_libipld(cid: &cid::Cid) -> Result<libipld::cid::Cid> {
    let bytes = cid.to_bytes();
    libipld::cid::Cid::try_from(bytes).map_err(|e| {
        Error::IpldError(format!(
            "Failed to convert CID {} to libipld format: {}",
            cid, e
        ))
    })
}

/// Convert from libipld's bundled cid type to crate-level cid::Cid.
pub fn cid_from_libipld(cid: &libipld::cid::Cid) -> Result<cid::Cid> {
    let bytes = cid.to_bytes();
    cid::Cid::try_from(bytes).map_err(|e| {
        Error::IpldError(format!(
            "Failed to convert libipld CID {} to native format: {}",
            cid, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_cid_roundtrip_conversion() {
        let cid = cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();
        let libipld_cid = cid_to_libipld(&cid).unwrap();
        let back = cid_from_libipld(&libipld_cid).unwrap();
        assert_eq!(cid, back);
    }
}
