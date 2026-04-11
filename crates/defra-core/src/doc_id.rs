use std::fmt;

use uuid::Uuid;

/// DocID version constant (matches Go's DocIDV0).
pub const DOC_ID_V0: u16 = 0x01;

/// Parsed DocID components shared across crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParsedDocId {
    version: u16,
    uuid: Uuid,
}

impl ParsedDocId {
    pub fn new(version: u16, uuid: Uuid) -> Result<Self, DocIdFormatError> {
        if version != DOC_ID_V0 {
            return Err(DocIdFormatError::InvalidVersion(version));
        }

        Ok(Self { version, uuid })
    }

    /// Parse a DocID from its string representation.
    ///
    /// Format: `{base32(version)}-{uuid}`
    pub fn from_string(s: &str) -> Result<Self, DocIdFormatError> {
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        if parts.len() != 2 {
            return Err(DocIdFormatError::Malformed);
        }

        let version_str = parts[0];
        let uuid_str = parts[1];

        let (_, version_bytes) = multibase::decode(version_str)?;
        if version_bytes.is_empty() {
            return Err(DocIdFormatError::Malformed);
        }

        let version = read_uvarint(&version_bytes).ok_or(DocIdFormatError::Malformed)?;
        if version != DOC_ID_V0 as u64 {
            return Err(DocIdFormatError::InvalidVersion(version as u16));
        }

        let uuid = Uuid::parse_str(uuid_str)?;

        Ok(Self {
            version: version as u16,
            uuid,
        })
    }

    /// Parse a DocID from its binary form.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DocIdFormatError> {
        if bytes.len() < 17 {
            return Err(DocIdFormatError::Malformed);
        }

        let version = read_uvarint(bytes).ok_or(DocIdFormatError::Malformed)?;
        if version != DOC_ID_V0 as u64 {
            return Err(DocIdFormatError::InvalidVersion(version as u16));
        }

        let uuid_start = varint_size(version);
        if bytes.len() < uuid_start + 16 {
            return Err(DocIdFormatError::Malformed);
        }

        let uuid_bytes: [u8; 16] = bytes[uuid_start..uuid_start + 16]
            .try_into()
            .map_err(|_| DocIdFormatError::Malformed)?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        Ok(Self {
            version: version as u16,
            uuid,
        })
    }

    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(18);
        write_uvarint(&mut buf, self.version as u64);
        buf.extend_from_slice(self.uuid.as_bytes());
        buf
    }
}

impl fmt::Display for ParsedDocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size = varint_size(self.version as u64);
        let mut version_buf = vec![0_u8; size];
        write_uvarint_to_slice(&mut version_buf, self.version as u64);
        let version_str = multibase::encode(multibase::Base::Base32Lower, &version_buf);

        write!(f, "{}-{}", version_str, self.uuid)
    }
}

pub fn validate_doc_id_str(s: &str) -> Result<(), DocIdFormatError> {
    ParsedDocId::from_string(s).map(|_| ())
}

#[derive(Debug, thiserror::Error)]
pub enum DocIdFormatError {
    #[error("malformed document ID")]
    Malformed,

    #[error("invalid document ID version: {0}")]
    InvalidVersion(u16),

    #[error("multibase decode error: {0}")]
    Multibase(#[from] multibase::Error),

    #[error("UUID parse error: {0}")]
    Uuid(#[from] uuid::Error),
}

fn read_uvarint(buf: &[u8]) -> Option<u64> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            return None;
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                return None;
            }
            return Some(x | (b as u64) << s);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    None
}

fn write_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

fn write_uvarint_to_slice(buf: &mut [u8], mut x: u64) -> usize {
    debug_assert!(
        buf.len() >= varint_size(x),
        "buffer too small for varint: need {} bytes, got {}",
        varint_size(x),
        buf.len()
    );

    let mut i = 0;
    while x >= 0x80 && i < buf.len() {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    if i < buf.len() {
        buf[i] = x as u8;
        i += 1;
    }
    i
}

fn varint_size(x: u64) -> usize {
    if x == 0 {
        return 1;
    }

    let mut size = 0;
    let mut v = x;
    while v > 0 {
        size += 1;
        v >>= 7;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_doc_id() {
        let parsed = ParsedDocId::from_string("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").unwrap();

        assert_eq!(parsed.version(), DOC_ID_V0);
        assert_eq!(
            parsed.to_string(),
            "bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc"
        );
    }

    #[test]
    fn test_parse_invalid_doc_id() {
        assert!(ParsedDocId::from_string("bae-not-a-uuid").is_err());
        assert!(ParsedDocId::from_string("nodash").is_err());
    }
}
