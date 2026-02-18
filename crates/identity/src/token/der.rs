//! DER signature format conversion utilities.

use crate::error::Error;
use crate::Result;

/// Convert DER-encoded ECDSA signature to raw R||S format (64 bytes).
pub(crate) fn der_to_raw(der: &[u8]) -> Result<Vec<u8>> {
    // DER format: 0x30 <len> 0x02 <r_len> <r> 0x02 <s_len> <s>
    if der.len() < 8 {
        return Err(Error::TokenEncoding("DER signature too short".to_string()));
    }
    if der[0] != 0x30 {
        return Err(Error::TokenEncoding(
            "invalid DER signature: expected SEQUENCE tag".to_string(),
        ));
    }

    let mut pos: usize = 2;

    if der[1] & 0x80 != 0 {
        let len_bytes = (der[1] & 0x7f) as usize;
        pos = pos
            .checked_add(len_bytes)
            .ok_or_else(|| Error::TokenEncoding("DER length field overflow".to_string()))?;
    }

    if pos >= der.len() {
        return Err(Error::TokenEncoding(
            "DER signature truncated: R tag position out of bounds".to_string(),
        ));
    }

    if der[pos] != 0x02 {
        return Err(Error::TokenEncoding(
            "invalid DER signature: expected INTEGER tag for R".to_string(),
        ));
    }
    pos += 1;

    if pos >= der.len() {
        return Err(Error::TokenEncoding(
            "DER signature truncated: R length position out of bounds".to_string(),
        ));
    }
    let r_len = der[pos] as usize;
    pos += 1;

    let r_start = pos;
    let r_end = r_start
        .checked_add(r_len)
        .ok_or_else(|| Error::TokenEncoding("DER R length overflow".to_string()))?;

    if r_end > der.len() {
        return Err(Error::TokenEncoding(format!(
            "DER signature truncated: R component extends beyond data (need {} bytes, have {})",
            r_end,
            der.len()
        )));
    }
    pos = r_end;

    if pos >= der.len() {
        return Err(Error::TokenEncoding(
            "DER signature truncated: S tag position out of bounds".to_string(),
        ));
    }

    if der[pos] != 0x02 {
        return Err(Error::TokenEncoding(
            "invalid DER signature: expected INTEGER tag for S".to_string(),
        ));
    }
    pos += 1;

    if pos >= der.len() {
        return Err(Error::TokenEncoding(
            "DER signature truncated: S length position out of bounds".to_string(),
        ));
    }
    let s_len = der[pos] as usize;
    pos += 1;

    let s_start = pos;
    let s_end = s_start
        .checked_add(s_len)
        .ok_or_else(|| Error::TokenEncoding("DER S length overflow".to_string()))?;

    if s_end > der.len() {
        return Err(Error::TokenEncoding(format!(
            "DER signature truncated: S component extends beyond data (need {} bytes, have {})",
            s_end,
            der.len()
        )));
    }

    let mut r = &der[r_start..r_end];
    let mut s = &der[s_start..s_end];

    // Remove leading zeros (DER INTEGER padding)
    while r.len() > 32 && r[0] == 0 {
        r = &r[1..];
    }
    while s.len() > 32 && s[0] == 0 {
        s = &s[1..];
    }

    let mut result = vec![0u8; 64];
    let r_offset = 32 - r.len().min(32);
    let s_offset = 32 - s.len().min(32);

    result[r_offset..32].copy_from_slice(&r[..r.len().min(32)]);
    result[32 + s_offset..64].copy_from_slice(&s[..s.len().min(32)]);

    Ok(result)
}

/// Convert raw R||S signature (64 bytes) to DER-encoded ECDSA signature.
pub(crate) fn raw_to_der(raw: &[u8]) -> Result<Vec<u8>> {
    if raw.len() != 64 {
        return Err(Error::TokenDecoding(format!(
            "invalid raw signature length: expected 64, got {}",
            raw.len()
        )));
    }

    let r = &raw[0..32];
    let s = &raw[32..64];

    fn encode_der_integer(bytes: &[u8]) -> Vec<u8> {
        let mut start = 0;
        while start < bytes.len() - 1 && bytes[start] == 0 {
            start += 1;
        }
        let trimmed = &bytes[start..];

        let mut result = Vec::new();
        result.push(0x02); // INTEGER tag

        // Add leading zero if high bit is set (to ensure positive number)
        if trimmed[0] & 0x80 != 0 {
            result.push((trimmed.len() + 1) as u8);
            result.push(0x00);
        } else {
            result.push(trimmed.len() as u8);
        }
        result.extend_from_slice(trimmed);
        result
    }

    let r_der = encode_der_integer(r);
    let s_der = encode_der_integer(s);

    let content_len = r_der.len() + s_der.len();
    if content_len > 127 {
        return Err(Error::TokenEncoding(format!(
            "DER content length {} exceeds single-byte short form limit",
            content_len
        )));
    }

    let mut result = Vec::new();
    result.push(0x30); // SEQUENCE tag
    result.push(content_len as u8);
    result.extend_from_slice(&r_der);
    result.extend_from_slice(&s_der);

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_der_to_raw_empty_input() {
        let result = der_to_raw(&[]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenEncoding(ref msg) if msg.contains("too short")));
    }

    #[test]
    fn test_der_to_raw_wrong_tag() {
        let result = der_to_raw(&[0x31, 0x40, 0x02, 0x20, 0x00, 0x00, 0x00, 0x00]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenEncoding(ref msg) if msg.contains("SEQUENCE tag")));
    }

    #[test]
    fn test_der_to_raw_truncated_r() {
        let result = der_to_raw(&[0x30, 0x44, 0x02, 0x20, 0x00, 0x00, 0x00, 0x00]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenEncoding(ref msg) if msg.contains("truncated")));
    }

    #[test]
    fn test_der_to_raw_missing_s_tag() {
        let mut der = vec![0x30, 0x26];
        der.push(0x02);
        der.push(0x20);
        der.extend_from_slice(&[0x00; 32]);

        let result = der_to_raw(&der);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::TokenEncoding(ref msg) if msg.contains("out of bounds") || msg.contains("truncated"))
        );
    }

    #[test]
    fn test_der_to_raw_wrong_r_tag() {
        let result = der_to_raw(&[0x30, 0x44, 0x03, 0x20, 0x00, 0x00, 0x00, 0x00]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenEncoding(ref msg) if msg.contains("INTEGER tag for R")));
    }

    #[test]
    fn test_raw_to_der_wrong_length() {
        let result = raw_to_der(&[0u8; 63]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenDecoding(ref msg) if msg.contains("expected 64")));
    }

    #[test]
    fn test_der_roundtrip() {
        // Create a valid raw signature (64 bytes)
        let mut raw = vec![0u8; 64];
        raw[0] = 0x12;
        raw[31] = 0x34;
        raw[32] = 0x56;
        raw[63] = 0x78;

        let der = raw_to_der(&raw).unwrap();
        let recovered = der_to_raw(&der).unwrap();

        assert_eq!(raw, recovered);
    }

    #[test]
    fn test_der_roundtrip_with_high_bits() {
        // Test with high bits set (requires leading zero in DER)
        let mut raw = vec![0u8; 64];
        raw[0] = 0xFF;
        raw[32] = 0x80;

        let der = raw_to_der(&raw).unwrap();
        let recovered = der_to_raw(&der).unwrap();

        assert_eq!(raw, recovered);
    }
}
