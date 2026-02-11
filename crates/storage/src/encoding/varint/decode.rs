use crate::corekv::{Error, Result};

use super::super::{INT_SMALL, INT_ZERO};

/// Decode a signed varint from ascending encoding
pub fn decode_varint_ascending(buf: &[u8]) -> Result<(&[u8], i64)> {
    if buf.is_empty() {
        return Err(Error::Other(
            "insufficient bytes to decode varint".to_string(),
        ));
    }

    let length = buf[0] as i16 - INT_ZERO as i16;
    if length < 0 {
        let length = (-length) as usize;
        let rem = &buf[1..];
        if rem.len() < length {
            return Err(Error::Other(
                "insufficient bytes to decode varint".to_string(),
            ));
        }

        // Build up a positive number using ones-complement, then invert
        let mut v: i64 = 0;
        for &t in &rem[..length] {
            v = (v << 8) | (!t) as i64;
        }
        Ok((&rem[length..], !v))
    } else {
        let (rest, v) = decode_uvarint_ascending(buf)?;
        if v > i64::MAX as u64 {
            return Err(Error::Other(format!("varint overflow: {}", v)));
        }
        Ok((rest, v as i64))
    }
}

/// Decode a signed varint from descending encoding
pub fn decode_varint_descending(buf: &[u8]) -> Result<(&[u8], i64)> {
    let (rest, v) = decode_varint_ascending(buf)?;
    Ok((rest, !v))
}

/// Decode an unsigned varint from ascending encoding
pub fn decode_uvarint_ascending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.is_empty() {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let length = buf[0] as i16 - INT_ZERO as i16;
    let rest = &buf[1..];

    if length <= INT_SMALL as i16 {
        return Ok((rest, length as u64));
    }

    let length = (length - INT_SMALL as i16) as usize;
    if length > 8 {
        return Err(Error::Other(format!("invalid uvarint length: {}", length)));
    }
    if rest.len() < length {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let mut v: u64 = 0;
    for &t in &rest[..length] {
        v = (v << 8) | t as u64;
    }
    Ok((&rest[length..], v))
}

/// Decode an unsigned varint from descending encoding
pub fn decode_uvarint_descending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.is_empty() {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let length = INT_ZERO as i16 - buf[0] as i16;
    let rest = &buf[1..];

    if !(0..=8).contains(&length) {
        return Err(Error::Other(format!("invalid uvarint length: {}", length)));
    }
    if rest.len() < length as usize {
        return Err(Error::Other(
            "insufficient bytes to decode uvarint".to_string(),
        ));
    }

    let mut x: u64 = 0;
    for &t in &rest[..length as usize] {
        x = (x << 8) | (!t) as u64;
    }
    Ok((&rest[length as usize..], x))
}
