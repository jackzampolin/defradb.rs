//! Float32 and Float64 encoding

use crate::corekv::{Error, Result};

use super::{
    peek_type, EncodedType, FLOAT32_NAN, FLOAT32_NAN_DESC, FLOAT32_NEG, FLOAT32_POS, FLOAT32_ZERO,
    FLOAT64_NAN, FLOAT64_NAN_DESC, FLOAT64_NEG, FLOAT64_POS, FLOAT64_ZERO,
};

#[cfg(test)]
mod tests;

/// Encode a 32-bit float in ascending order
pub fn encode_float32_ascending(mut buf: Vec<u8>, f: f32) -> Vec<u8> {
    if f.is_nan() {
        buf.push(FLOAT32_NAN);
        return buf;
    }
    if f == 0.0 {
        buf.push(FLOAT32_ZERO);
        return buf;
    }

    let mut u = f.to_bits();
    if (u & (1 << 31)) != 0 {
        u = !u;
        buf.push(FLOAT32_NEG);
    } else {
        buf.push(FLOAT32_POS);
    }
    buf.extend_from_slice(&u.to_be_bytes());
    buf
}

/// Encode a 32-bit float in descending order
pub fn encode_float32_descending(buf: Vec<u8>, f: f32) -> Vec<u8> {
    if f.is_nan() {
        let mut buf = buf;
        buf.push(FLOAT32_NAN_DESC);
        return buf;
    }
    encode_float32_ascending(buf, -f)
}

/// Decode a 32-bit float from ascending encoding
pub fn decode_float32_ascending(buf: &[u8]) -> Result<(&[u8], f32)> {
    if buf.is_empty() || peek_type(buf) != EncodedType::Float32 {
        return Err(Error::Other(format!(
            "cannot decode float32: markers not found in {:?}",
            buf.first()
        )));
    }

    match buf[0] {
        FLOAT32_NAN | FLOAT32_NAN_DESC => Ok((&buf[1..], f32::NAN)),
        FLOAT32_ZERO => Ok((&buf[1..], 0.0)),
        FLOAT32_NEG => {
            if buf.len() < 5 {
                return Err(Error::Other("insufficient bytes for float32".to_string()));
            }
            let u = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
            Ok((&buf[5..], f32::from_bits(!u)))
        }
        FLOAT32_POS => {
            if buf.len() < 5 {
                return Err(Error::Other("insufficient bytes for float32".to_string()));
            }
            let u = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
            Ok((&buf[5..], f32::from_bits(u)))
        }
        _ => Err(Error::Other("invalid float32 marker".to_string())),
    }
}

/// Decode a 32-bit float from descending encoding
pub fn decode_float32_descending(buf: &[u8]) -> Result<(&[u8], f32)> {
    let (rest, r) = decode_float32_ascending(buf)?;
    // Handle special cases to avoid -0.0 and preserve NaN without negation
    if r == 0.0 || r.is_nan() {
        return Ok((rest, r));
    }
    Ok((rest, -r))
}

/// Encode a 64-bit float in ascending order
pub fn encode_float64_ascending(mut buf: Vec<u8>, f: f64) -> Vec<u8> {
    if f.is_nan() {
        buf.push(FLOAT64_NAN);
        return buf;
    }
    if f == 0.0 {
        buf.push(FLOAT64_ZERO);
        return buf;
    }

    let mut u = f.to_bits();
    if (u & (1 << 63)) != 0 {
        u = !u;
        buf.push(FLOAT64_NEG);
    } else {
        buf.push(FLOAT64_POS);
    }
    buf.extend_from_slice(&u.to_be_bytes());
    buf
}

/// Encode a 64-bit float in descending order
pub fn encode_float64_descending(buf: Vec<u8>, f: f64) -> Vec<u8> {
    if f.is_nan() {
        let mut buf = buf;
        buf.push(FLOAT64_NAN_DESC);
        return buf;
    }
    encode_float64_ascending(buf, -f)
}

/// Decode a 64-bit float from ascending encoding
pub fn decode_float64_ascending(buf: &[u8]) -> Result<(&[u8], f64)> {
    if buf.is_empty() || peek_type(buf) != EncodedType::Float64 {
        return Err(Error::Other(format!(
            "cannot decode float64: markers not found in {:?}",
            buf.first()
        )));
    }

    match buf[0] {
        FLOAT64_NAN | FLOAT64_NAN_DESC => Ok((&buf[1..], f64::NAN)),
        FLOAT64_ZERO => Ok((&buf[1..], 0.0)),
        FLOAT64_NEG => {
            if buf.len() < 9 {
                return Err(Error::Other("insufficient bytes for float64".to_string()));
            }
            let u = u64::from_be_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((&buf[9..], f64::from_bits(!u)))
        }
        FLOAT64_POS => {
            if buf.len() < 9 {
                return Err(Error::Other("insufficient bytes for float64".to_string()));
            }
            let u = u64::from_be_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((&buf[9..], f64::from_bits(u)))
        }
        _ => Err(Error::Other("invalid float64 marker".to_string())),
    }
}

/// Decode a 64-bit float from descending encoding
pub fn decode_float64_descending(buf: &[u8]) -> Result<(&[u8], f64)> {
    let (rest, r) = decode_float64_ascending(buf)?;
    // Handle special cases to avoid -0.0 and preserve NaN without negation
    if r == 0.0 || r.is_nan() {
        return Ok((rest, r));
    }
    Ok((rest, -r))
}
