// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Order-preserving value encoding for secondary indexes
//!
//! This module implements CockroachDB-style encoding that maintains sort order
//! when values are compared lexicographically as byte slices.
//!
//! Adapted from CockroachDB's encoding package:
//! https://github.com/cockroachdb/cockroach/tree/v20.2.19/pkg/util/encoding

use crate::corekv::{Error, Result};

// Type markers matching Go's encoding/encoding.go
const ENCODED_NULL: u8 = 0;
const FLOAT64_NAN: u8 = 1;
const FLOAT64_NEG: u8 = 2;
const FLOAT64_ZERO: u8 = 3;
const FLOAT64_POS: u8 = 4;
const FLOAT64_NAN_DESC: u8 = 5;
const BYTES_MARKER: u8 = 6;
const BYTES_DESC_MARKER: u8 = 7;
const TIME_MARKER: u8 = 8;
const FALSE_MARKER: u8 = 9;
const TRUE_MARKER: u8 = 10;
const JSON_MARKER: u8 = 11;
const FLOAT32_NAN: u8 = 12;
const FLOAT32_NEG: u8 = 13;
const FLOAT32_ZERO: u8 = 14;
const FLOAT32_POS: u8 = 15;
const FLOAT32_NAN_DESC: u8 = 16;

// Integer encoding constants
const INT_MIN: u8 = 0x80; // 128
const INT_MAX_WIDTH: u8 = 8;
const INT_ZERO: u8 = INT_MIN + INT_MAX_WIDTH; // 136
const INT_SMALL: u8 = INT_MAX - INT_ZERO - INT_MAX_WIDTH; // 109
const INT_MAX: u8 = 0xfd; // 253
const ENCODED_NULL_DESC: u8 = 0xff;

// Byte escape sequences
const ESCAPE: u8 = 0x00;
const ESCAPED_TERM: u8 = 0x01;
const ESCAPED_00: u8 = 0xff;

/// Type of encoded value, used for decoding
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedType {
    Unknown,
    Null,
    Bool,
    Int,
    Float64,
    Float32,
    Bytes,
    BytesDesc,
    Time,
    Json,
}

/// Peek at the type of the encoded value at the start of the buffer
pub fn peek_type(buf: &[u8]) -> EncodedType {
    if buf.is_empty() {
        return EncodedType::Unknown;
    }
    let m = buf[0];
    match m {
        ENCODED_NULL | ENCODED_NULL_DESC => EncodedType::Null,
        BYTES_MARKER => EncodedType::Bytes,
        BYTES_DESC_MARKER => EncodedType::BytesDesc,
        m if m >= INT_MIN && m <= INT_MAX => EncodedType::Int,
        m if m >= FLOAT32_NAN && m <= FLOAT32_NAN_DESC => EncodedType::Float32,
        m if m >= FLOAT64_NAN && m <= FLOAT64_NAN_DESC => EncodedType::Float64,
        TIME_MARKER => EncodedType::Time,
        FALSE_MARKER | TRUE_MARKER => EncodedType::Bool,
        JSON_MARKER => EncodedType::Json,
        _ => EncodedType::Unknown,
    }
}

// =============================================================================
// Null encoding
// =============================================================================

/// Encode a null value in ascending order
pub fn encode_null_ascending(mut buf: Vec<u8>) -> Vec<u8> {
    buf.push(ENCODED_NULL);
    buf
}

/// Encode a null value in descending order
pub fn encode_null_descending(mut buf: Vec<u8>) -> Vec<u8> {
    buf.push(ENCODED_NULL_DESC);
    buf
}

/// Decode and check if buffer starts with null
pub fn decode_if_null(buf: &[u8]) -> (&[u8], bool) {
    match peek_type(buf) {
        EncodedType::Null => (&buf[1..], true),
        _ => (buf, false),
    }
}

// =============================================================================
// Boolean encoding
// =============================================================================

/// Encode a boolean in ascending order
pub fn encode_bool_ascending(mut buf: Vec<u8>, v: bool) -> Vec<u8> {
    buf.push(if v { TRUE_MARKER } else { FALSE_MARKER });
    buf
}

/// Encode a boolean in descending order
pub fn encode_bool_descending(buf: Vec<u8>, v: bool) -> Vec<u8> {
    encode_bool_ascending(buf, !v)
}

/// Decode a boolean encoded in ascending order
pub fn decode_bool_ascending(buf: &[u8]) -> Result<(&[u8], bool)> {
    if buf.is_empty() || peek_type(buf) != EncodedType::Bool {
        return Err(Error::Other(format!(
            "cannot decode bool: markers not found in {:?}",
            buf.first()
        )));
    }
    Ok((&buf[1..], buf[0] == TRUE_MARKER))
}

/// Decode a boolean encoded in descending order
pub fn decode_bool_descending(buf: &[u8]) -> Result<(&[u8], bool)> {
    let (rest, v) = decode_bool_ascending(buf)?;
    Ok((rest, !v))
}

// =============================================================================
// Integer encoding (varint)
// =============================================================================

/// Encode a signed 64-bit integer in ascending order (varint)
pub fn encode_varint_ascending(mut buf: Vec<u8>, v: i64) -> Vec<u8> {
    if v < 0 {
        match v {
            v if v >= -0xff => {
                buf.push(INT_MIN + 7);
                buf.push(v as u8);
            }
            v if v >= -0xffff => {
                buf.push(INT_MIN + 6);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffff => {
                buf.push(INT_MIN + 5);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffff => {
                buf.push(INT_MIN + 4);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffffff => {
                buf.push(INT_MIN + 3);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffffffff => {
                buf.push(INT_MIN + 2);
                buf.push((v >> 40) as u8);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            v if v >= -0xffffffffffffff => {
                buf.push(INT_MIN + 1);
                buf.push((v >> 48) as u8);
                buf.push((v >> 40) as u8);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
            _ => {
                buf.push(INT_MIN);
                buf.push((v >> 56) as u8);
                buf.push((v >> 48) as u8);
                buf.push((v >> 40) as u8);
                buf.push((v >> 32) as u8);
                buf.push((v >> 24) as u8);
                buf.push((v >> 16) as u8);
                buf.push((v >> 8) as u8);
                buf.push(v as u8);
            }
        }
        buf
    } else {
        encode_uvarint_ascending(buf, v as u64)
    }
}

/// Encode a signed integer in descending order
pub fn encode_varint_descending(buf: Vec<u8>, v: i64) -> Vec<u8> {
    encode_varint_ascending(buf, !v)
}

/// Encode an unsigned 64-bit integer in ascending order (uvarint)
pub fn encode_uvarint_ascending(mut buf: Vec<u8>, v: u64) -> Vec<u8> {
    match v {
        v if v <= INT_SMALL as u64 => {
            buf.push(INT_ZERO + v as u8);
        }
        v if v <= 0xff => {
            buf.push(INT_MAX - 7);
            buf.push(v as u8);
        }
        v if v <= 0xffff => {
            buf.push(INT_MAX - 6);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffff => {
            buf.push(INT_MAX - 5);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffff => {
            buf.push(INT_MAX - 4);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffff => {
            buf.push(INT_MAX - 3);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffff => {
            buf.push(INT_MAX - 2);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffffff => {
            buf.push(INT_MAX - 1);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        _ => {
            buf.push(INT_MAX);
            buf.push((v >> 56) as u8);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
    }
    buf
}

/// Encode an unsigned integer in descending order
pub fn encode_uvarint_descending(mut buf: Vec<u8>, v: u64) -> Vec<u8> {
    match v {
        0 => {
            buf.push(INT_MIN + 8);
        }
        v if v <= 0xff => {
            let v = !v;
            buf.push(INT_MIN + 7);
            buf.push(v as u8);
        }
        v if v <= 0xffff => {
            let v = !v;
            buf.push(INT_MIN + 6);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffff => {
            let v = !v;
            buf.push(INT_MIN + 5);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffff => {
            let v = !v;
            buf.push(INT_MIN + 4);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffff => {
            let v = !v;
            buf.push(INT_MIN + 3);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffff => {
            let v = !v;
            buf.push(INT_MIN + 2);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        v if v <= 0xffffffffffffff => {
            let v = !v;
            buf.push(INT_MIN + 1);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        _ => {
            let v = !v;
            buf.push(INT_MIN);
            buf.push((v >> 56) as u8);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
    }
    buf
}

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

    if length < 0 || length > 8 {
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

// =============================================================================
// Float encoding
// =============================================================================

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
    Ok((rest, -r))
}

// =============================================================================
// Bytes/String encoding
// =============================================================================

fn ones_complement(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        *b = !*b;
    }
}

/// Encode bytes in ascending order with escape-based encoding
pub fn encode_bytes_ascending(mut buf: Vec<u8>, data: &[u8]) -> Vec<u8> {
    buf.push(BYTES_MARKER);

    let mut i = 0;
    while i < data.len() {
        let byte = data[i];
        if byte == ESCAPE {
            buf.push(ESCAPE);
            buf.push(ESCAPED_00);
        } else {
            buf.push(byte);
        }
        i += 1;
    }

    // Terminator: 0x00 0x01
    buf.push(ESCAPE);
    buf.push(ESCAPED_TERM);
    buf
}

/// Encode bytes in descending order
pub fn encode_bytes_descending(mut buf: Vec<u8>, data: &[u8]) -> Vec<u8> {
    let start = buf.len();
    buf = encode_bytes_ascending(buf, data);
    buf[start] = BYTES_DESC_MARKER;
    ones_complement(&mut buf[start + 1..]);
    buf
}

/// Encode a string in ascending order
pub fn encode_string_ascending(buf: Vec<u8>, s: &str) -> Vec<u8> {
    encode_bytes_ascending(buf, s.as_bytes())
}

/// Encode a string in descending order
pub fn encode_string_descending(buf: Vec<u8>, s: &str) -> Vec<u8> {
    encode_bytes_descending(buf, s.as_bytes())
}

/// Decode bytes from ascending encoding
pub fn decode_bytes_ascending(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    if buf.is_empty() || buf[0] != BYTES_MARKER {
        return Err(Error::Other(format!(
            "cannot decode bytes: marker not found in {:?}",
            buf.first()
        )));
    }
    decode_bytes_internal(&buf[1..], ESCAPE, ESCAPED_TERM, ESCAPED_00)
}

/// Decode bytes from descending encoding
pub fn decode_bytes_descending(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    if buf.is_empty() || buf[0] != BYTES_DESC_MARKER {
        return Err(Error::Other(format!(
            "cannot decode bytes: marker not found in {:?}",
            buf.first()
        )));
    }

    let (rest, mut r) =
        decode_bytes_internal(&buf[1..], !ESCAPE, !ESCAPED_TERM, !ESCAPED_00)?;
    ones_complement(&mut r);
    Ok((rest, r))
}

fn decode_bytes_internal(
    mut buf: &[u8],
    escape: u8,
    escaped_term: u8,
    escaped_00: u8,
) -> Result<(&[u8], Vec<u8>)> {
    let mut result = Vec::new();

    loop {
        let i = buf.iter().position(|&b| b == escape);
        let i = match i {
            Some(i) => i,
            None => {
                return Err(Error::Other(
                    "terminator not found in bytes".to_string(),
                ))
            }
        };

        if i + 1 >= buf.len() {
            return Err(Error::Other("malformed escape sequence".to_string()));
        }

        let v = buf[i + 1];
        if v == escaped_term {
            result.extend_from_slice(&buf[..i]);
            return Ok((&buf[i + 2..], result));
        }

        if v != escaped_00 {
            return Err(Error::Other(format!(
                "unknown escape sequence: {:02x} {:02x}",
                escape, v
            )));
        }

        result.extend_from_slice(&buf[..i]);
        result.push(if escaped_00 == ESCAPED_00 { 0x00 } else { 0xff });
        buf = &buf[i + 2..];
    }
}

/// Decode a string from ascending encoding
pub fn decode_string_ascending(buf: &[u8]) -> Result<(&[u8], String)> {
    let (rest, bytes) = decode_bytes_ascending(buf)?;
    let s = String::from_utf8(bytes).map_err(|e| Error::Other(format!("invalid utf-8: {}", e)))?;
    Ok((rest, s))
}

/// Decode a string from descending encoding
pub fn decode_string_descending(buf: &[u8]) -> Result<(&[u8], String)> {
    let (rest, bytes) = decode_bytes_descending(buf)?;
    let s = String::from_utf8(bytes).map_err(|e| Error::Other(format!("invalid utf-8: {}", e)))?;
    Ok((rest, s))
}

// =============================================================================
// Time encoding
// =============================================================================

/// Encode a timestamp (as nanoseconds since Unix epoch) in ascending order
pub fn encode_time_ascending(mut buf: Vec<u8>, unix_nanos: i64) -> Vec<u8> {
    buf.push(TIME_MARKER);
    encode_varint_ascending(buf, unix_nanos)
}

/// Encode a timestamp in descending order
pub fn encode_time_descending(mut buf: Vec<u8>, unix_nanos: i64) -> Vec<u8> {
    buf.push(TIME_MARKER);
    encode_varint_descending(buf, unix_nanos)
}

/// Decode a timestamp from ascending encoding
pub fn decode_time_ascending(buf: &[u8]) -> Result<(&[u8], i64)> {
    if buf.is_empty() || buf[0] != TIME_MARKER {
        return Err(Error::Other(format!(
            "cannot decode time: marker not found in {:?}",
            buf.first()
        )));
    }
    decode_varint_ascending(&buf[1..])
}

/// Decode a timestamp from descending encoding
pub fn decode_time_descending(buf: &[u8]) -> Result<(&[u8], i64)> {
    if buf.is_empty() || buf[0] != TIME_MARKER {
        return Err(Error::Other(format!(
            "cannot decode time: marker not found in {:?}",
            buf.first()
        )));
    }
    decode_varint_descending(&buf[1..])
}

// =============================================================================
// Encode uint32/uint64 fixed width (for keys)
// =============================================================================

/// Encode uint32 in ascending order (big-endian, 4 bytes)
pub fn encode_uint32_ascending(mut buf: Vec<u8>, v: u32) -> Vec<u8> {
    buf.extend_from_slice(&v.to_be_bytes());
    buf
}

/// Encode uint32 in descending order
pub fn encode_uint32_descending(buf: Vec<u8>, v: u32) -> Vec<u8> {
    encode_uint32_ascending(buf, !v)
}

/// Decode uint32 from ascending encoding
pub fn decode_uint32_ascending(buf: &[u8]) -> Result<(&[u8], u32)> {
    if buf.len() < 4 {
        return Err(Error::Other(
            "insufficient bytes to decode uint32".to_string(),
        ));
    }
    let v = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    Ok((&buf[4..], v))
}

/// Decode uint32 from descending encoding
pub fn decode_uint32_descending(buf: &[u8]) -> Result<(&[u8], u32)> {
    let (rest, v) = decode_uint32_ascending(buf)?;
    Ok((rest, !v))
}

/// Encode uint64 in ascending order (big-endian, 8 bytes)
pub fn encode_uint64_ascending(mut buf: Vec<u8>, v: u64) -> Vec<u8> {
    buf.extend_from_slice(&v.to_be_bytes());
    buf
}

/// Encode uint64 in descending order
pub fn encode_uint64_descending(buf: Vec<u8>, v: u64) -> Vec<u8> {
    encode_uint64_ascending(buf, !v)
}

/// Decode uint64 from ascending encoding
pub fn decode_uint64_ascending(buf: &[u8]) -> Result<(&[u8], u64)> {
    if buf.len() < 8 {
        return Err(Error::Other(
            "insufficient bytes to decode uint64".to_string(),
        ));
    }
    let v = u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    Ok((&buf[8..], v))
}

/// Decode uint64 from descending encoding
pub fn decode_uint64_descending(buf: &[u8]) -> Result<(&[u8], u64)> {
    let (rest, v) = decode_uint64_ascending(buf)?;
    Ok((rest, !v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_encoding() {
        let buf = encode_null_ascending(vec![]);
        assert_eq!(buf, vec![ENCODED_NULL]);
        let (rest, is_null) = decode_if_null(&buf);
        assert!(is_null);
        assert!(rest.is_empty());

        let buf = encode_null_descending(vec![]);
        assert_eq!(buf, vec![ENCODED_NULL_DESC]);
        let (_, is_null) = decode_if_null(&buf);
        assert!(is_null);
    }

    #[test]
    fn test_bool_encoding() {
        // Ascending
        let buf = encode_bool_ascending(vec![], true);
        let (_, v) = decode_bool_ascending(&buf).unwrap();
        assert!(v);

        let buf = encode_bool_ascending(vec![], false);
        let (_, v) = decode_bool_ascending(&buf).unwrap();
        assert!(!v);

        // Descending
        let buf = encode_bool_descending(vec![], true);
        let (_, v) = decode_bool_descending(&buf).unwrap();
        assert!(v);

        let buf = encode_bool_descending(vec![], false);
        let (_, v) = decode_bool_descending(&buf).unwrap();
        assert!(!v);

        // Sort order: false < true in ascending
        let buf_false = encode_bool_ascending(vec![], false);
        let buf_true = encode_bool_ascending(vec![], true);
        assert!(buf_false < buf_true);
    }

    #[test]
    fn test_varint_encoding() {
        let test_values: Vec<i64> = vec![
            i64::MIN,
            i64::MIN + 1,
            -1_000_000,
            -1000,
            -1,
            0,
            1,
            1000,
            1_000_000,
            i64::MAX - 1,
            i64::MAX,
        ];

        for v in &test_values {
            let buf = encode_varint_ascending(vec![], *v);
            let (_, decoded) = decode_varint_ascending(&buf).unwrap();
            assert_eq!(decoded, *v, "ascending varint roundtrip failed for {}", v);

            let buf = encode_varint_descending(vec![], *v);
            let (_, decoded) = decode_varint_descending(&buf).unwrap();
            assert_eq!(
                decoded, *v,
                "descending varint roundtrip failed for {}",
                v
            );
        }

        // Verify sort order in ascending
        let encoded: Vec<Vec<u8>> = test_values
            .iter()
            .map(|v| encode_varint_ascending(vec![], *v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "ascending sort order failed: {} should be < {}",
                test_values[i],
                test_values[i + 1]
            );
        }
    }

    #[test]
    fn test_uvarint_encoding() {
        let test_values: Vec<u64> = vec![
            0,
            1,
            109, // INT_SMALL
            110,
            255,
            256,
            65535,
            65536,
            u32::MAX as u64,
            u64::MAX,
        ];

        for v in &test_values {
            let buf = encode_uvarint_ascending(vec![], *v);
            let (_, decoded) = decode_uvarint_ascending(&buf).unwrap();
            assert_eq!(
                decoded, *v,
                "ascending uvarint roundtrip failed for {}",
                v
            );

            let buf = encode_uvarint_descending(vec![], *v);
            let (_, decoded) = decode_uvarint_descending(&buf).unwrap();
            assert_eq!(
                decoded, *v,
                "descending uvarint roundtrip failed for {}",
                v
            );
        }

        // Verify sort order
        let encoded: Vec<Vec<u8>> = test_values
            .iter()
            .map(|v| encode_uvarint_ascending(vec![], *v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "ascending sort order failed: {} should be < {}",
                test_values[i],
                test_values[i + 1]
            );
        }
    }

    #[test]
    fn test_float64_encoding() {
        let test_values: Vec<f64> = vec![
            f64::NEG_INFINITY,
            f64::MIN,
            -1000.0,
            -1.0,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1000.0,
            f64::MAX,
            f64::INFINITY,
        ];

        for v in &test_values {
            let buf = encode_float64_ascending(vec![], *v);
            let (_, decoded) = decode_float64_ascending(&buf).unwrap();
            assert_eq!(decoded, *v, "float64 roundtrip failed for {}", v);

            let buf = encode_float64_descending(vec![], *v);
            let (_, decoded) = decode_float64_descending(&buf).unwrap();
            assert_eq!(
                decoded, *v,
                "descending float64 roundtrip failed for {}",
                v
            );
        }

        // Test NaN
        let buf = encode_float64_ascending(vec![], f64::NAN);
        let (_, decoded) = decode_float64_ascending(&buf).unwrap();
        assert!(decoded.is_nan());

        // Verify sort order (NaN first, then negatives, zero, positives)
        let encoded: Vec<Vec<u8>> = test_values
            .iter()
            .map(|v| encode_float64_ascending(vec![], *v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "float64 sort order failed: {} should be < {}",
                test_values[i],
                test_values[i + 1]
            );
        }
    }

    #[test]
    fn test_float32_encoding() {
        let test_values: Vec<f32> = vec![-1.0, 0.0, 1.0];

        for v in &test_values {
            let buf = encode_float32_ascending(vec![], *v);
            let (_, decoded) = decode_float32_ascending(&buf).unwrap();
            assert_eq!(decoded, *v, "float32 roundtrip failed for {}", v);
        }
    }

    #[test]
    fn test_bytes_encoding() {
        let test_cases: Vec<&[u8]> = vec![
            b"",
            b"hello",
            b"world",
            b"\x00",       // Null byte
            b"a\x00b",     // Embedded null
            b"\x00\x00",   // Multiple nulls
            b"\x00\x01",   // Looks like terminator
        ];

        for data in &test_cases {
            let buf = encode_bytes_ascending(vec![], data);
            let (_, decoded) = decode_bytes_ascending(&buf).unwrap();
            assert_eq!(&decoded, data, "bytes roundtrip failed");

            let buf = encode_bytes_descending(vec![], data);
            let (_, decoded) = decode_bytes_descending(&buf).unwrap();
            assert_eq!(&decoded, data, "descending bytes roundtrip failed");
        }

        // Verify sort order
        let test_sorted = vec![b"".as_slice(), b"a", b"aa", b"ab", b"b", b"ba"];
        let encoded: Vec<Vec<u8>> = test_sorted
            .iter()
            .map(|v| encode_bytes_ascending(vec![], v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "bytes sort order failed: {:?} should be < {:?}",
                test_sorted[i],
                test_sorted[i + 1]
            );
        }
    }

    #[test]
    fn test_string_encoding() {
        let test_cases = vec!["", "hello", "world", "test\x00string"];

        for s in &test_cases {
            let buf = encode_string_ascending(vec![], s);
            let (_, decoded) = decode_string_ascending(&buf).unwrap();
            assert_eq!(&decoded, s, "string roundtrip failed");

            let buf = encode_string_descending(vec![], s);
            let (_, decoded) = decode_string_descending(&buf).unwrap();
            assert_eq!(&decoded, s, "descending string roundtrip failed");
        }
    }

    #[test]
    fn test_time_encoding() {
        let test_values: Vec<i64> = vec![
            i64::MIN,
            -1_000_000_000, // -1 second
            0,
            1_000_000_000, // +1 second
            i64::MAX,
        ];

        for v in &test_values {
            let buf = encode_time_ascending(vec![], *v);
            let (_, decoded) = decode_time_ascending(&buf).unwrap();
            assert_eq!(decoded, *v, "time roundtrip failed for {}", v);

            let buf = encode_time_descending(vec![], *v);
            let (_, decoded) = decode_time_descending(&buf).unwrap();
            assert_eq!(decoded, *v, "descending time roundtrip failed for {}", v);
        }
    }

    #[test]
    fn test_uint32_fixed_encoding() {
        let test_values = vec![0u32, 1, 255, 256, u32::MAX / 2, u32::MAX];

        for v in &test_values {
            let buf = encode_uint32_ascending(vec![], *v);
            assert_eq!(buf.len(), 4);
            let (_, decoded) = decode_uint32_ascending(&buf).unwrap();
            assert_eq!(decoded, *v);

            let buf = encode_uint32_descending(vec![], *v);
            let (_, decoded) = decode_uint32_descending(&buf).unwrap();
            assert_eq!(decoded, *v);
        }
    }

    #[test]
    fn test_uint64_fixed_encoding() {
        let test_values = vec![0u64, 1, u32::MAX as u64, u64::MAX / 2, u64::MAX];

        for v in &test_values {
            let buf = encode_uint64_ascending(vec![], *v);
            assert_eq!(buf.len(), 8);
            let (_, decoded) = decode_uint64_ascending(&buf).unwrap();
            assert_eq!(decoded, *v);

            let buf = encode_uint64_descending(vec![], *v);
            let (_, decoded) = decode_uint64_descending(&buf).unwrap();
            assert_eq!(decoded, *v);
        }
    }

    #[test]
    fn test_peek_type() {
        assert_eq!(peek_type(&encode_null_ascending(vec![])), EncodedType::Null);
        assert_eq!(
            peek_type(&encode_bool_ascending(vec![], true)),
            EncodedType::Bool
        );
        assert_eq!(
            peek_type(&encode_varint_ascending(vec![], 42)),
            EncodedType::Int
        );
        assert_eq!(
            peek_type(&encode_float64_ascending(vec![], 3.14)),
            EncodedType::Float64
        );
        assert_eq!(
            peek_type(&encode_float32_ascending(vec![], 3.14)),
            EncodedType::Float32
        );
        assert_eq!(
            peek_type(&encode_bytes_ascending(vec![], b"test")),
            EncodedType::Bytes
        );
        assert_eq!(
            peek_type(&encode_time_ascending(vec![], 12345)),
            EncodedType::Time
        );
        assert_eq!(peek_type(&[]), EncodedType::Unknown);
    }

    // =====================================================================
    // Go-compatible encoding tests
    // These tests verify our encoding matches the Go reference implementation
    // =====================================================================

    /// Test varint ascending encoding matches Go implementation
    #[test]
    fn test_varint_go_compatible_ascending() {
        // Test cases from Go internal/encoding/int_test.go
        let test_cases: Vec<(i64, Vec<u8>)> = vec![
            (i64::MIN, vec![0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (i64::MIN + 1, vec![0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]),
            (-1 << 8, vec![0x86, 0xff, 0x00]),
            (-1, vec![0x87, 0xff]),
            (0, vec![0x88]),
            (1, vec![0x89]),
            (109, vec![0xf5]),
            (112, vec![0xf6, 0x70]),
            (1 << 8, vec![0xf7, 0x01, 0x00]),
            (i64::MAX, vec![0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        ];

        for (value, expected) in test_cases {
            let encoded = encode_varint_ascending(vec![], value);
            assert_eq!(
                encoded, expected,
                "varint ascending mismatch for {}: got {:02x?}, expected {:02x?}",
                value, encoded, expected
            );

            // Verify roundtrip
            let (_, decoded) = decode_varint_ascending(&encoded).unwrap();
            assert_eq!(decoded, value, "varint roundtrip failed for {}", value);
        }
    }

    /// Test varint descending encoding matches Go implementation
    #[test]
    fn test_varint_go_compatible_descending() {
        // Test cases from Go internal/encoding/int_test.go
        let test_cases: Vec<(i64, Vec<u8>)> = vec![
            (i64::MIN, vec![0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            (i64::MIN + 1, vec![0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]),
            (-1 << 8, vec![0xf6, 0xff]),
            (-110, vec![0xf5]),
            (-1, vec![0x88]),
            (0, vec![0x87, 0xff]),
            (1, vec![0x87, 0xfe]),
            (1 << 8, vec![0x86, 0xfe, 0xff]),
            (i64::MAX, vec![0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        ];

        for (value, expected) in test_cases {
            let encoded = encode_varint_descending(vec![], value);
            assert_eq!(
                encoded, expected,
                "varint descending mismatch for {}: got {:02x?}, expected {:02x?}",
                value, encoded, expected
            );

            // Verify roundtrip
            let (_, decoded) = decode_varint_descending(&encoded).unwrap();
            assert_eq!(decoded, value, "varint descending roundtrip failed for {}", value);
        }
    }

    /// Test uvarint ascending encoding matches Go implementation
    #[test]
    fn test_uvarint_go_compatible_ascending() {
        // Test cases from Go internal/encoding/int_test.go
        let test_cases: Vec<(u64, Vec<u8>)> = vec![
            (0, vec![0x88]),
            (1, vec![0x89]),
            (109, vec![0xf5]),
            (110, vec![0xf6, 0x6e]),
            (1 << 8, vec![0xf7, 0x01, 0x00]),
            (u64::MAX, vec![0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        ];

        for (value, expected) in test_cases {
            let encoded = encode_uvarint_ascending(vec![], value);
            assert_eq!(
                encoded, expected,
                "uvarint ascending mismatch for {}: got {:02x?}, expected {:02x?}",
                value, encoded, expected
            );

            // Verify roundtrip
            let (_, decoded) = decode_uvarint_ascending(&encoded).unwrap();
            assert_eq!(decoded, value, "uvarint roundtrip failed for {}", value);
        }
    }

    /// Test uvarint descending encoding matches Go implementation
    #[test]
    fn test_uvarint_go_compatible_descending() {
        // Test cases from Go internal/encoding/int_test.go
        let test_cases: Vec<(u64, Vec<u8>)> = vec![
            (0, vec![0x88]),
            (1, vec![0x87, 0xfe]),
            (1 << 8, vec![0x86, 0xfe, 0xff]),
            (u64::MAX - 1, vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]),
            (u64::MAX, vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        ];

        for (value, expected) in test_cases {
            let encoded = encode_uvarint_descending(vec![], value);
            assert_eq!(
                encoded, expected,
                "uvarint descending mismatch for {}: got {:02x?}, expected {:02x?}",
                value, encoded, expected
            );

            // Verify roundtrip
            let (_, decoded) = decode_uvarint_descending(&encoded).unwrap();
            assert_eq!(decoded, value, "uvarint descending roundtrip failed for {}", value);
        }
    }

    /// Test float64 ascending encoding matches Go implementation
    #[test]
    fn test_float64_go_compatible_ascending() {
        // Test cases from Go internal/encoding/float_test.go
        // Note: f64::from_bits(1) is the smallest subnormal, equivalent to Go's math.SmallestNonzeroFloat64
        let smallest_nonzero = f64::from_bits(1); // Equivalent to Go's math.SmallestNonzeroFloat64
        let test_cases: Vec<(f64, Vec<u8>)> = vec![
            (0.0, vec![FLOAT64_ZERO]),
            (smallest_nonzero, vec![FLOAT64_POS, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]),
            (0.00123, vec![FLOAT64_POS, 0x3f, 0x54, 0x26, 0xfe, 0x71, 0x8a, 0x86, 0xd7]),
            (1.0, vec![FLOAT64_POS, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (10.0, vec![FLOAT64_POS, 0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (100.0, vec![FLOAT64_POS, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (-1.0, vec![FLOAT64_NEG, 0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            (-100.0, vec![FLOAT64_NEG, 0x3f, 0xa6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        ];

        for (value, expected) in test_cases {
            let encoded = encode_float64_ascending(vec![], value);
            assert_eq!(
                encoded, expected,
                "float64 ascending mismatch for {}: got {:02x?}, expected {:02x?}",
                value, encoded, expected
            );

            // Verify roundtrip
            let (_, decoded) = decode_float64_ascending(&encoded).unwrap();
            assert_eq!(decoded, value, "float64 roundtrip failed for {}", value);
        }
    }

    /// Test that varint maintains sort order across all test values
    #[test]
    fn test_varint_sort_order_comprehensive() {
        // Test values from Go int_test.go int64TestCases
        // Note: Using parentheses for correct operator precedence
        let test_values: Vec<i64> = vec![
            i64::MIN, i64::MIN + 1,
            (-1i64 << 56) - 1, -1i64 << 56,
            (-1i64 << 48) - 1, -1i64 << 48,
            (-1i64 << 40) - 1, -1i64 << 40,
            (-1i64 << 32) - 1, -1i64 << 32,
            (-1i64 << 24) - 1, -1i64 << 24,
            (-1i64 << 16) - 1, -1i64 << 16,
            (-1i64 << 8) - 1, -1i64 << 8,
            -1, 0, 1,
            (1i64 << 8) - 1, 1i64 << 8,
            (1i64 << 16) - 1, 1i64 << 16,
            (1i64 << 24) - 1, 1i64 << 24,
            (1i64 << 32) - 1, 1i64 << 32,
            (1i64 << 40) - 1, 1i64 << 40,
            (1i64 << 48) - 1, 1i64 << 48,
            (1i64 << 56) - 1, 1i64 << 56,
            i64::MAX - 1, i64::MAX,
        ];

        // Test ascending order
        let encoded_asc: Vec<Vec<u8>> = test_values
            .iter()
            .map(|&v| encode_varint_ascending(vec![], v))
            .collect();

        for i in 0..encoded_asc.len() - 1 {
            assert!(
                encoded_asc[i] < encoded_asc[i + 1],
                "ascending sort order violated: {} ({:02x?}) should be < {} ({:02x?})",
                test_values[i], encoded_asc[i],
                test_values[i + 1], encoded_asc[i + 1]
            );

            // Verify roundtrip
            let (_, decoded) = decode_varint_ascending(&encoded_asc[i]).unwrap();
            assert_eq!(decoded, test_values[i]);
        }

        // Test descending order
        let encoded_desc: Vec<Vec<u8>> = test_values
            .iter()
            .map(|&v| encode_varint_descending(vec![], v))
            .collect();

        for i in 0..encoded_desc.len() - 1 {
            assert!(
                encoded_desc[i] > encoded_desc[i + 1],
                "descending sort order violated: {} ({:02x?}) should be > {} ({:02x?})",
                test_values[i], encoded_desc[i],
                test_values[i + 1], encoded_desc[i + 1]
            );

            // Verify roundtrip
            let (_, decoded) = decode_varint_descending(&encoded_desc[i]).unwrap();
            assert_eq!(decoded, test_values[i]);
        }
    }

    /// Test that uvarint maintains sort order across all test values
    #[test]
    fn test_uvarint_sort_order_comprehensive() {
        // Test values from Go int_test.go
        // Note: Using parentheses for correct operator precedence
        let test_values: Vec<u64> = vec![
            0, 1,
            (1 << 8) - 1, 1 << 8,
            (1 << 16) - 1, 1 << 16,
            (1 << 24) - 1, 1 << 24,
            (1 << 32) - 1, 1 << 32,
            (1 << 40) - 1, 1 << 40,
            (1 << 48) - 1, 1 << 48,
            (1 << 56) - 1, 1 << 56,
            u64::MAX - 1, u64::MAX,
        ];

        // Test ascending order
        let encoded_asc: Vec<Vec<u8>> = test_values
            .iter()
            .map(|&v| encode_uvarint_ascending(vec![], v))
            .collect();

        for i in 0..encoded_asc.len() - 1 {
            assert!(
                encoded_asc[i] < encoded_asc[i + 1],
                "ascending sort order violated: {} should be < {}",
                test_values[i], test_values[i + 1]
            );
        }

        // Test descending order
        let encoded_desc: Vec<Vec<u8>> = test_values
            .iter()
            .map(|&v| encode_uvarint_descending(vec![], v))
            .collect();

        for i in 0..encoded_desc.len() - 1 {
            assert!(
                encoded_desc[i] > encoded_desc[i + 1],
                "descending sort order violated: {} should be > {}",
                test_values[i], test_values[i + 1]
            );
        }
    }

    /// Test that float64 maintains sort order across typical values
    #[test]
    fn test_float64_sort_order_comprehensive() {
        // Test values from Go float_test.go
        let test_values: Vec<f64> = vec![
            f64::NEG_INFINITY,
            -f64::MAX,
            -1e308,
            -10000.0,
            -100.0,
            -1.0,
            -0.00123,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            0.00123,
            1.0,
            100.0,
            10000.0,
            1e308,
            f64::MAX,
            f64::INFINITY,
        ];

        // Test ascending order
        let encoded_asc: Vec<Vec<u8>> = test_values
            .iter()
            .map(|&v| encode_float64_ascending(vec![], v))
            .collect();

        for i in 0..encoded_asc.len() - 1 {
            assert!(
                encoded_asc[i] < encoded_asc[i + 1],
                "ascending sort order violated: {} should be < {}",
                test_values[i], test_values[i + 1]
            );

            // Verify roundtrip
            let (_, decoded) = decode_float64_ascending(&encoded_asc[i]).unwrap();
            assert_eq!(decoded, test_values[i], "roundtrip failed for {}", test_values[i]);
        }

        // Test descending order
        let encoded_desc: Vec<Vec<u8>> = test_values
            .iter()
            .map(|&v| encode_float64_descending(vec![], v))
            .collect();

        for i in 0..encoded_desc.len() - 1 {
            assert!(
                encoded_desc[i] > encoded_desc[i + 1],
                "descending sort order violated: {} should be > {}",
                test_values[i], test_values[i + 1]
            );
        }
    }
}
