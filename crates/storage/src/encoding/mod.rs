//! Order-preserving value encoding for secondary indexes
//!
//! This module implements CockroachDB-style encoding that maintains sort order
//! when values are compared lexicographically as byte slices.
//!
//! Adapted from CockroachDB's encoding package:
//! https://github.com/cockroachdb/cockroach/tree/v20.2.19/pkg/util/encoding

mod bool;
mod bytes;
mod fixed;
mod float;
pub mod json;
mod null;
mod time;
mod varint;

pub use self::bool::*;
pub use bytes::*;
pub use fixed::*;
pub use float::*;
pub use null::*;
pub use time::*;
pub use varint::*;

// Type markers matching Go's encoding/encoding.go
pub(crate) const ENCODED_NULL: u8 = 0;
pub(crate) const FLOAT64_NAN: u8 = 1;
pub(crate) const FLOAT64_NEG: u8 = 2;
pub(crate) const FLOAT64_ZERO: u8 = 3;
pub(crate) const FLOAT64_POS: u8 = 4;
pub(crate) const FLOAT64_NAN_DESC: u8 = 5;
pub(crate) const BYTES_MARKER: u8 = 6;
pub(crate) const BYTES_DESC_MARKER: u8 = 7;
pub(crate) const TIME_MARKER: u8 = 8;
pub(crate) const FALSE_MARKER: u8 = 9;
pub(crate) const TRUE_MARKER: u8 = 10;
pub(crate) const JSON_MARKER: u8 = 11;
pub(crate) const FLOAT32_NAN: u8 = 12;
pub(crate) const FLOAT32_NEG: u8 = 13;
pub(crate) const FLOAT32_ZERO: u8 = 14;
pub(crate) const FLOAT32_POS: u8 = 15;
pub(crate) const FLOAT32_NAN_DESC: u8 = 16;

// Integer encoding constants
pub(crate) const INT_MIN: u8 = 0x80; // 128
pub(crate) const INT_MAX_WIDTH: u8 = 8;
pub(crate) const INT_ZERO: u8 = INT_MIN + INT_MAX_WIDTH; // 136
pub(crate) const INT_SMALL: u8 = INT_MAX - INT_ZERO - INT_MAX_WIDTH; // 109
pub(crate) const INT_MAX: u8 = 0xfd; // 253
pub(crate) const ENCODED_NULL_DESC: u8 = 0xff;

// Byte escape sequences
pub(crate) const ESCAPE: u8 = 0x00;
pub(crate) const ESCAPED_TERM: u8 = 0x01;
pub(crate) const ESCAPED_00: u8 = 0xff;

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
        m if (INT_MIN..=INT_MAX).contains(&m) => EncodedType::Int,
        m if (FLOAT32_NAN..=FLOAT32_NAN_DESC).contains(&m) => EncodedType::Float32,
        m if (FLOAT64_NAN..=FLOAT64_NAN_DESC).contains(&m) => EncodedType::Float64,
        TIME_MARKER => EncodedType::Time,
        FALSE_MARKER | TRUE_MARKER => EncodedType::Bool,
        JSON_MARKER => EncodedType::Json,
        _ => EncodedType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
