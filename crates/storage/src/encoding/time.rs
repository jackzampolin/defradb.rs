//! Timestamp encoding

use crate::corekv::{Error, Result};

use super::{
    decode_varint_ascending, decode_varint_descending, encode_varint_ascending,
    encode_varint_descending, TIME_MARKER,
};

/// Encode `(seconds since the epoch, sub-second nanoseconds)` in ascending
/// order, as two varints matching Go's `encodeTime`.
///
/// One `i64` of nanoseconds would only span 1677-2262, far narrower than the
/// range RFC3339 and the schema accept. `nanos` must be the sub-second
/// remainder, in `0..1_000_000_000`, or lexicographic order stops matching
/// chronological order.
pub fn encode_time_ascending(mut buf: Vec<u8>, seconds: i64, nanos: u32) -> Vec<u8> {
    buf.push(TIME_MARKER);
    buf = encode_varint_ascending(buf, seconds);
    encode_varint_ascending(buf, i64::from(nanos))
}

/// Encode a timestamp in descending order. Both components are complemented,
/// as Go's `EncodeTimeDescending` spells out with `^t.Unix()`.
pub fn encode_time_descending(mut buf: Vec<u8>, seconds: i64, nanos: u32) -> Vec<u8> {
    buf.push(TIME_MARKER);
    buf = encode_varint_descending(buf, seconds);
    encode_varint_descending(buf, i64::from(nanos))
}

/// Decode an ascending timestamp as `(seconds, nanos)`.
pub fn decode_time_ascending(buf: &[u8]) -> Result<(&[u8], i64, u32)> {
    let rest = strip_marker(buf)?;
    let (rest, seconds) = decode_varint_ascending(rest)?;
    let (rest, nanos) = decode_varint_ascending(rest)?;
    Ok((rest, seconds, sub_second(nanos)?))
}

/// Decode a descending timestamp as `(seconds, nanos)`.
pub fn decode_time_descending(buf: &[u8]) -> Result<(&[u8], i64, u32)> {
    let rest = strip_marker(buf)?;
    let (rest, seconds) = decode_varint_descending(rest)?;
    let (rest, nanos) = decode_varint_descending(rest)?;
    Ok((rest, seconds, sub_second(nanos)?))
}

fn strip_marker(buf: &[u8]) -> Result<&[u8]> {
    if buf.is_empty() || buf[0] != TIME_MARKER {
        return Err(Error::Other(format!(
            "cannot decode time: marker not found in {:?}",
            buf.first()
        )));
    }
    Ok(&buf[1..])
}

fn sub_second(nanos: i64) -> Result<u32> {
    u32::try_from(nanos)
        .ok()
        .filter(|n| *n < 1_000_000_000)
        .ok_or_else(|| {
            Error::Other(format!(
                "cannot decode time: {nanos} is not a sub-second nanosecond remainder"
            ))
        })
}
