use super::super::{INT_MAX, INT_MIN, INT_SMALL, INT_ZERO};

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
