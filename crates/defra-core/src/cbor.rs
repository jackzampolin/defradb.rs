//! CBOR encode/decode helpers over `ciborium`.
//!
//! `ciborium` writes into a `std::io::Write` rather than returning a `Vec`, and
//! splits its error type across serialization and deserialization. These two
//! functions give the whole workspace one shape for both, so call sites read the
//! same everywhere and the error carries which direction failed.
//!
//! This is not the dag-cbor stack. `serde_ipld_dagcbor` carries docID/CID parity
//! and is entirely separate; nothing here touches it.

use serde::{de::DeserializeOwned, Serialize};

/// A CBOR encode or decode failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cbor serialization failed: {0}")]
    Serialize(String),
    #[error("cbor deserialization failed: {0}")]
    Deserialize(String),
}

/// Encode `value` to a CBOR byte vector.
pub fn to_vec<T>(value: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize + ?Sized,
{
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).map_err(|e| Error::Serialize(e.to_string()))?;
    Ok(out)
}

/// Decode a CBOR byte slice into `T`.
///
/// The whole slice must be consumed. `ciborium` decodes one item and stops, so
/// without this check `b"hello from some other producer"` decodes as the text
/// string `"ello fro"` and silently discards the remaining 21 bytes. These
/// bytes arrive from peers, so trailing data is rejected rather than ignored.
pub fn from_slice<T>(bytes: &[u8]) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let mut cursor = std::io::Cursor::new(bytes);
    let value =
        ciborium::from_reader(&mut cursor).map_err(|e| Error::Deserialize(e.to_string()))?;
    let consumed = cursor.position() as usize;
    if consumed != bytes.len() {
        return Err(Error::Deserialize(format!(
            "trailing data after CBOR item: consumed {consumed} of {} bytes",
            bytes.len()
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
    struct Frame {
        name: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        count: u32,
    }

    #[test]
    fn round_trips() {
        let frame = Frame {
            name: "hello".to_string(),
            payload: vec![1, 2, 3],
            count: 7,
        };
        let bytes = to_vec(&frame).unwrap();
        assert_eq!(from_slice::<Frame>(&bytes).unwrap(), frame);
    }

    /// Regression: `ciborium` stops after one item, so a padded or truncated
    /// frame would otherwise decode successfully and drop the remainder.
    #[test]
    fn trailing_data_is_rejected() {
        let mut bytes = to_vec(&"hi".to_string()).unwrap();
        assert!(from_slice::<String>(&bytes).is_ok());
        bytes.extend_from_slice(b"junk");
        let err = from_slice::<String>(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("trailing data"),
            "expected a trailing-data error, got {err}"
        );

        // The exact shape that regressed a p2p test: valid CBOR prefix, garbage after.
        assert!(from_slice::<ciborium::Value>(b"hello from some other producer").is_err());
    }

    #[test]
    fn decode_failure_is_reported_as_deserialize() {
        let err = from_slice::<Frame>(&[0xff, 0xff, 0xff]).unwrap_err();
        assert!(matches!(err, Error::Deserialize(_)), "got {err:?}");
    }
}
