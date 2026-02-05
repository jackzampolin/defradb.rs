//! CBOR serialization helpers for wire compatibility with Go.
//!
//! Go's fxamacker/cbor has specific behaviors for `[]byte` that Rust must match:
//!
//! | Rust Type | Go Equivalent | Serializer | When to Use |
//! |-----------|---------------|------------|-------------|
//! | `Vec<u8>` | `[]byte` (non-nil) | `serde_bytes` | Required bytes field |
//! | `Option<Vec<u8>>` | `[]byte` (nullable) | `optional_bytes` | Optional signature field |
//! | `Vec<u8>` (may be empty) | `[]byte` (nil=null) | `nullable_bytes` | Public key (empty→CBOR null) |
//!
//! ## `serde_bytes`
//! Standard CBOR byte string encoding. Use for `Vec<u8>` fields that always contain data.
//! Example: CID bytes, block data.
//!
//! ## `optional_bytes`
//! Handles `Option<Vec<u8>>` where `None` → CBOR null, `Some(bytes)` → CBOR byte string.
//! Use for fields that are conditionally present (e.g., signature before signing).
//!
//! ## `nullable_bytes`
//! Handles `Vec<u8>` where empty `Vec` → CBOR null (matching Go's `nil []byte`).
//! Use for fields like pubkey where Go sends CBOR null for unset values.
//! WARNING: On round-trip, empty bytes become CBOR null which becomes empty bytes.

/// Custom serialization for Option<Vec<u8>> as CBOR byte strings.
///
/// Use this for optional byte fields like signatures that may or may not be present.
/// - `None` serializes to CBOR null
/// - `Some(bytes)` serializes to CBOR byte string
pub mod optional_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(bytes) => serde_bytes::serialize(bytes, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<serde_bytes::ByteBuf> = Option::deserialize(deserializer)?;
        Ok(opt.map(|b| b.into_vec()))
    }
}

/// Custom serialization for Vec<u8> that treats empty as CBOR null.
///
/// Go's fxamacker/cbor sends `nil []byte` as CBOR null instead of empty byte string.
/// This serializer matches that behavior:
/// - Empty `Vec<u8>` serializes to CBOR null
/// - Non-empty `Vec<u8>` serializes to CBOR byte string
/// - CBOR null deserializes to empty `Vec<u8>`
///
/// Use for fields like pubkey where Go may send CBOR null for unset values.
pub mod nullable_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if value.is_empty() {
            serializer.serialize_none()
        } else {
            serde_bytes::serialize(value, serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<serde_bytes::ByteBuf> = Option::deserialize(deserializer)?;
        Ok(opt.map(|b| b.into_vec()).unwrap_or_default())
    }
}

/// Custom serialization for Vec<Vec<u8>> as CBOR byte strings.
///
/// Use this for arrays of byte fields like CID heads in DocSync.
pub mod vec_of_bytes {
    use serde::{de::SeqAccess, de::Visitor, ser::SerializeSeq, Deserializer, Serializer};

    pub fn serialize<S>(value: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(value.len()))?;
        for bytes in value {
            seq.serialize_element(&serde_bytes::Bytes::new(bytes))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VecBytesVisitor;

        impl<'de> Visitor<'de> for VecBytesVisitor {
            type Value = Vec<Vec<u8>>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence of byte arrays")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut result = Vec::new();
                while let Some(bytes) = seq.next_element::<serde_bytes::ByteBuf>()? {
                    result.push(bytes.into_vec());
                }
                Ok(result)
            }
        }

        deserializer.deserialize_seq(VecBytesVisitor)
    }
}
