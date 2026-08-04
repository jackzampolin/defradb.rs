//! Go-wire-compatible CBOR types for the KMS pubsub protocol.
//!
//! Matches Go's `internal/kms/pubsub.go`:
//!   fetchEncryptionKeyRequest { Identity []byte, Links [][]byte, EphemeralPublicKey []byte }
//!   fetchEncryptionKeyReply   { Links [][]byte, Blocks [][]byte, EphemeralPublicKey []byte }
//!
//! No envelope (Version/MessageID/Pubkey/Signature etc.) — Go publishes
//! bare CBOR on gossipsub topic "encryption" and matches replies by
//! cryptographic compatibility (the requester's ephemeral pubkey is
//! wrapped into each ECIES reply block).
//! Rust may add an optional signed replay capability; older Go and Rust
//! decoders ignore that unknown map field.

use serde::{Deserialize, Serialize};

/// Serde helper: encode `Vec<Vec<u8>>` as a CBOR array of byte strings
/// (Go's `[][]byte` shape), not the default array-of-arrays-of-integers.
mod vec_of_bytes {
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

/// KMS fetch request published on the `"encryption"` gossipsub topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchEncryptionKeyRequest {
    /// DID of the requesting node. Retained for Go wire compatibility; Rust
    /// responders authorize against the transport-authenticated peer DID.
    #[serde(rename = "Identity", with = "serde_bytes")]
    pub identity: Vec<u8>,

    /// CIDs of `Encryption` blocks the requester wants keys for.
    #[serde(rename = "Links", with = "vec_of_bytes")]
    pub links: Vec<Vec<u8>>,

    /// Requester's per-request X25519 ephemeral public key. Used by the
    /// responder to ECIES-wrap each reply block.
    #[serde(rename = "EphemeralPublicKey", with = "serde_bytes")]
    pub ephemeral_public_key: Vec<u8>,

    /// Signed explicit-replay capability used only for owner-authorized replay.
    #[serde(
        rename = "ExplicitReplayCapability",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub explicit_replay_capability: Option<String>,
}

/// KMS fetch reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchEncryptionKeyReply {
    /// Echo of CIDs in the same order as `blocks`. Only includes CIDs the
    /// responder is authorized to serve; missing entries indicate denial or
    /// not-held.
    #[serde(rename = "Links", with = "vec_of_bytes")]
    pub links: Vec<Vec<u8>>,

    /// ECIES-encrypted `Encryption` block bytes, one per `links` entry.
    #[serde(rename = "Blocks", with = "vec_of_bytes")]
    pub blocks: Vec<Vec<u8>>,

    /// Responder's per-request X25519 ephemeral public key (informational;
    /// `crypto::encrypt_ecies` prepends a per-block ephemeral pubkey into each
    /// `blocks` entry too).
    #[serde(rename = "EphemeralPublicKey", with = "serde_bytes")]
    pub ephemeral_public_key: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_cbor_roundtrip() {
        let req = FetchEncryptionKeyRequest {
            identity: vec![0xaa; 32],
            links: vec![vec![0x01; 36], vec![0x02; 36]],
            ephemeral_public_key: vec![0xee; 32],
            explicit_replay_capability: Some("signed-proof".into()),
        };
        let bytes = serde_cbor::to_vec(&req).unwrap();
        let parsed: FetchEncryptionKeyRequest = serde_cbor::from_slice(&bytes).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn reply_cbor_roundtrip() {
        let reply = FetchEncryptionKeyReply {
            links: vec![vec![0x01; 36]],
            blocks: vec![vec![0xff; 100]],
            ephemeral_public_key: vec![0xdd; 32],
        };
        let bytes = serde_cbor::to_vec(&reply).unwrap();
        let parsed: FetchEncryptionKeyReply = serde_cbor::from_slice(&bytes).unwrap();
        assert_eq!(reply, parsed);
    }

    #[test]
    fn wire_uses_go_pascal_case_keys() {
        let req = FetchEncryptionKeyRequest {
            identity: vec![1],
            links: vec![vec![2]],
            ephemeral_public_key: vec![3],
            explicit_replay_capability: None,
        };
        let bytes = serde_cbor::to_vec(&req).unwrap();
        let val: serde_cbor::Value = serde_cbor::from_slice(&bytes).unwrap();
        let map = match val {
            serde_cbor::Value::Map(m) => m,
            _ => panic!(),
        };
        let keys: Vec<String> = map
            .into_keys()
            .filter_map(|k| match k {
                serde_cbor::Value::Text(s) => Some(s),
                _ => None,
            })
            .collect();
        assert!(keys.contains(&"Identity".to_string()));
        assert!(keys.contains(&"Links".to_string()));
        assert!(keys.contains(&"EphemeralPublicKey".to_string()));
        assert!(!keys.contains(&"ExplicitReplayCapability".to_string()));
    }

    #[test]
    fn links_encode_as_cbor_byte_strings_not_int_arrays() {
        // Encoding `vec![vec![0x01, 0x02]]` as `Vec<Vec<u8>>` must produce CBOR
        // major-type-2 byte strings, matching Go's [][]byte.
        let req = FetchEncryptionKeyRequest {
            identity: vec![],
            links: vec![vec![0x01, 0x02]],
            ephemeral_public_key: vec![],
            explicit_replay_capability: None,
        };
        let bytes = serde_cbor::to_vec(&req).unwrap();
        // Find the Links field's value in the encoded bytes. We just check that
        // the byte sequence 0x42, 0x01, 0x02 appears (CBOR byte-string-of-len-2,
        // 0x01, 0x02). The wrong (legacy) encoding would emit 0x82, 0x01, 0x02
        // (CBOR array of two unsigned ints). The 0x42 form is what Go expects.
        assert!(
            bytes.windows(3).any(|w| w == [0x42, 0x01, 0x02]),
            "Links inner Vec<u8> must encode as CBOR byte string (0x42 0x01 0x02), not array of ints. Got: {:02x?}",
            bytes
        );
        assert!(
            !bytes.windows(3).any(|w| w == [0x82, 0x01, 0x02]),
            "Found legacy array-of-ints encoding (0x82 0x01 0x02); fix not applied. Got: {:02x?}",
            bytes
        );
    }
}
