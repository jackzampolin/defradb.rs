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

use serde::{Deserialize, Serialize};

/// KMS fetch request published on the `"encryption"` gossipsub topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchEncryptionKeyRequest {
    /// DID of the requesting principal (user identity if attached to the
    /// request context, else node identity). NOT the requesting peer's libp2p key.
    #[serde(rename = "Identity", with = "serde_bytes")]
    pub identity: Vec<u8>,

    /// CIDs of `Encryption` blocks the requester wants keys for.
    #[serde(rename = "Links")]
    pub links: Vec<Vec<u8>>,

    /// Requester's per-request X25519 ephemeral public key. Used by the
    /// responder to ECIES-wrap each reply block.
    #[serde(rename = "EphemeralPublicKey", with = "serde_bytes")]
    pub ephemeral_public_key: Vec<u8>,
}

/// KMS fetch reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchEncryptionKeyReply {
    /// Echo of CIDs in the same order as `blocks`. Only includes CIDs the
    /// responder is authorized to serve; missing entries indicate denial or
    /// not-held.
    #[serde(rename = "Links")]
    pub links: Vec<Vec<u8>>,

    /// ECIES-encrypted `Encryption` block bytes, one per `links` entry.
    #[serde(rename = "Blocks")]
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
    }
}
