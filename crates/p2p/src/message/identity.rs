use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes};
use super::traits::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityRequest {
    #[serde(rename = "Version")]
    pub version: String,

    #[serde(rename = "MessageID")]
    pub message_id: String,

    #[serde(rename = "SenderID")]
    pub sender_id: String,

    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    #[serde(rename = "PeerID")]
    pub peer_id: String,
}

impl IdentityRequest {
    pub fn new(peer_id: String) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            peer_id,
        }
    }
}

impl Message for IdentityRequest {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IdentityResponse {
    #[serde(rename = "Version")]
    pub version: String,

    #[serde(rename = "MessageID")]
    pub message_id: String,

    #[serde(rename = "SenderID")]
    pub sender_id: String,

    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    #[serde(rename = "IdentityToken", with = "serde_bytes")]
    pub identity_token: Vec<u8>,
}

impl IdentityResponse {
    pub fn success(request_message_id: &str, identity_token: Vec<u8>) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            identity_token,
        }
    }

    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
            identity_token: Vec::new(),
        }
    }
}

impl Message for IdentityResponse {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}
